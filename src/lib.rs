mod io_mgr;

use std::cmp::{max, min};
use std::collections::VecDeque;
use std::error::Error;
use std::ffi::OsString;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::fs::File;
use std::io::{Error as IoError, Write};
use std::num::ParseIntError;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use clap::Parser;
use percent_encoding::percent_decode_str;
use reqwest::header::{ToStrError, CONTENT_LENGTH, RANGE};
use reqwest::{Client, Error as ReqwestError};
use tokio::select;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::watch::error::SendError as WatchSendError;
use tokio::sync::watch::{
    channel as watch_channel, Receiver as WatchReceiver, Sender as WatchSender,
};
use tokio::task::{spawn, spawn_blocking, JoinError, JoinHandle, JoinSet};
use tokio::time::sleep;
use url::{ParseError, Url};

use io_mgr::{create_anon_mmap, create_mmap, MmapMut};

pub struct LinearJob {
    buf: MmapMut,
    offset: usize,
}

#[derive(Debug)]
pub enum LinearError {
    Io(IoError),
}

#[derive(Debug)]
pub enum MainError {
    Io(IoError),
    Join(JoinError),
    Linear(LinearError),
    MissingContentLength,
    OutputAlreadyExists,
    OutputFromUrl,
    Parse(ParseError),
    ParseInt(ParseIntError),
    Reqwest(ReqwestError),
    Task(TaskError),
    ToStr(ToStrError),
}

#[derive(Parser)]
struct Opts {
    #[arg(long, default_value = "16777216")]
    chunk_size: u64,
    #[arg(long, default_value = "64")]
    connections: usize,
    #[arg(long, required = false)]
    linear: Option<String>,
    #[arg(long, required = false)]
    no_output: bool,
    #[arg(default_value = "", long)]
    output: String,
    #[arg(long)]
    url: String,
}

#[derive(Debug)]
pub enum TaskError {
    CloneReq,
    Io(IoError),
    Reqwest(ReqwestError),
    SendProgress(WatchSendError<Option<TaskProgress>>),
    Send(SendError<LinearJob>),
}

struct TaskReturn {
    resources: TaskResources,
}

pub struct TaskProgress {
    bytes_received: usize,
    end: Instant,
    start: Instant,
}

struct TaskResources {
    client: Client,
    linear_sender: Option<Sender<LinearJob>>,
    output: String,
    progress_sender: WatchSender<Option<TaskProgress>>,
    url: String,
}

impl Display for LinearError {
    fn fmt(self: &Self, f: &mut Formatter) -> FmtResult {
        write!(f, "{:?}", self)
    }
}

impl Error for LinearError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        use LinearError::*;
        match self {
            Io(e) => e.source(),
        }
    }
}

impl Display for MainError {
    fn fmt(self: &Self, f: &mut Formatter) -> FmtResult {
        write!(f, "{:?}", self)
    }
}

impl Error for MainError {}

impl Display for TaskError {
    fn fmt(self: &Self, f: &mut Formatter) -> FmtResult {
        write!(f, "{:?}", self)
    }
}

impl Error for TaskError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        use TaskError::*;
        match self {
            CloneReq => None,
            Io(e) => e.source(),
            Reqwest(e) => e.source(),
            SendProgress(e) => e.source(),
            Send(e) => e.source(),
        }
    }
}

const RETRY_WAIT_BASE: Duration = Duration::new(0, 100_000_000); // 0.1 seconds

fn build_client() -> Result<Client, ReqwestError> {
    Client::builder().build()
}

#[tokio::main]
pub async fn try_main<I, T>(itr: I) -> Result<i32, MainError>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let opts = Opts::parse_from(itr);

    let mut output = String::new();

    if !opts.no_output {
        output = opts.output.clone();
        if output.is_empty() {
            output = percent_decode_str(
                Url::parse(&opts.url)
                    .map_err(MainError::Parse)?
                    .path_segments()
                    .ok_or(MainError::OutputFromUrl)?
                    .filter(|s| !s.is_empty())
                    .last()
                    .ok_or(MainError::OutputFromUrl)?,
            )
            .decode_utf8_lossy()
            .to_string();
            if output.is_empty() {
                eprintln!("couldn't calculate output file from url");
                return Err(MainError::OutputFromUrl);
            } else {
                eprintln!("output: {}", output);
                File::create_new(&output).map_err(|_| MainError::OutputAlreadyExists)?;
            }
        }
    }

    let (linear_sender, mut linear_receiver) = channel::<LinearJob>(1);
    let mut linear_task = Option::<JoinHandle<Result<(), LinearError>>>::None;

    if let Some(linear) = opts.linear {
        let mut linear_file = File::create(linear).map_err(MainError::Io)?;
        linear_task = Some(spawn_blocking(move || {
            let mut next_offset = 0;
            let mut queue = VecDeque::<LinearJob>::new();
            while let Some(job) = linear_receiver.blocking_recv() {
                let initial_offset = next_offset;
                if job.offset == next_offset {
                    linear_file.write_all(&job.buf).map_err(LinearError::Io)?;
                    next_offset += job.buf.len();
                } else {
                    queue.push_back(job);
                    queue.make_contiguous().sort_by_key(|j| j.offset);
                }
                while let Some(queued_job) = queue.pop_front() {
                    if queued_job.offset != next_offset {
                        queue.push_front(queued_job);
                        break;
                    }
                    linear_file
                        .write_all(&queued_job.buf)
                        .map_err(LinearError::Io)?;
                    next_offset += queued_job.buf.len();
                }
                if next_offset != initial_offset {
                    eprintln!(
                        "Linear wrote {} bytes at {}",
                        next_offset - initial_offset,
                        initial_offset
                    );
                }
            }
            Ok::<_, LinearError>(())
        }));
    }

    let mut pool = VecDeque::<TaskResources>::new();
    let progress_receivers = Arc::new(RwLock::new(
        VecDeque::<WatchReceiver<Option<TaskProgress>>>::new(),
    ));

    let make_resources = {
        let progress_receivers = progress_receivers.clone();
        let linear_enabled = linear_task.is_some();
        move || -> Result<TaskResources, MainError> {
            let (progress_sender, progress_receiver) = watch_channel(None);
            progress_receivers
                .write()
                .unwrap()
                .push_back(progress_receiver);

            Ok(TaskResources {
                client: build_client().map_err(MainError::Reqwest)?,
                linear_sender: if linear_enabled {
                    Some(linear_sender.clone())
                } else {
                    None
                },
                output: output.clone(),
                progress_sender,
                url: opts.url.clone(),
            })
        }
    };

    for _ in 0..opts.connections.saturating_sub(1) {
        pool.push_back(make_resources()?);
    }

    let head_resources = make_resources()?;

    let req = head_resources.client.head(&head_resources.url);
    let res = req.send().await.map_err(MainError::Reqwest)?;
    let content_length = res
        .headers()
        .get(CONTENT_LENGTH)
        .ok_or(MainError::MissingContentLength)?
        .to_str()
        .map_err(MainError::ToStr)?
        .parse::<u64>()
        .map_err(MainError::ParseInt)?;
    eprintln!("content length {}", content_length);

    pool.push_back(head_resources);

    let progress_cancel = Arc::new(AtomicBool::new(false));
    let progress_print_task = spawn({
        let progress_cancel = progress_cancel.clone();
        async move {
            loop {
                if progress_cancel.load(Ordering::Relaxed) {
                    break;
                }
                sleep(Duration::from_secs(1)).await;
                let now = Instant::now();
                let mut active_conns = 0;
                let mut total_conns = 0;
                let total_rate: u64 = progress_receivers
                    .write()
                    .unwrap()
                    .iter_mut()
                    .map(|pr| {
                        total_conns += 1;
                        let Some(progress) = &*pr.borrow_and_update() else {
                            return 0u64;
                        };
                        // assume 0 from tasks that haven't received for more than 1 second
                        if now.duration_since(progress.end) > Duration::from_secs(1) {
                            return 0u64;
                        }
                        active_conns += 1;
                        // otherwise assume same speed over last read
                        (progress.bytes_received as u64)
                            / max(
                                progress.end.duration_since(progress.start).as_millis() as u64,
                                1,
                            )
                    })
                    .sum();
                if total_rate > 0 {
                    eprintln!(
                        "Speed: {} KB/s. {}/{} connections received data in last second.",
                        total_rate, active_conns, total_conns
                    );
                }
            }
        }
    });

    let file_chunks = (content_length + opts.chunk_size - 1) / opts.chunk_size;
    eprintln!("{} chunks", file_chunks);
    let mut tasks = JoinSet::<Result<TaskReturn, TaskError>>::new();
    for chunk_i in 0..file_chunks {
        let resources = if let Some(r) = pool.pop_front() {
            Ok::<_, MainError>(r)
        } else {
            if let Some(task_return_join_result) = tasks.join_next().await {
                let task_return_result = task_return_join_result.map_err(MainError::Join)?;
                let task_return = task_return_result.map_err(MainError::Task)?;
                let r = task_return.resources;
                Ok(r)
            } else {
                Ok(make_resources()?)
            }
        }?;

        let range_begin = chunk_i * opts.chunk_size;
        let range_end = min(content_length, (chunk_i + 1u64) * opts.chunk_size);
        let range_size = range_end - range_begin;
        let range_str = format!("bytes={}-{}", range_begin, range_end - 1);
        let req = resources
            .client
            .get(&resources.url)
            .header(RANGE, range_str.clone())
            .build()
            .map_err(MainError::Reqwest)?;
        let _ = tasks.spawn(async move {
            // now acquire mmap
            let mut mapping = if resources.output.is_empty() {
                create_anon_mmap(range_size as usize)
            } else {
                create_mmap(
                    &resources.output,
                    content_length,
                    range_begin,
                    range_size as usize,
                )
            }
            .map_err(TaskError::Io)?;
            let mut retry = 0;
            'retry: loop {
                let delay = RETRY_WAIT_BASE * 2u32.pow(retry);
                let mut progress_start = Instant::now();
                // send request and wait for response
                let res_result = resources
                    .client
                    .execute(req.try_clone().ok_or(TaskError::CloneReq)?)
                    .await;
                match res_result {
                    Ok(mut res) => {
                        if res.status() != 206 {
                            eprintln!(
                                "Error downloading chunk {} ({}) (retry {}) wait {:?}: {}",
                                chunk_i,
                                &range_str,
                                retry,
                                &delay,
                                res.status()
                            );
                            tokio::time::sleep(delay).await;
                            retry += 1;
                            continue;
                        }
                        let mut bytes_received = 0usize;
                        loop {
                            let chunk_res = res.chunk().await;
                            let chunk_opt = match chunk_res {
                                Err(chunk_err) => {
                                    eprintln!(
                                        "Error downloading chunk {} ({}) (retry {}) wait {:?}: {:?}",
                                        chunk_i,
                                        &range_str,
                                        retry,
                                        &delay,
                                        chunk_err
                                    );
                                    tokio::time::sleep(delay).await;
                                    retry += 1;
                                    continue 'retry;
                                },
                                Ok(co) => co,
                            };
                            let Some(chunk) = chunk_opt else {
                                break;
                            };
                            let mut burst_bytes_received = 0;
                            let mut progress_end = Instant::now();
                            mapping[bytes_received + burst_bytes_received..bytes_received + burst_bytes_received + chunk.len()]
                                .copy_from_slice(chunk.as_ref());
                            burst_bytes_received += chunk.len();

                            let progress = TaskProgress {
                                end: progress_end,
                                bytes_received: burst_bytes_received,
                                start: progress_start,
                            };
                            resources
                                .progress_sender
                                .send(Some(progress))
                                .map_err(TaskError::SendProgress)?;

                            // also grab any further chunks that are immediately available
                            let mut got_none_chunk = false;
                            loop {
                                select! {
                                    biased;
                                    chunk_res = res.chunk() => {
                                        let chunk_opt = match chunk_res {
                                            Err(chunk_err) => {
                                                eprintln!(
                                                    "Error downloading chunk {} ({}) (retry {}) wait {:?}: {:?}",
                                                    chunk_i,
                                                    &range_str,
                                                    retry,
                                                    &delay,
                                                    chunk_err
                                                );
                                                tokio::time::sleep(delay).await;
                                                retry += 1;
                                                continue 'retry;
                                            },
                                            Ok(co) => co,
                                        };
                                        let Some(chunk) = chunk_opt else {
                                            got_none_chunk = true;
                                            break;
                                        };
                                        progress_end = Instant::now();
                                        mapping[bytes_received + burst_bytes_received..bytes_received + burst_bytes_received + chunk.len()]
                                            .copy_from_slice(chunk.as_ref());
                                        burst_bytes_received += chunk.len();

                                        let progress = TaskProgress {
                                            end: progress_end,
                                            bytes_received: burst_bytes_received,
                                            start: progress_start,
                                        };
                                        resources
                                            .progress_sender
                                            .send(Some(progress))
                                            .map_err(TaskError::SendProgress)?;
                                    },
                                    else => break,
                                }
                            }

                            bytes_received += burst_bytes_received;

                            if got_none_chunk {
                                break;
                            }

                            progress_start = Instant::now();
                        }
                        mapping.flush_async().map_err(TaskError::Io)?;
                        eprintln!(
                            "Finished downloading chunk {} ({}) (retry {})",
                            chunk_i,
                            &range_str,
                            retry,
                        );
                        break;
                    }
                    Err(e) => {
                        eprintln!(
                            "Error downloading chunk {} ({}) (retry {}) wait {:?}: {:?}",
                            chunk_i, &range_str, retry, &delay, e
                        );
                        tokio::time::sleep(delay).await;
                        retry += 1;
                    }
                }
            }
            if let Some(linear_sender) = &resources.linear_sender {
                linear_sender
                    .send(LinearJob {
                        buf: mapping,
                        offset: range_begin as usize,
                    })
                    .await
                    .map_err(TaskError::Send)?;
            }
            Ok(TaskReturn { resources })
        });
    }

    eprintln!("all dl tasks created");

    // clean up linear_sender clones
    drop(make_resources);
    drop(pool);

    eprintln!("resources cleaned up");

    while let Some(task_return_join_result) = tasks.join_next().await {
        task_return_join_result
            .map_err(MainError::Join)?
            .map_err(MainError::Task)?;
    }

    eprintln!("all dl tasks done");

    if let Some(lt) = linear_task {
        eprintln!("linear task wait");
        lt.await
            .map_err(MainError::Join)?
            .map_err(MainError::Linear)?;
        eprintln!("linear task done");
    }

    progress_cancel.store(true, Ordering::Relaxed);

    eprintln!("progress cancel requested");

    progress_print_task.await.map_err(MainError::Join)?;

    eprintln!("ok");

    Ok(0)
}
