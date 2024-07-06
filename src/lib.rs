mod io_mgr;

use std::cmp::min;
use std::collections::VecDeque;
use std::error::Error;
use std::ffi::OsString;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::fs::File;
use std::io::{Error as IoError, Write};
use std::time::Duration;

use clap::Parser;
use reqwest::header::{CONTENT_LENGTH, RANGE};
use reqwest::{Client, Error as ReqwestError};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{channel, Sender};
use tokio::task::{spawn_blocking, JoinHandle, JoinSet};

use io_mgr::{create_mmap, MmapMut};

struct LinearJob {
    buf: MmapMut,
    offset: usize,
}

#[derive(Debug)]
enum LinearError {
    Io(IoError),
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum MainError {
    MissingContentLength,
}

#[derive(Parser)]
struct Opts {
    #[arg(long, default_value = "64")]
    connections: usize,
    #[arg(long, required = false)]
    linear: Option<String>,
    #[arg(long)]
    output: String,
    #[arg(long)]
    url: String,
}

#[derive(Debug)]
enum TaskError {
    CloneReq,
    Io(IoError),
    Reqwest(ReqwestError),
    Send(SendError<LinearJob>),
}

struct TaskReturn {
    resources: TaskResources,
}

struct TaskResources {
    client: Client,
    linear_sender: Option<Sender<LinearJob>>,
    output: String,
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
            Send(e) => e.source(),
        }
    }
}

impl Display for MainError {
    fn fmt(self: &Self, f: &mut Formatter) -> FmtResult {
        write!(f, "{:?}", self)
    }
}

impl Error for MainError {}

const CHUNK_SIZE: u64 = 16 * 1024 * 1024;
const RETRY_WAIT_BASE: Duration = Duration::new(0, 100_000_000); // 0.1 seconds

fn build_client() -> Result<Client, ReqwestError> {
    Client::builder().build()
}

#[tokio::main]
pub async fn try_main<I, T>(itr: I) -> Result<i32, Box<dyn Error>>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let opts = Opts::parse_from(itr);

    let (linear_sender, mut linear_receiver) = channel::<LinearJob>(1);
    let mut linear_task = Option::<JoinHandle<Result<(), LinearError>>>::None;

    if let Some(linear) = opts.linear {
        let mut linear_file = File::create(linear)?;
        linear_task = Some(spawn_blocking(move || {
            let mut next_offset = 0;
            let mut queue = VecDeque::<LinearJob>::new();
            while let Some(job) = linear_receiver.blocking_recv() {
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
            }
            Ok::<_, LinearError>(())
        }));
    }

    let mut pool = VecDeque::<TaskResources>::new();

    let make_resources = || -> Result<TaskResources, ReqwestError> {
        Ok(TaskResources {
            client: build_client()?,
            linear_sender: if linear_task.is_some() {
                Some(linear_sender.clone())
            } else {
                None
            },
            output: opts.output.clone(),
            url: opts.url.clone(),
        })
    };

    for _ in 0..opts.connections.saturating_sub(1) {
        pool.push_back(make_resources()?);
    }

    let head_resources = make_resources()?;

    let req = head_resources.client.head(&head_resources.url);
    let res = req.send().await?;
    let content_length = res
        .headers()
        .get(CONTENT_LENGTH)
        .ok_or(MainError::MissingContentLength)?
        .to_str()?
        .parse::<u64>()?;
    eprintln!("content length {}", content_length);

    pool.push_back(head_resources);

    let file_chunks = (content_length + CHUNK_SIZE - 1) / CHUNK_SIZE;
    eprintln!("{} chunks", file_chunks);
    let mut tasks = JoinSet::<Result<TaskReturn, TaskError>>::new();
    for chunk_i in 0..file_chunks {
        let resources = if let Some(r) = pool.pop_front() {
            Ok::<_, MainError>(r)
        } else {
            if let Some(task_return_join_result) = tasks.join_next().await {
                let task_return_result = task_return_join_result?;
                let task_return = task_return_result?;
                let r = task_return.resources;
                Ok(r)
            } else {
                Ok(make_resources()?)
            }
        }?;

        let range_begin = chunk_i * CHUNK_SIZE;
        let range_end = min(content_length, (chunk_i + 1u64) * CHUNK_SIZE);
        let range_size = range_end - range_begin;
        let range_str = format!("bytes={}-{}", range_begin, range_end - 1);
        let req = resources
            .client
            .get(&resources.url)
            .header(RANGE, range_str.clone())
            .build()?;
        let _ = tasks.spawn(async move {
            // now acquire mmap
            let mut mapping = create_mmap(
                &resources.output,
                content_length,
                range_begin,
                range_size as usize,
            )
            .map_err(TaskError::Io)?;
            let mut retry = 0;
            loop {
                // send request and wait for response
                let res_result = resources
                    .client
                    .execute(req.try_clone().ok_or(TaskError::CloneReq)?)
                    .await;
                match res_result {
                    Ok(res) => {
                        if res.status() != 206 {
                            let delay = RETRY_WAIT_BASE * 2u32.pow(retry);
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
                        let bytes = res.bytes().await.map_err(TaskError::Reqwest)?;
                        mapping.copy_from_slice(bytes.as_ref());
                        mapping.flush_async().map_err(TaskError::Io)?;
                        break;
                    }
                    Err(e) => {
                        let delay = RETRY_WAIT_BASE * 2u32.pow(retry);
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

    while let Some(task_return_join_result) = tasks.join_next().await {
        task_return_join_result??;
    }

    drop(linear_sender);

    if let Some(lt) = linear_task {
        lt.await??;
    }

    Ok(0)
}
