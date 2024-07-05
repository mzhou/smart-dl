use smart_dl::try_main;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::process::exit(try_main(std::env::args_os())?)
}
