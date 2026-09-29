mod server_process;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    server_process::main(true)
}
