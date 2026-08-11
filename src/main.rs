use nauti_nodes::{run_coordinator, run_worker, Config, RuntimeMode};

#[tokio::main]
async fn main() {
    let config = Config::from_env().unwrap_or_default();
    let result = match config.mode {
        RuntimeMode::Worker => run_worker(config).await,
        RuntimeMode::Coordinator => run_coordinator(config).await,
    };
    if let Err(e) = result {
        eprintln!("nauti-nodes: {e}");
        std::process::exit(1);
    }
}
