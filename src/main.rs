use nauti_nodes::{cli, run_coordinator, run_worker, Config, RuntimeMode};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let config = Config::from_env().unwrap_or_default();

    let json_output = args.iter().any(|a| a == "--json");
    let cmd = args.get(1).map(|s| s.as_str()).unwrap_or("run");

    match cmd {
        "preflight" => {
            if let Err(e) = cli::run_preflight(&config, json_output) {
                eprintln!("Preflight error: {e}");
                std::process::exit(1);
            }
        }
        "status" => {
            if let Err(e) = cli::run_status(&config, json_output) {
                eprintln!("Status error: {e}");
                std::process::exit(1);
            }
        }
        "withdraw" => {
            if let Err(e) = cli::run_withdraw(&config).await {
                eprintln!("Withdrawal error: {e}");
                std::process::exit(1);
            }
        }
        "report" => {
            if let Err(e) = cli::run_report(&config, json_output) {
                eprintln!("Report error: {e}");
                std::process::exit(1);
            }
        }
        "bundle" | "diagnostic-bundle" => {
            let out_arg = args
                .iter()
                .position(|a| a == "--output-file" || a == "-o")
                .and_then(|idx| args.get(idx + 1).map(|s| s.as_str()));
            if let Err(e) = cli::run_bundle(&config, out_arg) {
                eprintln!("Diagnostic bundle error: {e}");
                std::process::exit(1);
            }
        }
        "worker" => {
            if let Err(e) = run_worker(config).await {
                eprintln!("Worker error: {e}");
                std::process::exit(1);
            }
        }
        "coordinator" => {
            if let Err(e) = run_coordinator(config).await {
                eprintln!("Coordinator error: {e}");
                std::process::exit(1);
            }
        }
        _ => {
            let result = match config.mode {
                RuntimeMode::Worker => run_worker(config).await,
                RuntimeMode::Coordinator => run_coordinator(config).await,
            };
            if let Err(e) = result {
                eprintln!("nauti-nodes: {e}");
                std::process::exit(1);
            }
        }
    }
}
