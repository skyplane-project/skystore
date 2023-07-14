use clap::{App, Arg, SubCommand};
use serde::{Deserialize, Serialize};
use serde_json;
use std::{
    fs::read_to_string,
    process::{Command, ExitStatus},
};
use tokio::{task::JoinHandle, time::Duration};

#[derive(Debug, Deserialize)]
struct InitConfig {
    init_regions: Vec<String>,
    client_from_region: String,
}

fn init(
    config_file: &str,
) -> (
    JoinHandle<()>,
    JoinHandle<Result<ExitStatus, std::io::Error>>,
) {
    let config: InitConfig = serde_json::from_str(&read_to_string(config_file).unwrap()).unwrap();
    let init_regions_str = config.init_regions.join(",");

    let mut skystore_server = Command::new("just")
        .arg("--justfile")
        .arg("s3-proxy/justfile")
        .arg("run-skystore-server")
        .env("INIT_REGIONS", init_regions_str.clone())
        .spawn()
        .expect("Failed to start skystore server");

    let skystore_server_handle = tokio::spawn(async move {
        skystore_server.wait().unwrap();
    });

    let s3_proxy_handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let mut child = Command::new("cargo")
            .env("INIT_REGIONS", init_regions_str.clone())
            .env("CLIENT_FROM_REGION", config.client_from_region)
            .current_dir("s3-proxy")
            .arg("run")
            .env("RUST_LOG", "INFO")
            .env("RUST_BACKTRACE", "full")
            .spawn()
            .expect("Failed to start s3-proxy");

        child.wait()
    });

    (skystore_server_handle, s3_proxy_handle)
}

#[derive(Debug, Serialize, Deserialize)]
struct PhysicalLocation {
    name: String,
    cloud: String,
    region: String,
    bucket: String,
    #[serde(default = "String::new")]
    prefix: String,
    #[serde(default)]
    is_primary: bool,
    #[serde(default)]
    need_warmup: bool,
}

impl Default for PhysicalLocation {
    fn default() -> Self {
        PhysicalLocation {
            name: "".to_string(),
            cloud: "".to_string(),
            region: "".to_string(),
            bucket: "".to_string(),
            prefix: "".to_string(),
            is_primary: false,
            need_warmup: false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RegisterConfiguration {
    bucket: String,
    config: Configuration,
}

#[derive(Debug, Serialize, Deserialize)]
struct Configuration {
    physical_locations: Vec<PhysicalLocation>,
}

impl Default for RegisterConfiguration {
    fn default() -> Self {
        RegisterConfiguration {
            bucket: "default-skybucket".to_string(),
            config: Configuration {
                physical_locations: vec![PhysicalLocation::default()],
            },
        }
    }
}

async fn register(register_config: &str) -> Result<(), Box<dyn std::error::Error>> {
    let register_config: RegisterConfiguration =
        serde_json::from_str(&read_to_string(register_config)?)?;
    let payload =
        serde_json::json!({ "bucket": register_config.bucket, "config": register_config.config });
    let client = reqwest::Client::new();
    let resp = client
        .post("http://localhost:3000/register_buckets")
        .json(&payload)
        .send()
        .await?;

    if resp.status().is_success() {
        println!("Successfully registered.");
    } else {
        println!("Registration failed.");
    }
    Ok(())
}

async fn warmup(
    bucket: &str,
    key: &str,
    warmup_regions: &Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload = serde_json::json!({
        "bucket": bucket,
        "key": key,
        "warmup_regions": warmup_regions,
    });

    let client = reqwest::Client::new();
    let resp = client
        .post("http://localhost:8002/warmup")
        .json(&payload)
        .send()
        .await?;

    if resp.status().is_success() {
        println!("Successfully started warmup.");
    } else {
        println!("Warmup failed.");
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    let matches = App::new("skystore")
        .version("1.0")
        .about("Skystore CLI")
        .subcommand(
            SubCommand::with_name("init")
                .about("Initialize SkyStore")
                .arg(
                    Arg::with_name("CONFIG")
                        .help("Sets the input config file to use, see init_config.json as an example")
                        .required(true)
                        .index(1),
                ),
        )
        .subcommand(
            SubCommand::with_name("register")
                .about("Register buckets in SkyStore")
                .arg(
                    Arg::with_name("REGISTER_CONFIG")
                        .help("Sets the register config file to use, see register_config.json as an example")
                        .required(true)
                        .index(1),
                ),
        )
        .subcommand(
            SubCommand::with_name("warmup")
                .about("Trigger a warmup operation")
                .arg(Arg::with_name("BUCKET").required(true).index(1))
                .arg(Arg::with_name("KEY").required(true).index(2))
                .arg(Arg::with_name("WARMUP_REGIONS").required(true).index(3)),
        )
        .get_matches();

    if let Some(matches) = matches.subcommand_matches("init") {
        let config_file = matches.value_of("CONFIG").unwrap();
        let (skystore_server_proc, registry_proc) = init(config_file);
        let _ = tokio::try_join!(skystore_server_proc, registry_proc);
    }

    if let Some(matches) = matches.subcommand_matches("register") {
        let register_config = matches.value_of("REGISTER_CONFIG").unwrap();
        register(register_config).await.unwrap();
    }
    if let Some(matches) = matches.subcommand_matches("warmup") {
        let bucket = matches.value_of("BUCKET").unwrap();
        let key = matches.value_of("KEY").unwrap();
        let warmup_regions = matches
            .value_of("WARMUP_REGIONS")
            .unwrap()
            .split(',')
            .map(String::from)
            .collect();
        warmup(bucket, key, &warmup_regions).await.unwrap();
    }
}
