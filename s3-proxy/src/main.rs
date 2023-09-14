use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;

use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;
use tracing::info;

mod client_impls;
mod objstore_client;
mod skyproxy;
mod stream_utils;
mod type_utils;

use crate::skyproxy::SkyProxy;
use futures::FutureExt;
use hyper::{Body as HyperBody, Response, StatusCode};
use std::env;
use tower::Service;

#[derive(serde::Deserialize)]
struct WarmupRequest {
    bucket: String,
    key: String,
    warmup_regions: Vec<String>,
}

#[tokio::main]
async fn main() {
    // Exit the process upon panic, this is used for debugging purpose.
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        // .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .init();

    // Setup our proxy object
    // let init_regions = vec!["aws:us-west-1".to_string(), "aws:us-east-2".to_string()];
    // let client_from_region = "aws:us-west-1".to_string();

    let init_regions: Vec<String> = env::var("INIT_REGIONS")
        .expect("INIT_REGIONS must be set")
        .split(',')
        .map(|s| s.to_string())
        .collect();
    let client_from_region: String =
        env::var("CLIENT_FROM_REGION").expect("CLIENT_FROM_REGION must be set");

    let proxy = SkyProxy::new(init_regions, client_from_region).await;

    // Setup S3 service
    // TODO: Add auth and configure virtual-host style domain
    // https://github.com/Nugine/s3s/blob/b0b6878dafee0e08a876bec5239425fc40c01271/crates/s3s-fs/src/main.rs#L58-L66

    // let s3_service = S3ServiceBuilder::new(proxy).build().into_shared();
    // TODO: hardcode for now for mount test
    let mut s3_service = {
        // let mut b = S3ServiceBuilder::new(proxy);
        let mut b = S3ServiceBuilder::new(proxy.clone());
        // Enable authentication
        let access_key = std::env::var("AWS_ACCESS_KEY_ID").expect("ACCESS_KEY must be set");
        let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY").expect("SECRET_KEY must be set");
        b.set_auth(SimpleAuth::from_single(access_key, secret_key));

        // Configure virtual-host style domain?
        // let domain_name = "domain_name".to_string();
        // b.set_base_domain(domain_name);

        b.build().into_shared()
    };

    let service = ServiceBuilder::new()
        .layer(
            TraceLayer::new_for_http()
                .on_request(|req: &hyper::Request<hyper::Body>, _span: &tracing::Span| {
                    info!("{} {} {:?}", req.method(), req.uri(), req.headers());
                })
                .on_response(
                    |res: &hyper::Response<s3s::Body>,
                     latency: std::time::Duration,
                     _span: &tracing::Span| {
                        info!(
                            "{} {}ms: {:?}",
                            res.status(),
                            latency.as_millis(),
                            res.body().bytes()
                        );
                    },
                ),
        )
        .service_fn(move |req: hyper::Request<hyper::Body>| {
            let proxy_clone = proxy.clone();

            println!("req.uri().path(): {:?}", req.uri().path());

            if req.uri().path() == "/warmup_object" {
                async move {
                    if let Ok(body) = hyper::body::to_bytes(req.into_body()).await {
                        // print body
                        println!("body: {:?}", body);
                        let warmup_req: WarmupRequest = serde_json::from_slice(&body).unwrap();

                        let resp_body = match proxy_clone
                            .warmup_object(
                                warmup_req.bucket,
                                warmup_req.key,
                                warmup_req.warmup_regions,
                            )
                            .await
                        {
                            Ok(_) => "Warmup completed.",
                            Err(_) => "Warmup failed.",
                        };

                        let s3s_body = s3s::Body::from(HyperBody::from(resp_body));
                        Ok(Response::new(s3s_body))
                    } else {
                        let error_body =
                            s3s::Body::from(HyperBody::from("Failed to parse request."));
                        Ok(Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(error_body)
                            .unwrap())
                    }
                }
                .boxed()
            } else {
                s3_service.call(req)
            }
        });

    // Run server
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 8002));
    info!("Starting server on {}", addr);
    hyper::Server::bind(&addr)
        .serve(tower::make::Shared::new(service))
        .await
        .expect("server error");
}
