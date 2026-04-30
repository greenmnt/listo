mod classify;
mod config;
mod db;
mod error;
mod geo;
mod service;

pub mod pb {
    tonic::include_proto!("listo.v1");
}

use http::header::HeaderName;
use http::Method;
use tonic::transport::Server;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                EnvFilter::new(
                    "listo_api=info,tower_http=warn,sqlx=warn,tonic=info,h2=warn",
                )
            }),
        )
        .with_target(false)
        .compact()
        .init();

    let cfg = config::Config::from_env()?;
    let pool = db::connect(&cfg.database_url).await?;
    tracing::info!("connected to db; binding {}", cfg.bind_addr);

    let svc = service::ListoServiceImpl::new(pool);

    // CORS for the Vite dev server. Mirror = reflect the request's
    // Origin/Headers/Method. Tonic's HTTP/1.1 router rejects OPTIONS
    // before tower layers see it when we use GrpcWebLayer at the
    // server level — using `tonic_web::enable()` per-service makes the
    // wrapper handle OPTIONS preflights, then this CORS layer sets the
    // response headers on the way back out.
    let cors = CorsLayer::new()
        .allow_origin(tower_http::cors::AllowOrigin::mirror_request())
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers(tower_http::cors::AllowHeaders::mirror_request())
        .expose_headers([
            HeaderName::from_static("grpc-status"),
            HeaderName::from_static("grpc-message"),
            HeaderName::from_static("grpc-status-details-bin"),
            HeaderName::from_static("grpc-encoding"),
            HeaderName::from_static("grpc-accept-encoding"),
        ])
        .allow_credentials(false)
        .max_age(std::time::Duration::from_secs(60 * 60 * 24));

    let addr = cfg.bind_addr.parse()?;
    tracing::info!("listening on http://{}", addr);

    Server::builder()
        // Required for tonic-web — browsers can't speak HTTP/2 cleartext, so
        // we accept HTTP/1.1 and tonic_web::enable() wraps each service
        // to translate gRPC-Web frames + handle OPTIONS preflights.
        .accept_http1(true)
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .add_service(tonic_web::enable(pb::listo_server::ListoServer::new(svc)))
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.ok();
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    tracing::info!("shutdown signal received");
}
