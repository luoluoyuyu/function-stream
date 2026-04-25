// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Minimal async HTTP server that exposes `GET /metrics` for Prometheus
//! scraping.
//!
//! Uses only `tokio::net` (already a project dependency) — no additional HTTP
//! framework is required.  Any path other than `/metrics` returns `404`.

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::metrics::manager::MetricsManager;

/// Spawn a Tokio task that serves `GET /metrics` until `shutdown` resolves.
///
/// # Arguments
///
/// * `bind_address` — TCP address to listen on (e.g. `"0.0.0.0:9090"`).
/// * `manager`     — global [`MetricsManager`]; `gather()` output is returned.
/// * `shutdown`     — future that resolves when the server should stop.
pub async fn serve_metrics<F>(
    bind_address: &str,
    manager: Arc<MetricsManager>,
    shutdown: F,
) -> anyhow::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind(bind_address).await?;
    info!(
        address = bind_address,
        "Prometheus /metrics HTTP server listening"
    );

    tokio::select! {
        _ = accept_loop(listener, manager) => {},
        _ = shutdown => {
            info!("Prometheus /metrics HTTP server shutting down");
        }
    }

    Ok(())
}

async fn accept_loop(listener: TcpListener, manager: Arc<MetricsManager>) {
    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                let manager = manager.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, manager).await {
                        error!(peer = %peer_addr, error = %e, "Metrics HTTP handler error");
                    }
                });
            }
            Err(e) => {
                error!(error = %e, "Failed to accept metrics connection");
            }
        }
    }
}

async fn handle_connection(
    mut stream: tokio::net::TcpStream,
    manager: Arc<MetricsManager>,
) -> anyhow::Result<()> {
    let mut buf = [0u8; 4096];
    let n = stream.read(&mut buf).await?;
    let request = std::str::from_utf8(&buf[..n]).unwrap_or("");

    let response = if request.starts_with("GET /metrics") {
        let body = manager.gather();
        format!(
            "HTTP/1.1 200 OK\r\n\
             Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
             Content-Length: {}\r\n\
             \r\n\
             {}",
            body.len(),
            body
        )
    } else {
        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n".to_string()
    };

    stream.write_all(response.as_bytes()).await?;
    Ok(())
}
