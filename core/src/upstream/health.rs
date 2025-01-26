//! Copyright (c) 2024-2025 Hyflux, Inc.
//!
//! This file is part of Hyflux
//!
//! This program is free software: you can redistribute it and/or modify
//! it under the terms of the GNU Affero General Public License as published by
//! the Free Software Foundation, either version 3 of the License, or
//! (at your option) any later version.
//!
//! This program is distributed in the hope that it will be useful
//! but WITHOUT ANY WARRANTY; without even the implied warranty of
//! MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//! GNU Affero General Public License for more details.
//!
//! You should have received a copy of the GNU Affero General Public License
//! along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::sync::Arc;

use crate::{
    network::socket::SocketAddress,
    pool::manager::StreamManager,
    service::peer::UpstreamPeer,
    session::{request::RequestHeader, response::ResponseHeader, upstream::Upstream},
};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use http::Version;

use super::backend::Backend;

/// interface for observation health changes callback
#[async_trait]
pub trait HealthObservation {
    /// observe the backend health
    async fn observe(&self, target: &Backend, healthy: bool);
}

/// callback type for health observation
pub type HealthObservationCallback = Box<dyn HealthObservation + Send + Sync>;

/// interface to implement backend health checks
#[async_trait]
pub trait HealthCheck {
    /// health check the given backend
    async fn check(&self, target: &Backend) -> tokio::io::Result<()>;
    /// called when backend health changes
    async fn health_status_callback(&self, _target: &Backend, _healthy: bool) {}
    /// get the number of consecutive either success or failed
    fn health_threshold(&self, success: bool) -> usize;
}

/// Tcp health check
pub struct TcpHealthCheck {
    /// upstream peer
    peer: UpstreamPeer,
    /// number of successful checks to switch from unhealthy to healthy
    pub consecutive_success: usize,
    /// number of failed checks to switch from healthy to unhealthy
    pub consecutive_failure: usize,
    /// manager used as connector to connection pool
    manager: StreamManager,
    /// a callback is invoked when a healthy backend status changed
    pub health_change_callback: Option<HealthObservationCallback>,
}

impl TcpHealthCheck {
    pub fn new() -> Box<Self> {
        // temporary address
        let tmp_addr = SocketAddress::parse_tcp("0.0.0.0:1");
        let hc = TcpHealthCheck {
            peer: UpstreamPeer::new("", "", tmp_addr),
            consecutive_success: 1,
            consecutive_failure: 1,
            manager: StreamManager::new(None),
            health_change_callback: None,
        };
        Box::from(hc)
    }

    // pub fn with_tls() -> Box<Self> {}

    pub fn set_manager(&mut self, manager: StreamManager) {
        self.manager = manager;
    }
}

#[async_trait]
impl HealthCheck for TcpHealthCheck {
    async fn check(&self, target: &Backend) -> tokio::io::Result<()> {
        let mut observation_peer = self.peer.clone();
        observation_peer.address = target.address.clone();
        self.manager
            .get_connection_from_pool(&observation_peer)
            .await
            .map(|_| {})
    }

    async fn health_status_callback(&self, target: &Backend, healthy: bool) {
        if let Some(callback) = &self.health_change_callback {
            callback.observe(target, healthy).await;
        }
    }

    fn health_threshold(&self, success: bool) -> usize {
        if success {
            self.consecutive_success
        } else {
            self.consecutive_failure
        }
    }
}

/// type used for response validation
type Validator = Box<dyn Fn(&ResponseHeader) -> tokio::io::Result<()> + Send + Sync>;

/// health check strategy with HTTP(s) response from the given backend
pub struct HttpHealthCheck {
    /// upstream peer
    peer: UpstreamPeer,
    /// number of successful checks to switch from unhealthy to healthy
    pub consecutive_success: usize,
    /// number of failed checks to switch from healthy to unhealthy
    pub consecutive_failure: usize,
    /// flag for connection reuse
    pub reuse_connection: bool,
    /// request header to be send to backend
    pub req: RequestHeader,
    /// manager used as connector to connection pool
    manager: StreamManager,
    /// backend response validator
    validator: Option<Validator>,
    /// perform on a given diffrent port
    port_override: Option<u16>,
    /// a callback is invoked when a healthy backend status changed
    pub health_change_callback: Option<HealthObservationCallback>,
}

impl HttpHealthCheck {
    pub fn new(host: &str, _tls: bool) -> Self {
        let mut req = RequestHeader::build("GET", "/", Version::HTTP_11, None);
        req.append_header("Host", host);
        let tmp_addr = SocketAddress::parse_tcp("0.0.0.0:1");
        HttpHealthCheck {
            peer: UpstreamPeer::new("", "", tmp_addr),
            consecutive_success: 1,
            consecutive_failure: 1,
            reuse_connection: false,
            req,
            manager: StreamManager::new(None),
            validator: None,
            port_override: None,
            health_change_callback: None,
        }
    }

    pub fn set_manager(&mut self, manager: StreamManager) {
        self.manager = manager;
    }
}

#[async_trait]
impl HealthCheck for HttpHealthCheck {
    async fn check(&self, target: &Backend) -> tokio::io::Result<()> {
        let mut observation_peer = self.peer.clone();
        observation_peer.address = target.address.clone();
        if let Some(port) = self.port_override {
            observation_peer.address.set_port(port);
        }
        // make session from stream
        let stream = self
            .manager
            .get_connection_from_pool(&observation_peer)
            .await?;
        let mut session = Upstream::new(stream.0);
        let req = self.req.clone();
        // send reqest & get response
        session.write_request_header(req).await?;
        session.read_response().await?;
        let response = session.get_response_headers();
        // reponse validator
        if let Some(validator) = self.validator.as_ref() {
            validator(response)?;
        } else {
            // return error here
        }
        while session.read_response_body().await?.is_some() {
            // drain body
        }
        // reuse stream
        if self.reuse_connection {
            let stream = session.return_stream();
            self.manager
                .return_connection_to_pool(stream, &observation_peer)
                .await;
        }
        Ok(())
    }

    async fn health_status_callback(&self, target: &Backend, healthy: bool) {
        if let Some(callback) = &self.health_change_callback {
            callback.observe(target, healthy).await;
        }
    }

    fn health_threshold(&self, success: bool) -> usize {
        if success {
            self.consecutive_success
        } else {
            self.consecutive_failure
        }
    }
}

/// health status metadata
#[derive(Clone)]
struct HealthMeta {
    /// health status flag
    healthy: bool,
    enabled: bool,
    consecutive_counter: usize,
}

/// health of backend that can be updated atomically
pub struct Health(ArcSwap<HealthMeta>);

impl Clone for Health {
    fn clone(&self) -> Self {
        let meta = self.0.load_full();
        Health(ArcSwap::new(meta))
    }
}

impl Health {
    pub fn new() -> Self {
        Health(ArcSwap::new(Arc::new(HealthMeta {
            healthy: false,
            enabled: true,
            consecutive_counter: 0,
        })))
    }

    /// check if health is ready
    pub fn ready(&self) -> bool {
        let health = self.0.load();
        health.healthy && health.enabled
    }

    /// set the health enabled flag
    pub fn enable(&self, enabled: bool) {
        let health = self.0.load();
        if health.enabled != enabled {
            // clone the metadata
            let mut new_health = (**health).clone();
            new_health.enabled = enabled;
            self.0.store(Arc::new(new_health));
        }
    }

    /// observe the health from metadata
    pub fn observe_health(&self, health: bool, flip_treshold: usize) -> bool {
        let h = self.0.load();
        let mut flipped = false;
        if h.healthy != health {
            let mut new_health = (**h).clone();
            new_health.consecutive_counter += 1;
            if new_health.consecutive_counter >= flip_treshold {
                new_health.healthy = health;
                new_health.consecutive_counter = 0;
                flipped = true;
            }
            self.0.store(Arc::new(new_health));
        } else if h.consecutive_counter > 0 {
            let mut new_health = (**h).clone();
            new_health.consecutive_counter = 0;
            self.0.store(Arc::new(new_health));
        }
        flipped
    }
}
