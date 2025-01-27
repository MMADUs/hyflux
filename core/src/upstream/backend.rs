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

use arc_swap::ArcSwap;
use derivative::Derivative;
use http::Extensions;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::runtime::Handle;
use tracing::warn;

use crate::network::socket::SocketAddress;

use super::discovery::ServiceDiscovery;
use super::health::{Health, HealthCheck};

/// the backend node type
#[derive(Derivative)]
#[derivative(Clone, Hash, PartialEq, PartialOrd, Eq, Ord, Debug)]
pub struct Backend {
    /// backend server address
    pub address: SocketAddress,
    /// traffic weight
    pub weight: usize,
    /// used to store some extensive data into the backend
    #[derivative(PartialEq = "ignore")]
    #[derivative(PartialOrd = "ignore")]
    #[derivative(Hash = "ignore")]
    #[derivative(Ord = "ignore")]
    pub extension: Extensions,
}

impl Backend {
    pub fn new(address: &str, weight: Option<usize>) -> Self {
        Self {
            address: SocketAddress::parse_tcp(address),
            weight: weight.unwrap_or(1),
            extension: Extensions::new(),
        }
    }

    pub fn get_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

/// a collection of backends with health check & discovery
pub struct Backends {
    /// atomic collection of backends
    backends: ArcSwap<BTreeSet<Backend>>,
    /// service discovery
    discovery: Box<dyn ServiceDiscovery + Send + Sync + 'static>,
    /// health checks
    health_check: Option<Arc<dyn HealthCheck + Send + Sync + 'static>>,
    /// atomic health table of metadata
    health: ArcSwap<HashMap<u64, Health>>,
}

impl Backends {
    /// new backends with discovery
    pub fn new(discovery: Box<dyn ServiceDiscovery + Send + Sync + 'static>) -> Self {
        Backends {
            backends: Default::default(),
            discovery,
            health_check: None,
            health: Default::default(),
        }
    }

    /// set backends health check
    pub fn set_health_check(&mut self, health_check: Box<dyn HealthCheck + Send + Sync + 'static>) {
        self.health_check = Some(health_check.into())
    }

    /// update backends with the new discovered backends
    pub async fn update<F>(&self, callback: F) -> tokio::io::Result<()>
    where
        F: Fn(Arc<BTreeSet<Backend>>),
    {
        // discover backends
        let (new_backends, enablement) = self.discovery.discover().await?;
        // check if its diffrent from the current set
        if (**self.backends.load()) != new_backends {
            let old_health = self.health.load();
            let mut health = HashMap::with_capacity(new_backends.len());
            for backend in new_backends.iter() {
                let hash = backend.get_hash();
                let backend_health = old_health.get(&hash).cloned().unwrap_or(Health::new());
                // override enablement
                if let Some(backend_enabled) = enablement.get(&hash) {
                    backend_health.enable(*backend_enabled);
                }
                health.insert(hash, backend_health);
            }
            // store backends
            let new_backends = Arc::new(new_backends);
            callback(new_backends.clone());
            self.backends.store(new_backends);
            self.health.store(Arc::new(health));
        } else {
            // no updates, just check enablement
            for (hash, backend_enabled) in enablement.iter() {
                if let Some(backend_health) = self.health.load().get(hash) {
                    backend_health.enable(*backend_enabled);
                }
            }
        }
        Ok(())
    }

    /// check if the backend is ready to serve traffic
    pub fn ready(&self, backend: &Backend) -> bool {
        let hash = backend.get_hash();
        self.health
            .load()
            .get(&hash)
            .map_or(self.health_check.is_none(), |h| h.ready())
    }

    /// manually set if the backend is ready to serve traffic
    pub fn set_enable(&self, backend: &Backend, enabled: bool) {
        let hash = backend.get_hash();
        if let Some(health) = self.health.load().get(&hash) {
            health.enable(enabled)
        }
    }

    /// get the collection of backends
    pub fn get_backends(&self) -> Arc<BTreeSet<Backend>> {
        self.backends.load_full()
    }

    /// run health check for all backends
    pub async fn run_health_check(&self, parallel: bool) {
        // internal function to observe health
        async fn observe(
            backend: &Backend,
            hc: &Arc<dyn HealthCheck + Send + Sync>,
            health_table: &HashMap<u64, Health>,
        ) {
            // check for health failure
            let errored = hc.check(backend).await.err();
            let hash = backend.get_hash();
            // observe health
            if let Some(h) = health_table.get(&hash) {
                let flip_treshold = hc.health_threshold(errored.is_none());
                let flipped = h.observe_health(errored.is_none(), flip_treshold);
                if flipped {
                    hc.health_status_callback(backend, errored.is_none()).await;
                    if let Some(e) = errored {
                        warn!("{backend:?} becomes unhealthy, {e}");
                    } else {
                        warn!("{backend:?} becomes healthy");
                    }
                }
            }
        }
        // checks if health check exist, otherwise not running the health observation check
        let Some(health_check) = self.health_check.as_ref() else {
            return;
        };
        // run the observations
        let backends = self.backends.load();
        if parallel {
            let health_table = self.health.load_full();
            let runtime = Handle::current();
            let jobs = backends.iter().map(|backend| {
                let backend = backend.clone();
                let hc = health_check.clone();
                let ht = health_table.clone();
                runtime.spawn(async move {
                    observe(&backend, &hc, &ht).await;
                })
            });
            futures::future::join_all(jobs).await;
        } else {
            for backend in backends.iter() {
                observe(backend, health_check, &self.health.load()).await;
            }
        }
    }
}
