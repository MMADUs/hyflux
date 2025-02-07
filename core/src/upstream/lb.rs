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

use std::{
    collections::{BTreeSet, HashSet},
    net::ToSocketAddrs,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;

use arc_swap::ArcSwap;
use futures::FutureExt;
use tokio::sync::watch;

use crate::server::task::BackgroundTask;

use super::{
    backend::{Backend, Backends},
    discovery,
    health::HealthCheck,
};

/// backend iterator interface
pub trait BackendIterator {
    /// return some backend
    fn next(&mut self) -> Option<&Backend>;
}

/// interface for selecting backend
pub trait BackendSelection {
    /// return type for the iter below
    type Iter;
    /// build with backends
    fn build(backends: &BTreeSet<Backend>) -> Self;
    /// return the backend iterator
    fn iter(self: &Arc<Self>, key: &[u8]) -> Self::Iter
    where
        Self::Iter: BackendIterator;
}

/// interface for backend selection algoritm
pub trait SelectionAlgorithm {
    /// new algorithm implementation
    fn new() -> Self;
    /// return the index of the backend
    fn next(&self, key: &[u8]) -> u64;
}

pub struct UniqueIterator<I>
where
    I: BackendIterator,
{
    iter: I,
    seen: HashSet<u64>,
    max_iterations: usize,
    steps: usize,
}

impl<I> UniqueIterator<I>
where
    I: BackendIterator,
{
    pub fn new(iter: I, max_iterations: usize) -> Self {
        UniqueIterator {
            iter,
            seen: HashSet::new(),
            max_iterations,
            steps: 0,
        }
    }

    pub fn get_next(&mut self) -> Option<Backend> {
        while let Some(backend) = self.iter.next() {
            if self.steps >= self.max_iterations {
                return None;
            }
            self.steps += 1;
            let hash = backend.get_hash();
            if !self.seen.contains(&hash) {
                self.seen.insert(hash);
                return Some(backend.clone());
            }
        }
        None
    }
}

/// the main backend load balancer
pub struct LoadBalancer<S> {
    /// backends to load balance
    backends: Backends,
    /// backend selector algorithm
    selector: ArcSwap<S>,
    /// health check frequency, if None only run at beginning
    pub health_check_frequency: Option<Duration>,
    /// backends update with dicovery frequency, if None only run at beginning
    pub update_frequency: Option<Duration>,
    /// parallel health check for faster health checks
    pub parallel_health_check: bool,
}

impl<'a, S: BackendSelection> LoadBalancer<S>
where
    S: BackendSelection + 'static,
    S::Iter: BackendIterator,
{
    /// build load balancer from backends
    pub fn from_backends(backends: Backends) -> Self {
        let collection = backends.get_backends();
        let selector = ArcSwap::new(Arc::new(S::build(&collection)));
        LoadBalancer {
            backends,
            selector,
            health_check_frequency: None,
            update_frequency: None,
            parallel_health_check: false,
        }
    }

    /// update the backends with discovery
    pub async fn update(&self) -> tokio::io::Result<()> {
        self.backends
            .update(|backends| self.selector.store(Arc::new(S::build(&backends))))
            .await
    }

    /// build load balancer from an iterator backends
    pub fn from_iterator<A, T: IntoIterator<Item = A>>(iter: T) -> tokio::io::Result<Self>
    where
        A: ToSocketAddrs,
    {
        let discovery = discovery::BackendStorage::from_iterator(iter)?;
        let backends = Backends::new(discovery);
        let lb = Self::from_backends(backends);
        lb.update()
            .now_or_never()
            .expect("static should not block")
            .expect("static should not error");
        Ok(lb)
    }

    /// select a healthy backend
    pub fn select(&self, key: &[u8], max_iterations: usize) -> Option<Backend> {
        self.select_with(key, max_iterations, |_, health| health)
    }

    /// make a callback before selecting a healthy backend
    pub fn select_with<F>(&self, key: &[u8], max_iterations: usize, accept: F) -> Option<Backend>
    where
        F: Fn(&Backend, bool) -> bool,
    {
        let selection = self.selector.load();
        let mut iter = UniqueIterator::new(selection.iter(key), max_iterations);
        while let Some(b) = iter.get_next() {
            if accept(&b, self.backends.ready(&b)) {
                return Some(b);
            }
        }
        None
    }

    /// set health check to load balancer
    pub fn set_health_check(&mut self, hc: Box<dyn HealthCheck + Send + Sync + 'static>) {
        self.backends.set_health_check(hc);
    }

    /// get the backends from load balancer
    pub fn backends(&self) -> &Backends {
        &self.backends
    }
}

/// implement load balancer as a background task executed by process
#[async_trait]
impl<S> BackgroundTask for LoadBalancer<S>
where
    S: BackendSelection + Send + Sync + 'static,
    S::Iter: BackendIterator,
{
    async fn run_task(&self, shutdown_notifier: watch::Receiver<bool>) {
        // 136 years
        const NEVER: Duration = Duration::from_secs(u32::MAX as u64);
        let mut now = Instant::now();
        // run update and healthcheck once
        let mut next_backend_update = now;
        let mut next_health_check = now;
        // infinite loop
        loop {
            // stop task immediately on shutdown
            if *shutdown_notifier.borrow() {
                return;
            }
            // do backend update
            if next_backend_update <= now {
                let _ = self.update().await;
                next_backend_update = now + self.update_frequency.unwrap_or(NEVER);
            }
            // do health check update
            if next_health_check <= now {
                self.backends
                    .run_health_check(self.parallel_health_check)
                    .await;
                next_health_check = now + self.health_check_frequency.unwrap_or(NEVER);
            }
            // if frequency are none, stop the task
            if self.update_frequency.is_none() && self.health_check_frequency.is_none() {
                return;
            }
            // continue task
            let to_wake = std::cmp::min(next_backend_update, next_health_check);
            tokio::time::sleep_until(to_wake.into()).await;
            now = Instant::now();
        }
    }
}
