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

use std::{collections::BTreeSet, net::ToSocketAddrs, sync::Arc};

use ahash::{HashMap, HashMapExt};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use http::Extensions;

use crate::network::socket::SocketAddress;

use super::backend::Backend;

/// the interface to discover backends
#[async_trait]
pub trait ServiceDiscovery {
    /// return the discovered backends
    async fn discover(&self) -> tokio::io::Result<(BTreeSet<Backend>, HashMap<u64, bool>)>;
}

/// a static storage that store collection of backends
pub struct BackendStorage {
    backends: ArcSwap<BTreeSet<Backend>>,
}

impl BackendStorage {
    /// new storage from backends
    pub fn new(backends: BTreeSet<Backend>) -> Box<Self> {
        Box::new(BackendStorage {
            backends: ArcSwap::new(Arc::new(backends)),
        })
    }

    /// new storage from an iterator backends
    pub fn from_iterator<A, T: IntoIterator<Item = A>>(iter: T) -> tokio::io::Result<Box<Self>>
    where
        A: ToSocketAddrs,
    {
        let mut backends = BTreeSet::new();
        for address in iter.into_iter() {
            let address = address.to_socket_addrs()?.map(|addr| Backend {
                address: SocketAddress::Tcp(addr),
                weight: 1,
                extension: Extensions::new(),
            });
            backends.extend(address);
        }
        Ok(Self::new(backends))
    }

    /// set the new backends
    pub fn set(&self, backends: BTreeSet<Backend>) {
        self.backends.store(backends.into());
    }

    /// get the stored backends
    pub fn get(&self) -> BTreeSet<Backend> {
        BTreeSet::clone(&self.backends.load())
    }

    /// add backend into storage backends
    pub fn add(&self, backend: Backend) {
        let mut new = self.get();
        new.insert(backend);
        self.set(new);
    }

    /// remove backend from storage backends
    pub fn remove(&self, backend: &Backend) {
        let mut new = self.get();
        new.remove(backend);
        self.set(new);
    }
}

#[async_trait]
impl ServiceDiscovery for BackendStorage {
    async fn discover(&self) -> tokio::io::Result<(BTreeSet<Backend>, HashMap<u64, bool>)> {
        let health = HashMap::new();
        Ok((self.get(), health))
    }
}
