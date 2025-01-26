use arc_swap::ArcSwap;
use http::Extensions;
use derivative::Derivative;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeSet, HashMap};
use std::hash::{Hash, Hasher};

use crate::network::socket::SocketAddress;

#[derive(Derivative)]
#[derivative(Clone, Hash, Debug)]
pub struct Backend {
    /// backend server address
    pub address: SocketAddress,
    /// traffic weight
    pub weight: usize,
    /// used to store some extensive data into the backend
    #[derivative(Hash = "ignore")]
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

pub struct Backends {
    backends: ArcSwap<BTreeSet<Backend>>,
}

impl Backends {

}
