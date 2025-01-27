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

use std::{collections::BTreeSet, sync::Arc};

use crate::upstream::{
    backend::Backend,
    lb::{BackendIterator, BackendSelection, SelectionAlgorithm},
};

/// weighted iterator type
pub struct WeightedIterator<A> {
    index: u64,
    backend: Arc<Weighted<A>>,
    first: bool,
}

impl<A: SelectionAlgorithm> WeightedIterator<A> {
    fn new(input: &[u8], backend: Arc<Weighted<A>>) -> Self {
        WeightedIterator {
            index: backend.algorithm.next(input),
            backend,
            first: true,
        }
    }
}

impl<A: SelectionAlgorithm> BackendIterator for WeightedIterator<A> {
    fn next(&mut self) -> Option<&Backend> {
        if self.backend.backends.is_empty() {
            return None;
        }

        if self.first {
            self.first = false;
            let len = self.backend.weighted.len();
            let index = self.backend.weighted[self.index as usize % len];
            Some(&self.backend.backends[index as usize])
        } else {
            self.index = self.backend.algorithm.next(&self.index.to_le_bytes());
            let len = self.backend.backends.len();
            Some(&self.backend.backends[self.index as usize % len])
        }
    }
}

/// weighted selection type
pub struct Weighted<A> {
    backends: Box<[Backend]>,
    weighted: Box<[u16]>,
    algorithm: A,
}

impl<A: SelectionAlgorithm> BackendSelection for Weighted<A> {
    type Iter = WeightedIterator<A>;

    fn build(backends: &BTreeSet<Backend>) -> Self {
        let backends = Vec::from_iter(backends.iter().cloned()).into_boxed_slice();
        let mut weighted = Vec::with_capacity(backends.len());
        for (index, backend) in backends.iter().enumerate() {
            for _ in 0..backend.weight {
                weighted.push(index as u16);
            }
        }
        Weighted {
            backends,
            weighted: weighted.into_boxed_slice(),
            algorithm: A::new(),
        }
    }

    fn iter(self: &Arc<Self>, key: &[u8]) -> Self::Iter {
        WeightedIterator::new(key, self.clone())
    }
}
