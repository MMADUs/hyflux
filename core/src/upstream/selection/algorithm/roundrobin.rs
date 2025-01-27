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

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::upstream::{lb::SelectionAlgorithm, selection::weighted::Weighted};

/// the selection type for round robin
pub struct RoundRobinSelection (AtomicUsize);

impl SelectionAlgorithm for RoundRobinSelection {
    fn new() -> Self {
        Self(AtomicUsize::new(0))
    }

    fn next(&self, _key: &[u8]) -> u64 {
        self.0.fetch_add(1, Ordering::Relaxed) as u64
    }
}

/// the round robin algorithm
pub type RoundRobin = Weighted<RoundRobinSelection>;
