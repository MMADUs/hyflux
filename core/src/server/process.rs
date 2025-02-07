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

use super::fd::ListenerFd;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{watch, Mutex};

/// the process interface for running a process in the system
/// the process refers to anything that used a certain threads & individual runtime
#[async_trait]
pub trait Process: Send + Sync {
    /// function called when the server starts every process
    /// often used as a running process in the background
    async fn start_process(
        &mut self,
        listener_fd: Option<Arc<Mutex<ListenerFd>>>,
        shutdown_notifier: watch::Receiver<bool>,
    );

    /// each process creates a new runtime with the given threads
    /// give the runtime a name
    fn process_name(&self) -> String;

    /// set the threads to be allocated in the process
    /// the default runtime thread is set to 1
    fn alloc_threads(&self) -> Option<usize> {
        Some(1)
    }
}
