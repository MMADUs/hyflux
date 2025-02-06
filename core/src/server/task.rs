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

use async_trait::async_trait;
use tokio::sync::watch;

/// the running background task interface
#[async_trait]
pub trait BackgroundTask {
    /// a function is called when background task starts
    async fn run_task(&self, mut shutdown_notifier: watch::Receiver<bool>);
}

/// type for executable task
pub struct Task<T>
where
    T: BackgroundTask + Send + Sync + 'static,
{
    name: String,
    task: Arc<T>,
    threads: Option<usize>,
}

impl<T> Task<T>
where
    T: BackgroundTask + Send + Sync + 'static,
{
    /// make a new executable task
    pub fn new(name: &str, task: T, threads: Option<usize>) -> Self {
        Task {
            name: name.to_string(),
            task: Arc::new(task),
            threads,
        }
    }

    /// execute task
    pub async fn execute(&self, shutdown_notifier: watch::Receiver<bool>) {
        self.task.run_task(shutdown_notifier).await;
    }
}
