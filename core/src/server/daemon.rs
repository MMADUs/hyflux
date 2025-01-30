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

use std::ffi::CString;
use std::fs::{self, OpenOptions};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use daemonize::Daemonize;
use tracing::{error, info, warn};

/// daemon process configuration
pub struct DaemonConfig {
    /// process id file path
    pid_path: String,
    /// error log file path
    error_log: Option<String>,
    /// unix username permission
    user: Option<String>,
    /// unix group permission
    group: Option<String>,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            pid_path: String::from("/tmp/hyflux.dpid"),
            error_log: None,
            user: None,
            group: None,
        }
    }
}

/// helper function to replace the old pid file to fresh pid file
fn replace_pid(path: &str) {
    if !Path::new(path).exists() {
        warn!("old pid file does not exist");
        return;
    }
    let new_path = format!("{path}.dpid");
    match fs::rename(path, &new_path) {
        Ok(_) => info!("old pid file renamed"),
        Err(e) => error!("failed to rename pid file from {path} to {new_path}: {e}"),
    }
}

/// get group id by the given username
unsafe fn get_gid(username: &CString) -> Option<libc::gid_t> {
    let passwd = libc::getpwnam(username.as_ptr() as *const libc::c_char);
    if !passwd.is_null() {
        return Some((*passwd).pw_gid);
    }
    None
}

/// main process to start daemonized the server process
pub fn daemonize_server(config: &DaemonConfig) {
    let daemonize = Daemonize::new().umask(0o007).pid_file(&config.pid_path);
    // check if error log file is provided
    let daemonize = if let Some(error_log) = config.error_log.as_ref() {
        let error = OpenOptions::new()
            .append(true)
            .create(true)
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(error_log)
            .unwrap();
        daemonize.stderr(error)
    } else {
        daemonize
    };
    // configure user-specific settings
    let daemonize = match config.user.as_ref() {
        Some(user) => {
            let user_cstr = CString::new(user.as_str()).unwrap();

            let group_id = unsafe { get_gid(&user_cstr) };

            daemonize
                .privileged_action(move || {
                    if let Some(gid) = group_id {
                        unsafe {
                            libc::initgroups(user_cstr.as_ptr() as *const libc::c_char, gid);
                        }
                    }
                })
                .user(user.as_str())
                .chown_pid_file(true)
        }
        None => daemonize,
    };
    // set group
    let daemonize = match config.group.as_ref() {
        Some(group) => daemonize.group(group.as_str()),
        None => daemonize,
    };
    // replace pid path if old path exist
    replace_pid(&config.pid_path);
    // start daemon process
    daemonize.start().unwrap()
}
