// Copyright (c) 2024-2025 ArcX, Inc.
//
// This file is part of ArcX Gateway
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::collections::HashMap;
use async_trait::async_trait;

use std::sync::Arc;
use std::fs::File;

use pingora::lb::LoadBalancer;
use pingora::prelude::{background_service, HttpPeer, Opt, RoundRobin, TcpHealthCheck};
use pingora::proxy::{http_proxy_service, ProxyHttp, Session};
use pingora::server::Server;
use pingora::{Error, HTTPStatus, Result};
use pingora::services::background::GenBackgroundService;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct ClusterConfig {
    name: String,
    prefix: String,
    upstreams: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct Config {
    clusters: Vec<ClusterConfig>,
}

struct Router {
    clusters: Vec<Arc<LoadBalancer<RoundRobin>>>,
    prefix_map: HashMap<String, usize>,
}

#[async_trait]
impl ProxyHttp for Router {
    type CTX = ();
    fn new_ctx(&self) {}

    async fn upstream_peer(&self, session: &mut Session, _ctx: &mut ()) -> Result<Box<HttpPeer>> {
        // Get the request path
        let path = session.req_header().uri.path().to_string();

        // Find the cluster address based on the URI prefix
        let cluster_idx = self
            .prefix_map
            .iter()
            .find(|(prefix, _)| path.starts_with(prefix.as_str()))
            .map(|(_, &idx)| idx)
            .unwrap_or_else(|| {
                // Default to the first cluster if no prefix matches
                0
            });

        // Select the cluster based on the selected address
        let cluster = &self.clusters[cluster_idx];

        // Set up the upstream
        let upstream = cluster.select(b"", 256).unwrap(); // Hash doesn't matter for round robin

        println!("upstream peer is: {:?}", upstream);
        println!("cluster idx is: {:?}", cluster_idx);
        println!("cluster path is: {:?}", path);

        // Set SNI to the clusters host
        let peer = Box::new(HttpPeer::new(upstream, false, "host.docker.internal".to_string()));
        Ok(peer)
    }
}

fn load_config(file_path: &str) -> Config {
    let file = File::open(file_path).expect("Unable to open the file");
    serde_yaml::from_reader(file).expect("Unable to parse YAML")
}

fn build_cluster_service(
    upstreams: &[&str],
) -> GenBackgroundService<LoadBalancer<RoundRobin>> {
    let mut cluster = LoadBalancer::try_from_iter(upstreams).unwrap();
    cluster.set_health_check(TcpHealthCheck::new());
    cluster.health_check_frequency = Some(std::time::Duration::from_secs(1));
    background_service("cluster health check", cluster)
}

fn main() {
    // Setup a server
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt)).unwrap();
    my_server.bootstrap();

    // Read config from the yaml
    let config = load_config("config.yaml");

    // List of clusters and prefix
    let mut clusters = Vec::new();
    let mut prefix_map = HashMap::new();

    // Set up a cluster based on config
    for (idx, clusterConf) in config.clusters.iter().enumerate() {
        let cluster_service = build_cluster_service(
            &clusterConf.upstreams.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        );

        // Add the cluster to the list
        clusters.push(cluster_service.task());
        my_server.add_service(cluster_service);

        // Add the prefix to the prefix list
        prefix_map.insert(clusterConf.prefix.clone(), idx);
        println!("Setting up cluster: {}", idx)
    }

    // Set the list of clusters into routes
    let router = Router{
        clusters,
        prefix_map,
    };

    // Build the proxy with the list of clusters
    let mut router_service = http_proxy_service(&my_server.configuration, router);

    // Proxy server port
    router_service.add_tcp("0.0.0.0:6188");

    // Set the proxy to the server
    my_server.add_service(router_service);
    my_server.run_forever();
}