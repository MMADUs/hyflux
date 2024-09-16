/**
 * Copyright (c) 2024-2025 ArcX, Inc.
 *
 * This file is part of ArcX Gateway
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

use std::collections::HashMap;
use std::time::Duration;

use pingora::prelude::{HttpPeer};
use pingora::proxy::{ProxyHttp, Session};
use pingora::{Error, Result};
use pingora::http::ResponseHeader;

use async_trait::async_trait;

use crate::config::ClusterMetadata;
use crate::cluster::select_cluster;
use crate::limiter::rate_limiter;

// Main Struct as Router to implement ProxyHttp
pub struct ProxyRouter {
    pub clusters: Vec<ClusterMetadata>,
    pub prefix_map: HashMap<String, usize>,
}

// struct for proxy context
pub struct RouterCtx {
    pub cluster_address: usize,
    pub proxy_retry: usize,
}

#[async_trait]
impl ProxyHttp for ProxyRouter {
    // initialize ctx types
    type CTX = RouterCtx;

    // initial ctx values
    fn new_ctx(&self) -> Self::CTX { RouterCtx {
        cluster_address: 0,
        proxy_retry: 0,
    }}

    /**
    * The upstream_peer is the third phase that executes in the lifecycle
    * if this lifecycle returns the http peer
    */
    async fn upstream_peer(
        &self,
        _session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        // Select the cluster based on the selected index
        let cluster = &self.clusters[ctx.cluster_address];

        // Set up the upstream
        let upstream = cluster.upstream.select(b"", 256).unwrap(); // Hash doesn't matter for round_robin
        println!("upstream peer is: {:?}", upstream);

        // Set SNI to the cluster's host
        let mut peer = Box::new(HttpPeer::new(upstream, false, cluster.host.clone()));

        // given the proxy timeout
        let timeout = cluster.timeout.unwrap_or(100);
        peer.options.connection_timeout = Some(Duration::from_millis(timeout));
        Ok(peer)
    }

    /**
    * The request_filter is the first phase that executes in the lifecycle
    * if this lifecycle returns true = the proxy stops | false = continue to upper lifecycle
    */
    async fn request_filter(
        &self,
        session: &mut Session,
        ctx: &mut Self::CTX
    ) -> Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        // Clone the original request header and get the URI path
        let cloned_req_header = session.req_header().clone();
        let original_uri = cloned_req_header.uri.path();
        println!("original uri: {}", original_uri);

        // result to consider if request should continue or stop
        let mut result: bool = true;

        // select the cluster based on prefix
        result = select_cluster(&self.prefix_map, original_uri, session, ctx).await;

        // Select the cluster based on the selected index
        let cluster = &self.clusters[ctx.cluster_address];

        println!("uri before send to upstream: {}", session.req_header().uri.path());
        println!("The value of my_bool is: {}", result);

        // validate if rate limit exist from config
        match cluster.rate_limit {
            Some(limit) => {
                // rate limit incoming request
                result = rate_limiter(limit, session, ctx).await;
            },
            None => {}
        }
        Ok(result)
    }

    /**
    * The proxy_upstream_filter is the second phase that executes in the lifecycle
    * if this lifecycle returns false = the proxy stops | true = continue to upper lifecycle
    */
    async fn proxy_upstream_filter(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        Ok(true)
    }

    // 5. fifth phase executed
    async fn response_filter(
        &self,
        _session: &mut Session,
        upstream_response: &mut ResponseHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        // Remove content-length because the size of the new body is unknown
        upstream_response.remove_header("Content-Length");
        upstream_response.insert_header("Transfer-Encoding", "Chunked")?;
        Ok(())
    }

    /**
    * The fail_to_connect is the phase that executes after upstream_peer
    * in this lifecycle it checks if request can be retryable or error
    */
    fn fail_to_connect(
        &self,
        _session: &mut Session,
        _peer: &HttpPeer,
        ctx: &mut Self::CTX,
        mut e: Box<Error>,
    ) -> Box<Error> {
        // Select the cluster based on the selected index
        let cluster = &self.clusters[ctx.cluster_address];

        // check if retry reach limits
        if ctx.proxy_retry > cluster.retry.unwrap_or(1) {
            return e;
        }
        // set to be retryable
        ctx.proxy_retry += 1;
        e.set_retry(true);
        e
    }

    // async fn logging(
    //     &self,
    //     session: &mut Session,
    //     _e: Option<&pingora_core::Error>,
    //     ctx: &mut Self::CTX,
    // ) {
    //     let response_code = session
    //         .response_written()
    //         .map_or(0, |resp| resp.status.as_u16());
    //     info!(
    //         "{} response code: {response_code}",
    //         self.request_summary(session, ctx)
    //     );
    //
    //     self.req_metric.inc();
    // }
}