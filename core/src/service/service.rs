use futures::future;
use std::sync::Arc;

use crate::listener::listener::{ListenerAddress, Socket};
use crate::listener::socket::SocketAddress;
use crate::pool::stream::StreamManager;
use crate::service::peer::UpstreamPeer;
use crate::stream::stream::Stream;

// TESTING traits for customization soon
pub trait ServiceType: Send + Sync + 'static {
    fn say_hi(&self) -> String;
}

// the network stack is used for network configurations
// this held many network for 1 service
pub struct NetworkStack {
    pub address_stack: Vec<ListenerAddress>,
}

impl NetworkStack {
    // new network stack
    pub fn new() -> Self {
        NetworkStack {
            address_stack: Vec::new(),
        }
    }

    // add tcp address to network list
    pub fn new_tcp_address(&mut self, addr: &str) {
        let tcp_address = ListenerAddress::Tcp(addr.to_string());
        self.address_stack.push(tcp_address);
    }

    // add unix socket path to network list
    pub fn new_unix_path(&mut self, path: &str) {
        let unix_path = ListenerAddress::Unix(path.to_string());
        self.address_stack.push(unix_path);
    }
}

// used to build service
// each service can serve on multiple network
// many service is served to the main server
pub struct Service<A> {
    name: String,
    service: A,
    network: NetworkStack,
    stream_session: StreamManager,
}

// service implementation mainly for managing service
impl<A> Service<A> {
    // new service
    pub fn new(name: &str, service_type: A) -> Self {
        Service {
            name: name.to_string(),
            service: service_type,
            network: NetworkStack::new(),
            stream_session: StreamManager::new(None),
        }
    }

    // add new tcp address to service
    pub fn add_tcp_network(&mut self, address: &str) {
        self.network.new_tcp_address(address);
    }

    // add new unix socket path to service
    pub fn add_unix_socket(&mut self, path: &str) {
        self.network.new_unix_path(path);
    }

    // this is probably getting rid soon,
    // just some temporary to get things working
    pub fn get_address_stack(&self) -> Vec<ListenerAddress> {
        self.network.address_stack.clone()
    }
}

// service implementation mainly for running the service
impl<A: ServiceType + Send + Sync + 'static> Service<A> {
    // for starting up service
    pub async fn start_service(self: &Arc<Self>, address_stack: Vec<ListenerAddress>) {
        let handlers = address_stack.into_iter().map(|network| {
            // cloning the arc self is used to keep sharing reference in multithread.
            // same as any method that calls self
            let service = Arc::clone(self);
            tokio::spawn(async move {
                service.run_service(network).await;
            })
        });
        future::join_all(handlers).await;
    }

    // run service is the main service runtime itself
    async fn run_service(self: &Arc<Self>, service_address: ListenerAddress) {
        let listener = service_address.bind_to_listener().await;
        println!("service is running");
        // began infinite loop
        // accepting incoming connections
        loop {
            let new_io = tokio::select! {
                new_io = listener.accept_stream() => new_io,
                // shutdown signal here to break loop
            };
            match new_io {
                Ok((downstream, socket_address)) => {
                    // get self reference
                    let service = Arc::clone(self);
                    tokio::spawn(async move {
                        // handle here
                        service.handle_connection(downstream, socket_address).await
                    });
                }
                Err(e) => {
                    println!("failed to accept uds connection: {:?}", e);
                }
            };
        }
    }

    // handling incoming request to here
    async fn handle_connection(self: &Arc<Self>, downstream: Stream, _socket_address: Socket) {
        println!("some message!: {}", self.service.say_hi());

        let address = SocketAddress::parse_tcp("127.0.0.1:8000");

        // simulate a given backend peer
        let peer = UpstreamPeer::new(
            "node 1",
            &self.name,
            address,
            None,
        );

        // get upstream connection
        let upstream = match self.stream_session.get_connection_from_pool(&peer).await {
            Ok((upstream, is_reused)) => {
                if is_reused {
                    println!("reusing stream from pool");
                } else {
                    println!("connection does not exist in pool, new stream created");
                }
                upstream
            },
            Err(_) => panic!("error getting stream from pool"),
        };

        // handle io copy & returned the upstream
        let upstream = match self.handle_process(downstream, upstream).await {
            Ok(stream) => stream,
            Err(_) => panic!("error during io copy"),
        };

        // return upstream to pool
        self.stream_session.return_connection_to_pool(upstream, &peer).await;
        println!("upstream connection returned");
    }
}
