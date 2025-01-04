use dotenv::dotenv;
use std::sync::Arc;
use tokio::runtime::{Builder, Handle};
use tokio::signal::unix;

use crate::service::service::{Service, ServiceType};

// just a tokio runtime builder
// used for multithreaded and work stealing configs
pub struct Runtime {
    runtime: tokio::runtime::Runtime,
}

impl Runtime {
    // new runtime builder
    pub fn new(thread_name: &str, alloc_threads: usize) -> Self {
        let built_runtime = Builder::new_multi_thread()
            .enable_all()
            .worker_threads(alloc_threads)
            .thread_name(thread_name)
            .build()
            .unwrap();
        Runtime {
            runtime: built_runtime,
        }
    }
    // handle thread work
    pub fn handle_work(&self) -> &Handle {
        self.runtime.handle()
    }
}

// shutdown types
enum ShutdownType {
    Graceful,
    Fast,
}

// the main server instance
// the server can held many services
pub struct Server<A> {
    services: Vec<Service<A>>,
}

impl<A: ServiceType + Send + Sync + 'static> Server<A> {
    // new server
    pub fn new() -> Self {
        Server {
            services: Vec::new(),
        }
    }

    // add each service
    pub fn add_service(&mut self, service: Service<A>) {
        self.services.push(service);
    }

    // this is the method that is used to run the server
    // forever
    pub fn run_forever(&mut self) {
        println!("running server...");
        // load env
        dotenv().ok();
        // setup tracing
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_file(true)
            .with_line_number(true)
            .with_thread_ids(true)
            .with_target(true)
            .pretty()
            .init();
        // running runtimes
        let mut runtimes: Vec<Runtime> = Vec::new();
        while let Some(service) = self.services.pop() {
            // example threads, make it dynamic later
            let alloc_threads = 10;
            let runtime = Self::run_service(service, alloc_threads);
            runtimes.push(runtime);
        }
        // the main server runtime
        let main_runtime = Runtime::new("main-runtime", 1);
        let shutdown_type = main_runtime.handle_work().block_on(
            // this block forever until the entire program exit
            Self::run_server(),
        );
        match shutdown_type {
            ShutdownType::Graceful => println!("shutdown gracefully"),
            ShutdownType::Fast => println!("shutdown fast"),
        }
    }

    // used for running every service
    // that was added to the server
    fn run_service(service: Service<A>, alloc_threads: usize) -> Runtime {
        // run each listener service on top of the runtime
        let runtime = Runtime::new("runtime", alloc_threads);
        let address_stack = service.address_stack.clone();
        // wrap the service into Arc
        // this service will be handled across threads.
        let service = Arc::new(service);
        runtime.handle_work().spawn(async move {
            // run the async service here
            service.start_service(address_stack).await;
        });
        runtime
    }

    // this is the where main process will be blocked
    // the function wait for an exit process signal
    async fn run_server() -> ShutdownType {
        // waiting for exit signal
        let mut graceful_upgrade_signal = unix::signal(unix::SignalKind::quit()).unwrap();
        let mut graceful_terminate_signal = unix::signal(unix::SignalKind::terminate()).unwrap();
        let mut fast_shutdown_signal = unix::signal(unix::SignalKind::interrupt()).unwrap();
        tokio::select! {
            _ = graceful_upgrade_signal.recv() => {
                println!("graceful upgrade signalled");
                ShutdownType::Graceful
            },
            _ = graceful_terminate_signal.recv() => {
                println!("graceful termintation signalled");
                ShutdownType::Graceful
            },
            _ = fast_shutdown_signal.recv() => {
                println!("fast shutdown signalled");
                ShutdownType::Fast
            }
        }
    }
}
