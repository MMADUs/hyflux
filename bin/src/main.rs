use std::sync::Arc;
use core::upstream::lb::LoadBalancer;
use core::upstream::selection::algorithm::roundrobin::RoundRobin;

struct Service1 {
    name: String,
    lb: Arc<LoadBalancer<RoundRobin>>,
}

impl Service1 {
    fn new(name: &str, backends: Vec<&str>) -> Self {
        let lb = LoadBalancer::from_iterator(backends).unwrap();
        Service1 {
            name: name.to_string(),
            lb: Arc::new(lb),
        }
    }
}

impl core::service::service::ServiceType for Service1 {
    fn say_hi(&self) -> String {
        println!("my name is: {}", &self.name);
        self.name.clone()
    }
}

fn main() {
    // println!("Hello, world!");
    // println!("2 + 2 is: {}", add(2, 2));
    let backends = vec!["127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002"];
    let mut service1 = core::service::service::Service::new("service-1", Service1::new("jeremy", backends));
    service1.add_tcp("127.0.0.1:8500", None);
    service1.add_tcp("127.0.0.1:8600", None);
    let mut server = core::server::server::Server::new();
    server.add_service(service1);
    server.run_forever();
}
