struct Service1 {
    name: String,
}

impl Service1 {
    fn new(name: &str) -> Self {
        Service1 {
            name: name.to_string(),
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
    let mut service1 = core::service::service::Service::new("service-1", Service1::new("athaya"));
    service1.add_tcp("127.0.0.1:8500", None);
    service1.add_tcp("127.0.0.1:8600", None);
    let mut server = core::server::server::Server::new();
    server.add_service(service1);
    server.run_forever();
}
