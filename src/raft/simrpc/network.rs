use super::{server::Server, Request, TestError};
use super::{Response, Result};
use rand::{thread_rng, Rng};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;

struct ExecutionInfo {
    server: Server,
    request_delay_ms: u32,
    response_delay_ms: u32,
    lost_before_handle: bool,
    lost_after_handle: bool,
}

struct Endpoints {
    clients: HashMap<String, mpsc::Receiver<Request>>,
    servers: HashMap<String, Server>,
    connections: HashMap<String, String>,
}

#[derive(Clone, Copy)]
struct NetworkStatus {
    // delay milliseconds across network
    delay_upper_bound_ms: u32,

    // delay milliseconds across network
    delay_lower_bound_ms: u32,

    // Permillage of packet loss across network, [0,1000]
    packet_loss_permillage: u32,
}

impl NetworkStatus {
    async fn delay(&self) {
        let ms = thread_rng().gen_range(self.delay_lower_bound_ms..=self.delay_upper_bound_ms);
        sleep(Duration::from_millis(ms as u64)).await;
    }

    fn is_loss(&self) -> bool {
        thread_rng().gen_ratio(self.packet_loss_permillage, 1000)
    }
}

pub struct NetworkInner {
    status: Mutex<NetworkStatus>,
    // Endpoints record, server, client, connection
    endpoints: Mutex<Endpoints>,
}

#[derive(Clone)]
pub struct Network {
    inner: Arc<NetworkInner>,
}

impl Network {
    pub fn start(&self, mut channel: mpsc::Receiver<Request>) {
        let network = self.clone();

        tokio::spawn(async move {
            // Accept new rpc request
            while let Some(request) = channel.recv().await {
                // Dispatch to handle rpc
                let mut new_network = network.clone();
                tokio::spawn(async move {
                    new_network.handle_rpc(request).await;
                });
            }
        });
    }

    // Simulating network delay, request lost, reply lost like real world network
    pub async fn handle_rpc(&mut self, mut request: Request) {
        // Copy rpc execution info
        // Get target server
        let server = {
            let endpoints = self.get_endpoints();
            if let Some(server_name) = endpoints.connections.get(&request.client_name) {
                endpoints.servers.get(server_name).cloned()
            } else {
                None
            }
        };
        // Copy network status this this moment
        let status = *self.inner.status.lock().unwrap();

        // Simulation the rpc procedure
        let response = rpc_simulation(&request, status, server).await;

        // Send the response to the request client
        request.send_response(response);
    }

    pub fn set_delay_ms(&mut self, upper_bound_ms: u32, lower_bound_ms: u32) {
        if upper_bound_ms < lower_bound_ms {
            return;
        }
        let mut status = self.inner.status.lock().unwrap();
        status.delay_upper_bound_ms = upper_bound_ms;
        status.delay_lower_bound_ms = lower_bound_ms;
    }

    pub fn set_packet_loss_permillage(&mut self, loss: u32) {
        if loss > 1000 {
            return;
        }
        let mut status = self.inner.status.lock().unwrap();
        status.packet_loss_permillage = loss;
    }

    pub fn add_server(&mut self, server: Server) {
        self.get_endpoints()
            .servers
            .insert(server.name().to_string(), server);
    }

    pub fn add_client(&mut self, client_name: String, receiver: mpsc::Receiver<Request>) {
        self.get_endpoints().clients.insert(client_name, receiver);
    }

    pub fn connect(&mut self, client_name: &str, server_name: &str) {
        self.get_endpoints()
            .connections
            .insert(client_name.to_string(), server_name.to_string());
    }

    pub fn disconnect_by_client(&mut self, client_name: &str) {
        self.get_endpoints().connections.remove(client_name);
    }

    #[inline]
    fn get_endpoints(&mut self) -> MutexGuard<Endpoints> {
        self.inner.endpoints.lock().unwrap()
    }
}

// Simulating a rpc procedure with request content, network, server
async fn rpc_simulation(
    request: &Request,
    status: NetworkStatus,
    server: Option<Server>,
) -> Result<Response> {
    // Simulating a request sent delay over network
    status.delay().await;

    // Simulating connection lost lost
    if server.is_none() {
        Err(TestError::ConnectionLoss)?;
    }

    // Simulating a request loss
    if status.is_loss() {
        Err(TestError::PacketLoss)?;
    }

    let response = server.unwrap().handle(request).await;

    // Simulating a response loss
    if status.is_loss() {
        Err(TestError::PacketLoss)?;
    }

    response
}
