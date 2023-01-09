use tonic::async_trait;

use super::{Request, Response, Result, TestError};
use std::collections::HashMap;
use std::sync::Arc;

#[async_trait]
pub trait Service: Sync + Send + 'static {
    async fn handle(&self, data: &[u8]) -> Vec<u8>;
}

struct ServerBuilder {
    inner: ServerInner,
}

impl ServerBuilder {
    pub fn new(name: String) -> Self {
        ServerBuilder {
            inner: ServerInner {
                name,
                services: HashMap::new(),
            },
        }
    }

    pub fn add_service(&mut self, service_name: String, service: Box<dyn Service>) {
        self.inner.services.insert(service_name, service);
    }

    pub fn build(self) -> Server {
        Server {
            inner: Arc::new(self.inner),
        }
    }
}

struct ServerInner {
    name: String,
    services: HashMap<String, Box<dyn Service>>,
}

#[derive(Clone)]
pub struct Server {
    inner: Arc<ServerInner>,
}

impl Server {
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub async fn handle(&self, request: &Request) -> Result<Response> {
        let service = self
            .inner
            .services
            .get(&request.service_name)
            .ok_or(TestError::ServiceLoss)?;

        Ok(Response {
            data: service.handle(&request.data).await,
        })
    }
}
