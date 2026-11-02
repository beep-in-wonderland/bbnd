use super::*;

use crate::axum::{
    routing::{post, get},
    extract::{State, Json},
    http::StatusCode,
};
use crate::serde::Deserialize;


#[derive(Deserialize)]
struct StatusUpdate {
    webhook: String,
    update_type: String,
    content: Vec<TagValue>,
}

impl Display for StatusUpdate {

    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {

        let mut values_str: String = String::from("\n");

        for pl in &self.content {
            values_str = values_str + format!("{} : \"{}\"", pl.tag, pl.value).as_str();
        }

        write!(f, "StatusUpdate:
        webhook: {}
        update_type: {}
        values:{}",
        self.webhook,
        self.update_type,
        values_str
    )
    }
}


#[derive(Deserialize)]
pub struct TagValue {
    pub tag: String,
    pub value: String
}

impl TagValue {
    pub fn new<T>(tag: T, value: T) -> Self 
    where T: Display + Clone,
    {
        TagValue { tag: tag.to_string().clone(), value: value.to_string().clone() }
    }
}


pub(super) fn run(service: crate::service::Service) 
-> Result<ThreadHandle<Result<(), errors::Error>>, errors::Error> {
    
    Ok(
        ThreadHandle::Listener(
            thread::spawn(move||{
                listen_http(service)
            })
        )
    )
}



fn listen_http(service: crate::service::Service) -> Result<(), errors::Error> {

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to build listener runtime");

    loop {

        // Continuously try and listen on the provided address.
        if let Err(e) = rt.block_on(build_listener(&service)) {
            error!("HTTP listener failed: {e}");
        }

        // Only try and restart once per second.
        thread::sleep(std::time::Duration::from_secs(1));

        // TODO implement a dynamic error counter to decide when "too many recent errors"
        // means that the program should abort.
    }
}


async fn build_listener(service: &Service) -> Result<(), errors::Error> {

    let guard = &service.session;
    let target_ip = format!("{}:{}",
        &service.runtime_config.listener_addr,
        &service.runtime_config.listener_port
    );

    let app: axum::Router = axum::Router::new()

        // POST paths
        //              /status - Tag updates
        .route("/status", post(post_status))
        .with_state(guard.clone())

        //              /reload - Reload `webhooks.d` configs
        .route("/reload", post(post_reload))
        .with_state(service.clone())

        // GET paths
        //              /health - Simple health check
        .route("/health", get(get_health));

    let listener = tokio::net::TcpListener::bind(target_ip).await?;
    axum::serve(listener, app).await?;

    Ok(())
}


// --------------------------------- Endpoint functions

async fn post_status(
    State(guard): State<Arc<Mutex<database::Session>>>, 
    Json(payload): Json<StatusUpdate>
) -> StatusCode 
{

    info!("POST received to HTTP listener for webhook: {}", &payload.webhook);

    if payload.update_type.to_lowercase().trim().ne("status") {
        error!("Incorrect webhook type for endpoint /status");
        return StatusCode::NOT_ACCEPTABLE;
    }

    let session = guard.lock().expect("Mutex poisoned in listener status_handler. Exiting thread.");

    for pl in &payload.content {
        if let Err(e) = database::write::update_webhook_tag(
            &session, 
            &payload.webhook, 
            &pl.tag, 
            &pl.value,
        ) {
            warn!("Tag {} : \"{}\" failed to update, error {e}", pl.tag, pl.value)
        } else {
            debug!("Tag value updated successfully: {}: \"{}\"", pl.tag, pl.value);
        }
    };

    StatusCode::OK

}


async fn post_reload(
    State(service): State<service::Service>
    ) -> StatusCode
{   
    info!("Config reload requested. Processing...");
    
    if let Err(e) = service.reload_configs() {
        error!("Error reloading configs: {e}");
        return StatusCode::INTERNAL_SERVER_ERROR
    }

    StatusCode::OK
}


async fn get_health() -> StatusCode 
{
    info!("Health check received");
    StatusCode::OK
}
