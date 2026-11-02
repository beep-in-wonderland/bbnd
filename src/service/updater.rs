use super::*;
use crate::errors::Error::HttpErr;
use crate::thread;
use crate::HashMap;
use crate::reqwest;

const URI_FLAGS_COMPONENT: &str = "with_components=true";
const URI_FLAGS_WAIT: &str = "wait=true";
const MIN_WAIT_TIME: i64 = 1000; // In milliseconds

/// Spawns a thread which continuously updates status type webhooks with session db values
pub fn run(service: service::Service) -> Result<service::ThreadHandle<Result<(), errors::Error>>, errors::Error> {
    
    Ok(
        service::ThreadHandle::Updater(
            thread::spawn(move || {
                update_loop(service)
            })
        )
    )
}


/// Main loop for the updater thread. This function is responsible for periodically
/// pulling current tags from the database and POSTing to all configured
/// webhooks.
/// Each iteration, the loop will:
///     1. Fetch and iterate over all `status` type webhooks
///     2. Send due webhook updates, and update the `last` field for each status.
///     3. Keep track of the shortest time until the *next* scheduled webhook execution.
///     4. Once all webhooks are iterated, sleep until the next scheduled webhook.
fn update_loop(service: service::Service) -> Result<(), errors::Error> {

    let mut sleep_target_timestamp;
    let mut next_millis: i64;
    let mut now_millis: i64;
    let mut sleep_duration_millis: u64 = 0;
    
    // Create a tokio async runtime for sending multiple HTTP requests
    let rt = tokio::runtime::Runtime::new().unwrap();

    loop {
        debug!("Starting updater loop iteration");

        let guard = &service.session;
        let session = guard.lock().expect("Mutex poisoned in updater update_loop. Exiting thread.");
        let webhooks = database::read::webhook_statuses(&session)
            .expect("Fatal: The database cannot be read.");

        now_millis = chrono::Utc::now().timestamp_millis();
        sleep_target_timestamp = 1<<60; // A big number to start with

        // Debug line for timestamping, remove when finished
        // let _ = write::update_webhook_tag(
        //     &session, 
        //     &"wesad-status".to_string(), 
        //     &"timestamp".to_string(), 
        //     &now_millis.to_string());
    
        // Release the mutex guard 
        drop(session);


        // TODO
        // The current below loop does not properly implement async, and each call to 
        // the process_webhook_update function will block on the current iteration of 
        // the for-loop.
        // 
        // Need to rewrite the process_webhook_update as an async function, gather all
        // the futures in a temp vector, and only after sending all the http requests
        // I need to .await and process all the futures in a separate for loop.
        // 
        // In order to do this, I probably also need to move the for loops into their 
        // own async function so that I can use .await
        
        for webhook in webhooks {
            
            match process_webhook_update(&rt, &now_millis, &guard, &webhook) {
                Ok(t) => next_millis = t,
                Err(e) => {
                    error!("Webhook update for {} failed with error: {}", webhook.name, e);
                    next_millis = now_millis + MIN_WAIT_TIME;
                }
            };
            // Check if the next execution of the webhook is the soonest
            if next_millis < sleep_target_timestamp {
                sleep_target_timestamp = next_millis;
                sleep_duration_millis = (&sleep_target_timestamp - now_millis).clamp(MIN_WAIT_TIME, i64::MAX/2-1) as u64;
                debug!("New shortest sleep target for update loop: {}", &sleep_duration_millis / 1000)
            }
        }

        // With the shortest next sleep target found, put the thread to sleep for that long
        debug!("Putting updater thread to sleep for {} seconds", &sleep_duration_millis / 1000);
        thread::sleep(std::time::Duration::from_millis(sleep_duration_millis));

    }
    

}


fn process_webhook_update(
    rt: &tokio::runtime::Runtime, 
    now_millis: &i64,
    guard: &Arc<Mutex<Session>>, 
    webhook: &webhook::Status) 
-> Result<i64, errors::Error>
{
    let next_millis: i64;
    if webhook.should_update(&now_millis) {
        let msg_id = rt.block_on(
            http_send_webhook_update(&guard, &webhook))
            // If the update fails, we should log it and skip this iteration.
            .unwrap_or_else( |e| {
                error!("Webhook update for {} failed with error: {}", webhook.name, e);
                None
            });

        let session = guard.lock().expect("Mutex poisoned in updater update_loop. Exiting thread.");
        if msg_id.is_some() {
            // If a new msg_id was returned, update it in the database
            database::write::update_webhook_msg_id(&session, &webhook.name, &msg_id.unwrap()).unwrap();
        }

        // Update the last execution of the webhook to the current timestamp in milliseconds
        database::write::update_webhook_last(&session, &webhook.name, &now_millis.to_string()).unwrap();

        // Since we just executed, webhook.last is now stale. Use now_millis instead for finding next execution time
        next_millis = webhook.period + *now_millis;

    } else {
        // If no update occurred, use the last execution from the webhook status
        next_millis = webhook.period + webhook.last;
    }

    Ok(next_millis)

}



/// Updates the webhook in `status` based on the `{status.name}.json` template, using current tag values
/// from the database. This function will also check for existing message_id from the database, and verify it
/// with a GET to the webhook before sending the PATCH. If the GET returns unsuccessfully, it will instead send
/// a new webhook message via POST. 
async fn http_send_webhook_update(guard: &Arc<Mutex<Session>>, status: &webhook::Status) -> Result<Option<String>, errors::Error> {

    // Open the .json webhook template as a mutable string
    let mut content = io::open_as_string(&status.path, &status.name, ".json")?;
    
    // Update the contents of the string in-place using current database values.
    update_content_string(&guard, &mut content, &status.name)?;

    if status.msg_id.is_none() {
        // No existing message id, so we should send a new message rather than patch an existing one
        let msg_id = http_post_new_webhook(status, &content).await?;
        return Ok(Some(msg_id));
    } else {

        // There is a message id in the table. We should check it's valid before proceeding.
        if http_check_webhook_message_exists(status).await? {

            // The message id is valid, proceed with a regular patch
            http_patch_existing_webhook(status, &content).await?;

        } else {

            // Invalid message id, so we should send a new message
            let msg_id = http_post_new_webhook(status, &content).await?;
            return Ok(Some(msg_id));
        }

    };
    
    Ok(None)
}


/// Performs in-place update of the provided `content` String using current tag values from the database.
/// The webhook `name` is used as a key to retrieve all tags and their values.
fn update_content_string(guard: &Arc<Mutex<Session>>, content: &mut String, name: &String) -> Result<(), errors::Error> {
    
    let session = guard.lock().expect("Mutex poisoned during updater update_content_string. Exiting thread.");
    let tag_map = database::read::all_tags_by_webhook_name(&session, name)?;
    // Release the mutex lock
    drop(session);

    do_tag_replacements(tag_map, content)?;

    Ok(())
}

/// Performs in-place replacement of values in `content` by searching for instances of `"$$key$$"` and 
/// replacing it with `"value"` for all `"key" : "value"` pairs in `map`.
pub(super) fn do_tag_replacements(map: HashMap<String, String>, content: &mut String) -> Result<(), errors::Error> {

    let mut temp: String = content.clone();

    for (k, v) in map.iter() {
        temp = temp.replace(
            &format!("$${k}$$"),
            v
        );
    }

    content.clone_from(&temp);

    Ok(())
}


async fn http_post_new_webhook(status: &webhook::Status, content: &String) -> Result<String, errors::Error> {

    let uri = format!("{}?{}&{}", status.url, URI_FLAGS_COMPONENT, URI_FLAGS_WAIT);

    let client = reqwest::Client::new();

    let request = client.post(uri)
        .header("Content-Type", "application/json")
        .body(content.clone())
        .send()
        .await?;

    info!("POST sent for {}", &status.name);
    
    match request.status() {
        reqwest::StatusCode::OK => {
            let message = json::parse(&request.text().await?.to_string())?;
            Ok(message["id"].to_string())
        },
        _ => Err(HttpErr(format!("PATCH failed with code: {}",
            request.status().as_u16()
        )))
    }
    
}


async fn http_patch_existing_webhook(status: &webhook::Status, content: &String) -> Result<(), errors::Error> {

    let msg_id = status.msg_id.as_ref()
                            .expect("msg_id should have validated in calling function http_send_webhook_update.");

    let uri = format!("{}/messages/{}?{}&{}", status.url, msg_id, URI_FLAGS_COMPONENT, URI_FLAGS_WAIT);

    let client = reqwest::Client::new();

    let request = client.patch(uri)
        .header("Content-Type", "application/json")
        .body(content.clone())
        .send()
        .await?;

    info!("PATCH sent for {}", &status.name);

    match request.status() {
        reqwest::StatusCode::OK => Ok(()),
        _ => Err(HttpErr(format!("PATCH failed with code: {} - {}",
            request.status().as_u16(),
            request.text().await.unwrap()
        )))
    }

}


async fn http_check_webhook_message_exists(status: &webhook::Status) -> Result<bool, errors::Error> {

    let msg_id = status.msg_id.as_ref()
                            .expect("msg_id should have validated in calling function http_send_webhook_update.");

    let uri = format!("{}/messages/{}?{}&{}", status.url, msg_id, URI_FLAGS_COMPONENT, URI_FLAGS_WAIT);

    let client = reqwest::Client::new();

    let request = client.get(uri)
        .header("Content-Type", "application/json")
        .send()
        .await?;

    debug!("GET sent for {}", &status.name);
    
    match request.status() {
        reqwest::StatusCode::OK => {
            debug!("Valid message id for {}", status.name);
            Ok(true)
        },
        _ => {
            debug!("Invalid message id for {}. Sending new message.", status.name);
            Ok(false)
        },
    }
    
}