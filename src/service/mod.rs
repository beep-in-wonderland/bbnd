use super::*;


use crate::database::Session;
use crate::io;
use crate::read_dir;
use crate::PathBuf;
use crate::thread;
use crate::Not;

use crate::fancy_regex::Regex;
use crate::webhook;
use crate::{Arc,Mutex};
use crate::logging::{info,error,debug,warn};
use crate::{database, errors};
use crate::json;

use chrono;
use fancy_regex::Captures;


pub(super) mod listener;
pub(super) mod updater;

#[cfg(test)]
mod tests;


pub(super) enum ThreadHandle<T> {
    Updater(JoinHandle<T>),
    Listener(JoinHandle<T>),
}

impl ThreadHandle<Result<(),errors::Error>> {
    fn is_finished(&self) -> bool {
        match self {
            ThreadHandle::Updater(h) => h.is_finished(),
            ThreadHandle::Listener(h) => h.is_finished()
        }    
    }
}


/// The Service struct contains the environment paths for the program and the 
/// runtime config (wesad.conf). As well, it contains a wrapper for the sqlite
/// databse, which is futher wrapped by a Mutex and Arc for sharing across
/// threads.
/// 
/// `paths` and `runtime_config` are computed only once on startup. Cloning them
/// provides no worry of stale data.
/// 
/// As such, the running `service` instance will be cloned into both the
/// `listener` and `updater` threads so as to have an easy entrypoint into
/// some maintenance functions, such as `reload_configs(&self)`.
#[derive(Clone)]
pub struct Service {
    paths : io::ProgramPaths,
    session : Arc<Mutex<database::Session>>,
    runtime_config: io::RuntimeConfig,
}

impl Service {

    /// Initialization function of the daemon only requires that the paths be specified for this runtime.
    pub fn new(is_dev_build: bool, log_level: &logging::Level) -> Result<Self, errors::Error> {

        // TODO Rewrite this as a builder eventually with .with_dev_build() as a buildparam
        let paths = io::ProgramPaths::build_paths(is_dev_build);
        
        // Initialize the logging system
        logging::init_logger(log_level, paths.get_log());

        // Initialize persistent local db.
        let session = 
            database::Session::init(&paths.get_persist())
            .expect("An unexpected error occurred initializing the database.");
        
        let config = io::RuntimeConfig::new(&paths);

        Ok (Self { 
                paths,
                session : Arc::new(Mutex::new(session)),
                runtime_config : config,
            })
    }


    pub fn run(&mut self) -> Result<(), errors::Error> {

        // Read configs and store data in db
        if let Err(e) = self.parse_all_webhook_configs() {
            error!("Error occurred reading webhook configs. Consider investigating and restarting the daemon: {e}");
        }

        // Lock the session mutex and drop all orphaned values
        let session = self.session.lock().expect("poisoned");
        database::write::drop_orphaned_values(&session)?;
        drop(session);

        let (updater_handle, 
            listener_handle
        ) = self.get_runtime_threads()?;

        self.thread_handle_loop(&updater_handle, &listener_handle)?;

        Ok (())
    }


    pub fn reload_configs(&self) -> Result<(), errors::Error>
    {
        if let Err(e) = self.parse_all_webhook_configs() {
            error!("Error occurred reading webhook configs. Consider investigating and restarting the daemon: {e}");
        };

        Ok(())
    }

    fn get_runtime_threads(&self) -> Result<(ThreadHandle<Result<(),errors::Error>>, ThreadHandle<Result<(),errors::Error>>), errors::Error>
    {   
        // Spawns a thread to continuously update webhooks with current database values
        let updater_handle = updater::run(
            self.clone())
            .expect("Program does nothing if this fails");

        // Spawns a thread to continuously listen for & execute database updates
        let listener_handle = listener::run(
            self.clone())
            .expect("Program does nothing if this fails");

        Ok((updater_handle, listener_handle))
    }


    fn thread_handle_loop(
        &mut self,
        updater: &ThreadHandle<Result<(), errors::Error>>, 
        listener: &ThreadHandle<Result<(), errors::Error>>) 
    -> Result<(), errors::Error> 
    {
        loop  {
            // Check if either handle has finished executing, which should only happen on a thread-unrecoverable error.
            if updater.is_finished() {
                error!("Updater loop failed unexpectedly. Attempting to restart...");

                self.recover_from_thread_failure()
                    .expect("It is not possible to recover from the current state.");

            } else if listener.is_finished() {
                error!("HTTP listener failed unexpectedly. Attempting to restart...");

                self.recover_from_thread_failure()
                    .expect("It is not possible to recover from the current state.");
            }

            // Put the loop to sleep so we only check for thread failure every second
            debug!("Putting main thread to sleep for 1s...");
            thread::sleep(std::time::Duration::from_secs(1));
        }
    }
    

    fn recover_from_thread_failure(
        &mut self)
    -> Result<(ThreadHandle<Result<(), errors::Error>>, ThreadHandle<Result<(), errors::Error>>), errors::Error>
    {
        // Get a new clean session object
        let session = database::Session::init(&self.paths.get_persist())
            .expect("Database could not be opened. Exiting.");

        // Put it into a new Arc<Mutex<>> and then send it into the daemon object
        self.session = Arc::new(Mutex::new(session));

        self.get_runtime_threads()
    }


    fn parse_all_webhook_configs(&self) -> Result<(), errors::Error> {
        log::debug!("Starting webhook parse");

        let webhooks_dir = &self.paths.get_etc().join(io::ETC_PATH_WEBHOOKS);
        let webhooks = read_dir(webhooks_dir)?;
        let mut webhook_path: PathBuf;

        let regex_conf = Regex::new(io::REGEX_CONF)?;
        let mut captures: Option<Captures<'_, String>> ;
        let mut path: String;
        let mut name: String;

        let mut webhook_config: io::WebhookConfigContents;
        let mut webhook_tags: Vec<String>;


        for webhook in webhooks {
            webhook_path = webhook.unwrap().path();

            let target_string = webhook_path.display().to_string();
            captures = regex_conf.captures(&target_string)?;


            // If the regex found a capture, it means it must have been a .conf file
            if captures.is_none().not() {
                log::debug!("Config file found: {}", target_string);

                // Extract the 'path' & 'filename' groups from the regex capture
                path = captures.as_ref().unwrap().get(1).unwrap().as_str().to_string();
                name = captures.as_ref().unwrap().get(2).unwrap().as_str().to_string();


                match io::read_webhook_config(&path, &name) {
                    Ok(config) => webhook_config = config,
                    Err(e) => {
                        error!("Failed to read webhook config {}/{}.conf. Skipping with error code {e}", &path, &name);
                        continue
                    }
                }

                match io::read_webhook_json_tags(&path, &name) {
                    Ok(tags) => webhook_tags = tags,
                    Err(e) => {
                        error!("Failed to parse webhook tags from {}/{}.json. Skipping with error code {e}", &path, &name);
                        continue
                    }
                }

                // Calculate the period, in seconds, based on `interval` and `period`
                let period= webhook::Period::from_type_value(
                   &webhook_config.interval, webhook_config.period);

                log::debug!("Creating webhook status for {}", name);
                let status = webhook::Status {
                    name : webhook_config.name.clone(),
                    url : webhook_config.url,
                    path,
                    webhook_type : webhook_config.webhook_type,
                    period : period,
                    last: 0,
                    msg_id: webhook_config.msg_id
                };
                
                let session = self.session.lock().expect("poisoned");

                database::write::init_webhook_parent(&session, &status)?;
                database::write::init_webhook_tags(&session, &webhook_config.name, &webhook_tags)?;

            }

        }

        debug!("Finished webhook parse");
        Ok(())
    }


}