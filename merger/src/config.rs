use hocon::HoconLoader;
use lazy_static::lazy_static;
use serde::Deserialize;

// Derive the Deserialize and Debug traits for the Config struct
#[derive(Deserialize, Debug)]
pub struct Config {
    pub kafka_broker: String,
    pub topic: String,
    pub group_id: String,
}

lazy_static! {
    // Define a static CONFIG variable initialized with the result of the get_config function
    pub static ref CONFIG: Config = get_config();
}

fn get_config() -> Config {
    // Load and resolve the HOCON configuration file
    let config: Config = HoconLoader::new()
        .load_file("src/resource/application.config")
        .expect("Unable to load config file")
        .resolve()
        .expect("Config deserialize error");
    config // Return the resolved configuration
}