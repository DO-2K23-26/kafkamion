use std::process::ExitCode;

pub mod driver;
pub mod time_registration;
pub mod truck;
pub mod position;

pub trait EventSource {
    fn generate(&self) -> String;
}
