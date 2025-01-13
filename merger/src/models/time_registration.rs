use serde::Deserialize;

#[derive(Deserialize)]
pub struct TimeRegistration {
    pub type_: String,
    pub timestamp: String,
    pub driver_id: String,
    pub truck_id: String,
}