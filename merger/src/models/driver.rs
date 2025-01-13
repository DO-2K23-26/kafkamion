use serde::Deserialize;

#[derive(Deserialize)]
pub struct Driver {
    pub type_: String,
    pub driver_id: String,
    pub first_name: String,
    pub last_name: String,
    pub email: String,
    pub phone: String,
}