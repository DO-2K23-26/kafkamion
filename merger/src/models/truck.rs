use serde::Deserialize;

#[derive(Deserialize)]
pub struct Truck {
    pub type_: String,
    pub truck_id: String,
    pub immatriculation: String,
}