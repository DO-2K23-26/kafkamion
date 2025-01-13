use serde::Deserialize;

#[derive(Deserialize)]
pub struct Position {
    pub truck_id: String,
    pub latitude: f64,
    pub longitude: f64,
    pub timestamp: String,
}