use serde_derive::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Position {
    pub truck_id: String,
    pub latitude: f64,
    pub longitude: f64,
    pub timestamp: String,
}

impl Position {
    pub fn new(truck_id: String, latitude: f64, longitude: f64, timestamp: String) -> Position {
        Position {
            truck_id: truck_id,
            latitude: latitude,
            longitude: longitude,
            timestamp: timestamp,
        }
    }
}