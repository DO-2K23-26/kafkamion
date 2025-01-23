use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct Position {
    pub type_: String,
    pub truck_id: String,
    pub latitude: f64,
    pub longitude: f64,
    pub timestamp: String,
}

impl Position {
    pub fn new(truck_id: String, latitude: f64, longitude: f64, timestamp: String) -> Position {
        Position {
            type_: "position".to_string(),
            truck_id: truck_id,
            latitude: latitude,
            longitude: longitude,
            timestamp: timestamp,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FullPosition {
    pub start: Option<Position>,
    pub rest: Option<Position>,
    pub end: Option<Position>,
}

impl FullPosition {
    pub fn new() -> FullPosition {
        FullPosition {
            start: None,
            rest: None,
            end: None,
        }
    }
}