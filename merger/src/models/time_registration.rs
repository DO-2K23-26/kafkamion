use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct TimeRegistration {
    pub type_: String,
    pub timestamp: String,
    pub driver_id: String,
    pub truck_id: String,
}

impl TimeRegistration {
    pub fn new(type_: String, timestamp: String, driver_id: String, truck_id: String) -> TimeRegistration {
        TimeRegistration {
            type_: type_,
            timestamp: timestamp,
            driver_id: driver_id,
            truck_id: truck_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FullTimeRegistration {
    pub start: Option<TimeRegistration>,
    pub rest: Option<TimeRegistration>,
    pub end: Option<TimeRegistration>,
}

impl FullTimeRegistration {
    pub fn new() -> FullTimeRegistration {
        FullTimeRegistration {
            start: None,
            rest: None,
            end: None,
        }
    }
}
