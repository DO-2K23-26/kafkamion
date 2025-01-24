use fake::{
    faker::address::fr_fr::{Latitude, Longitude},
    Fake,
};
use rand::Rng;
use serde::Serialize;

use super::{time_registration::{TimeRegistration, TimeRegistrationType}, EventSource};

#[derive(Debug, Clone, Serialize)]
pub struct Position {
    truck_id: String,
    latitude: String,
    longitude: String,
    timestamp: String,
}

impl Position {
    pub fn new(truck_id: String, timestamp: String) -> Self {
        let fake_latitude = Latitude().fake();
        let fake_longitude = Longitude().fake();

        Self {
            truck_id,
            timestamp,
            latitude: fake_latitude,
            longitude: fake_longitude,
        }
    }
}

pub struct PositionEvent {
    time_registration_pool: Vec<Vec<TimeRegistration>>, //we want complete days of time
                                                        //registration. Otherwise it would be a little difficult for the merger to work
}

impl PositionEvent {
    pub fn new(time_registration_pool: Vec<Vec<TimeRegistration>>) -> Self {
        Self {
            time_registration_pool,
        }
    }
}

impl EventSource<Position> for PositionEvent {
    fn generate(&self) -> (Vec<String>, Vec<Position>) {
        let mut rng = rand::thread_rng();
        let random_time_registrations = self
            .time_registration_pool
            .get(rng.gen_range(0..self.time_registration_pool.len()))
            .unwrap();

        let mut data = Vec::new();
        let mut positions = Vec::new();

        let random_time_registrations = random_time_registrations.iter().filter(|e| {
            e.r#type != TimeRegistrationType::EndBreak.to_string()
        });

        for random_time_registration in random_time_registrations {
            let time_registration = random_time_registration.clone();
            let position = Position::new(time_registration.truck_id, time_registration.timestamp);
            data.push(serde_json::to_string(&position).unwrap());
            positions.push(position);
        }

        (data, positions)
    }
}
