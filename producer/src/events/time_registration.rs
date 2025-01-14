use chrono::{TimeZone, Utc};
use fake::{Dummy, Fake, Faker};
use rand::Rng;
use serde::Serialize;

use super::{driver::Driver, truck::Truck, EventSource};

pub struct TimeRegistrationEvent {
    //truck_data: Vec<Truck>,
    //driver_data: Vec<Driver>
}


#[derive(Debug, Clone, Serialize)]
pub struct TimeRegistration {
    r#type: String,
    truck_id: String,
    timestamp: String,
    driver_id: String
}

enum TimeRegistrationType {
    StartDay,
    StartBreak,
    EndBreak,
    EndDay,
}

impl TimeRegistrationType {
    pub fn to_string(&self) -> String {
        match self {
            Self::StartDay => "start_day".to_string(),
            Self::StartBreak => "start_break".to_string(),
            Self::EndBreak => "end_break".to_string(),
            Self::EndDay => "end_day".to_string()
        }
    }
}

impl TimeRegistration {
    pub fn new(r#type: TimeRegistrationType) -> Self {
        let driver: Driver = Faker.fake();
        let truck: Truck = Faker.fake();
        let mut rng = rand::thread_rng();
        let year = rng.gen_range(2022..2025);
        let month = rng.gen_range(1..12);
        let day = rng.gen_range(1..27);
        let hour = match r#type {
            TimeRegistrationType::StartDay =>  rng.gen_range(5..11),
            TimeRegistrationType::StartBreak =>  rng.gen_range(12..14),
            TimeRegistrationType::EndBreak =>  rng.gen_range(15..18),
            TimeRegistrationType::EndDay =>  rng.gen_range(19..23),
        };
        let dt = Utc.with_ymd_and_hms(year, month, day, hour, 0, 0).unwrap().timestamp_micros().to_string();

        Self {
            r#type: r#type.to_string(),
            driver_id: driver.driver_id,
            timestamp: dt,
            truck_id: truck.truck_id,
        }
    }
}

impl TimeRegistrationEvent {
    pub fn new() -> Self {
        Self{
        }
    }
}

impl EventSource for TimeRegistrationEvent {
    fn generate(&self) -> Vec<String> {
        let start_day: TimeRegistration = TimeRegistration::new(TimeRegistrationType::StartDay);
        let start_break: TimeRegistration = TimeRegistration::new(TimeRegistrationType::StartBreak);
        let end_break: TimeRegistration = TimeRegistration::new(TimeRegistrationType::EndBreak);
        let end_day: TimeRegistration = TimeRegistration::new(TimeRegistrationType::EndDay);
        let mut data = Vec::new();
        data.push(serde_json::to_string(&start_day).unwrap());
        data.push(serde_json::to_string(&start_break).unwrap());
        data.push(serde_json::to_string(&end_break).unwrap());
        data.push(serde_json::to_string(&end_day).unwrap());

        data
    }
}
