use chrono::{TimeZone, Utc};
use fake::{Dummy, Fake, Faker};
use rand::Rng;
use serde::Serialize;

use super::{driver::Driver, truck::Truck, EventSource};

pub struct TimeRegistrationEvent {
    truck_pool: Vec<Truck>,
    driver_pool: Vec<Driver>
}

#[derive(Debug, Clone, Serialize)]
pub struct TimeRegistration {
    r#type: String,
    truck_id: String,
    timestamp: String,
    driver_id: String,
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
            Self::EndDay => "end_day".to_string(),
        }
    }
}

pub struct TimeRegistrationBuilder {
    driver: Driver,
    truck: Truck,
    date: String,
    r#type: TimeRegistrationType,
}

impl TimeRegistrationBuilder {
    pub fn new(r#type: TimeRegistrationType) -> Self {
        //default initialization
        let driver: Driver = Faker.fake();
        let truck: Truck = Faker.fake();
        let mut rng = rand::thread_rng();
        let year = rng.gen_range(2022..2025);
        let month = rng.gen_range(1..12);
        let day = rng.gen_range(1..27);
        let hour = match r#type {
            TimeRegistrationType::StartDay => rng.gen_range(5..11),
            TimeRegistrationType::StartBreak => rng.gen_range(12..14),
            TimeRegistrationType::EndBreak => rng.gen_range(15..18),
            TimeRegistrationType::EndDay => rng.gen_range(19..23),
        };
        let dt = Utc
            .with_ymd_and_hms(year, month, day, hour, 0, 0)
            .unwrap()
            .timestamp_micros()
            .to_string();
        Self {
            driver,
            truck,
            date: dt,
            r#type,
        }
    }

    pub fn with_driver(mut self, driver: Driver) -> Self {
        self.driver = driver;
        self
    }

    pub fn with_truck(mut self, truck: Truck) -> Self {
        self.truck = truck;
        self
    }

    pub fn build(&self) -> TimeRegistration {
        TimeRegistration {
            r#type: self.r#type.to_string(),
            driver_id: self.driver.clone().driver_id,
            truck_id: self.truck.clone().truck_id,
            timestamp: self.date.clone(),
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
            TimeRegistrationType::StartDay => rng.gen_range(5..11),
            TimeRegistrationType::StartBreak => rng.gen_range(12..14),
            TimeRegistrationType::EndBreak => rng.gen_range(15..18),
            TimeRegistrationType::EndDay => rng.gen_range(19..23),
        };
        let dt = Utc
            .with_ymd_and_hms(year, month, day, hour, 0, 0)
            .unwrap()
            .timestamp_micros()
            .to_string();

        Self {
            r#type: r#type.to_string(),
            driver_id: driver.driver_id,
            timestamp: dt,
            truck_id: truck.truck_id,
        }
    }
}

impl TimeRegistrationEvent {
    pub fn new(driver_pool: Vec<Driver>, truck_pool: Vec<Truck>) -> Self {
        Self {
            driver_pool,
            truck_pool
        }
    }
}

impl EventSource for TimeRegistrationEvent {
    fn generate(&self) -> Vec<String> {
        let time_registration_types = [TimeRegistrationType::StartDay, TimeRegistrationType::StartBreak, TimeRegistrationType::EndBreak, TimeRegistrationType::EndDay];

        let mut rng = rand::thread_rng();
        //first need to get a random driver
        let random_driver = self.driver_pool.get(rng.gen_range(0..self.driver_pool.len()));
        //then the truck
        let random_truck = self.truck_pool.get(rng.gen_range(0..self.truck_pool.len()));
        let mut data = Vec::new();
        for time_registration_type in time_registration_types {
            let event = TimeRegistrationBuilder::new(time_registration_type)
                .with_driver(random_driver.unwrap().clone())
                .with_truck(random_truck.unwrap().clone())
                .build();
            
            data.push(serde_json::to_string(&event).unwrap());
        }
        data
    }
}
