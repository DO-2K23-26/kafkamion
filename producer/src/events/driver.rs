use fake::{Dummy, Fake, Faker};
use fake::faker::name::en::{Name, LastName, FirstName};
use fake::faker::internet::en::FreeEmail;

use serde::Serialize;
use uuid::Uuid;

use super::EventSource;

pub struct DriverEvent {
}

#[derive(Debug, Clone, Dummy, Serialize)]
pub struct Driver {
    #[dummy(expr = "\"driver\".to_string()")]
    pub r#type: String,
    #[dummy(expr = "Uuid::new_v4().to_string()")]
    pub driver_id: String,
    #[dummy(faker = "FirstName()")]
    pub first_name: String,
    #[dummy(faker = "LastName()")]
    pub last_name: String,
    #[dummy(faker = "FreeEmail()")]
    pub email: String,
    #[dummy(faker = "Name()")]
    pub phone: String
}

impl DriverEvent {
    pub fn new() -> Self {
        Self{
        }
    }
}


impl EventSource<Driver> for DriverEvent {
    fn generate(&self) -> (Vec<String>, Vec<Driver>) {
        let data: Driver = Faker.fake();
        (vec![serde_json::to_string(&data).unwrap()], vec![data])
    }
}
