use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Driver {
    pub type_: String,
    pub driver_id: String,
    pub first_name: String,
    pub last_name: String,
    pub email: String,
    pub phone: String,
}

#[derive(Debug, Deserialize)]
pub struct Truck {
    pub type_: String,
    pub truck_id: String,
    pub immatriculation: String,
}

#[derive(Deserialize, Debug)]
pub enum Entity {
    Driver(Driver),
    Truck(Truck),
}

impl Entity {
    pub fn new_driver(type_: String, driver_id: String, first_name: String, last_name: String, email: String, phone: String) -> Entity {
        Entity::Driver(Driver {
            type_: type_,
            driver_id: driver_id,
            first_name: first_name,
            last_name: last_name,
            email: email,
            phone: phone,
        })
    }

    pub fn new_truck(type_: String, truck_id: String, immatriculation: String) -> Entity {
        Entity::Truck(Truck {
            type_: type_,
            truck_id: truck_id,
            immatriculation: immatriculation,
        })
    }
}