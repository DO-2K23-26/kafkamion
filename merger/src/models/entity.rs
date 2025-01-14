use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub enum Entity {
    Driver { type_: String, driver_id: String, first_name: String, last_name: String, email: String, phone: String },
    Truck { type_: String, truck_id: String, immatriculation: String },
}

impl Entity {
    pub fn newDriver(type_: String, driver_id: String, first_name: String, last_name: String, email: String, phone: String) -> Entity {
        Entity::Driver { type_: type_, driver_id: driver_id, first_name: first_name, last_name: last_name, email: email, phone: phone }
    }

    pub fn newTruck(type_: String, truck_id: String, immatriculation: String) -> Entity {
        Entity::Truck { type_: type_, truck_id: truck_id, immatriculation: immatriculation }
    }
}