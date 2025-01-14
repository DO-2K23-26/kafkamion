#[derive(Deserialize)]
enum Entity {
    Driver { type_: String, driver_id: String, first_name: String, last_name: String, email: String, phone: String },
    Truck { type_: String, truck_id: String, immatriculation: String },
}