use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Report {
    pub driver_id: String,
    pub first_name: String,
    pub last_name: String,
    pub email: String,
    pub phone: String,
    pub truck_id: String,
    pub immatriculation: String,
    pub start_time: String,
    pub end_time: String,
    pub rest_time: String,
    pub latitude_start: f64,
    pub longitude_start: f64,
    pub timestamp_start: String,
    pub latitude_end: f64,
    pub longitude_end: f64,
    pub timestamp_end: String,
    pub latitude_rest: f64,
    pub longitude_rest: f64,
    pub timestamp_rest: String,
}