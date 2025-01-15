use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
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

impl Report {
    pub fn new(
        driver_id: String,
        first_name: String,
        last_name: String,
        email: String,
        phone: String,
        truck_id: String,
        immatriculation: String,
        start_time: String,
        end_time: String,
        rest_time: String,
        latitude_start: f64,
        longitude_start: f64,
        timestamp_start: String,
        latitude_end: f64,
        longitude_end: f64,
        timestamp_end: String,
        latitude_rest: f64,
        longitude_rest: f64,
        timestamp_rest: String,
    ) -> Report {
        Report {
            driver_id: driver_id,
            first_name: first_name,
            last_name: last_name,
            email: email,
            phone: phone,
            truck_id: truck_id,
            immatriculation: immatriculation,
            start_time: start_time,
            end_time: end_time,
            rest_time: rest_time,
            latitude_start: latitude_start,
            longitude_start: longitude_start,
            timestamp_start: timestamp_start,
            latitude_end: latitude_end,
            longitude_end: longitude_end,
            timestamp_end: timestamp_end,
            latitude_rest: latitude_rest,
            longitude_rest: longitude_rest,
            timestamp_rest: timestamp_rest,
        }
    }

    pub fn is_complete(&self) -> bool {
        self.driver_id != ""
            && self.first_name != ""
            && self.last_name != ""
            && self.email != ""
            && self.phone != ""
            && self.truck_id != ""
            && self.immatriculation != ""
            && self.start_time != ""
            && self.end_time != ""
            && self.rest_time != ""
            && self.latitude_start != 0.0
            && self.longitude_start != 0.0
            && self.timestamp_start != ""
            && self.latitude_end != 0.0
            && self.longitude_end != 0.0
            && self.timestamp_end != ""
            && self.latitude_rest != 0.0
            && self.longitude_rest != 0.0
            && self.timestamp_rest != ""
    }
}