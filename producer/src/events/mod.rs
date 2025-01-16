pub mod driver;
pub mod time_registration;
pub mod truck;
pub mod position;

pub trait EventSource {
    fn generate(&self) -> Vec<String>;
}

pub trait ReusableEventSource<T> {
    fn generate_with_id(&self) -> (Vec<String>, Vec<T>);
}
