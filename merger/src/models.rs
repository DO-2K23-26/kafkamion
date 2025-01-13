use serde::Deserialize;
#[derive(Deserialize, Debug)]
pub struct Person {
    pub name: String,
    pub age: u32,
    pub gender: String,
}