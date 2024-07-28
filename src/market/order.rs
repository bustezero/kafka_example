use axum::{extract::Json, response::IntoResponse};
use log::info;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Order {
    pub id: u64,
    pub item: String,
    pub quantity: u32,
}