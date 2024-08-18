use crate::db::Order;
use async_trait::async_trait;
use axum::{
    body::Body,
    extract::{FromRequest, Json},
    http::{Request, Response},
    middleware::Next,
};
use log::info;
use std::convert::Infallible;
use std::time::Instant;
use uuid::Uuid;

#[async_trait]
trait Interceptor {
    async fn intercept(&self, body: Order) -> Result<Order, String>;
}

struct OrderInterceptor;

#[async_trait]
impl Interceptor for OrderInterceptor {
    async fn intercept(&self, mut order: Order) -> Result<Order, String> {
        // 对 order 进行验证或修改
        if order_is_valid(&order) {
            // 例如，可以在这里修改 order 的某些字段
            // order.field = new_value;
            if order.order_id.is_none() {
                order.order_id = Some(Uuid::new_v4());
            }
            order.created_at = Some(chrono::Utc::now());
            order.updated_at = Some(chrono::Utc::now());
            Ok(order)
        } else {
            Err("Order validation failed".into())
        }
    }
}

fn get_interceptor(path: &str) -> Option<Box<dyn Interceptor + Send + Sync>> {
    if path.starts_with("/order") {
        Some(Box::new(OrderInterceptor))
    } else {
        None
    }
}

pub async fn log_request(mut req: Request<Body>, next: Next) -> Result<Response<Body>, Infallible> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let start = Instant::now();

    // 提取并解析请求体
    if let Some(interceptor) = get_interceptor(&path) {
        let (parts, body) = req.into_parts();
        let Json(order): Json<Order> =
            match Json::from_request(Request::from_parts(parts.clone(), body), &()).await {
                Ok(json) => json,
                Err(e) => {
                    return Ok(Response::new(Body::from(format!(
                        "Failed to parse order: {}",
                        e
                    ))))
                }
            };

        // 调用 interceptor 对 order 进行处理
        match interceptor.intercept(order).await {
            Ok(modified_order) => {
                // 如果成功，可以选择用修改后的 order 重新构建请求体
                let new_body = Body::from(serde_json::to_string(&modified_order).unwrap());
                req = Request::from_parts(parts, new_body);
            }
            Err(error_message) => return Ok(Response::new(Body::from(error_message))),
        }
    }

    let response = next.run(req).await;
    let duration = start.elapsed();

    info!("{} {} took {:?}", method, path, duration);

    Ok(response)
}

fn order_is_valid(order: &Order) -> bool {
    //     info!("validating order: {}", serde_json::to_string(order).unwrap_or_else(|_| "Failed to serialize order".to_string())); // 打印Order对象，进行调试
    info!("validating order: {:?}", order); // 打印Order对象，进行调试
    true
}
