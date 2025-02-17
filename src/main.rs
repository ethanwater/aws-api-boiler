pub mod api;
use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use api::s3::S3;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::primitives::ByteStream;
use aws_config::BehaviorVersion;
use aws_types::region::Region;
use std::env;
use aws_sdk_s3;
use aws_sdk_s3::types::Object;

#[actix_web::main]
async fn main() { 
    let region_provider =
        RegionProviderChain::first_try(env::var("AWSREGION").ok().map(Region::new))
            .or_default_provider()
            .or_else(Region::new("us-east-1"));
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;

    let client = aws_sdk_s3::Client::new(&config);
    let s3m: S3 = S3::new(client, std::env::var("AWSBUCKET").unwrap()).await;
}

#[get("/fetch/object/all")]
async fn fetch_objects(s3m: web::Data<S3>) -> impl Responder {
    let _: Vec<Object> = s3m.fetch_bucket_objects().await.unwrap();
    HttpResponse::Ok()
}

#[get("/fetch/object/{key}")]
async fn fetch_object(s3m: web::Data<S3>, key: web::Path<String>) -> impl Responder{
    HttpResponse::NotImplemented()
}

#[get("/download/object/{key}")]
async fn download_object(s3m: web::Data<S3>, key: web::Path<String>) -> impl Responder {
    let response = s3m.download_bucket_object(key.to_string()).await;
    match response {
        Ok(_) => {
            return HttpResponse::Ok();

        }
        Err(_) => {
            return HttpResponse::BadRequest();
        }
    }
}

#[post("/upload/object/{key}/{option}")]
async fn upload_object(s3m: web::Data<S3>, key: web::Path<String>, option: web::Path<String>) -> impl Responder {
    //TODO: revise HttpResponse's
    match option.to_string().as_str() {
        "multi" => {
            if let Ok(_) = s3m.multipart_upload(&key.to_string()).await {
                return HttpResponse::Ok()
            }
            return HttpResponse::BadRequest();
        },
        "default" => {
            if let Ok(bytestream) =  ByteStream::from_path(key.to_string()).await {
                let response = s3m.upload_bucket_object(bytestream, &key).await;
                match response {
                    Ok(_) => {
                        return HttpResponse::Ok();                        }
                    Err(_) => {
                        return HttpResponse::BadRequest();
                    }
                }
            };
            return HttpResponse::BadRequest();
        }
        _ => {
            return HttpResponse::BadRequest();
        }
    }
}




