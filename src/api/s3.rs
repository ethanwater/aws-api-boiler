use aws_sdk_s3 as s3;
use s3::primitives::ByteStream;
use std::fs::File;
use std::io::Write;

pub struct S3 {
    client: s3::Client,
    bucket: String,
}

impl S3 {
    pub async fn new(client: s3::Client, bucket: String) -> Self {
        Self {
            client: client,
            bucket: bucket,
        }
    }
    pub async fn fetch_bucket_objects(&self) -> Result<Vec<aws_sdk_s3::types::Object>, s3::Error> {
        let mut response = self
            .client
            .list_objects_v2()
            .bucket(self.bucket.to_owned())
            .into_paginator()
            .send();

        while let Some(result) = response.next().await {
            match result {
                Ok(output) => {
                    return Ok(output.contents().to_vec());
                }
                Err(err) => {
                    eprintln!("{err:?}");
                }
            }
        }

        let vacant_bucket: Vec<aws_sdk_s3::types::Object> = Vec::new();
        Ok(vacant_bucket)
    }

    pub async fn download_bucket_object(&self, object_key: String) -> Result<(), aws_sdk_s3::primitives::ByteStreamError> {
        let response = self.client
            .get_object()
            .bucket(&self.bucket)
            .key(object_key.clone())
            .send()
            .await;
    
        match response {
            Ok(result) => {
                dbg!(&result);
                let stream = result.body.collect().await?.into_bytes();
                {
                    let mut file = File::create(object_key.as_str())?;
                    file.write_all(&stream)?;
                }
            }
            Err(err) => {
                eprintln!("{:?}", err);
            }
        }
    
        Ok(())
    }

    pub async fn upload_bucket_object(&self, bytes: ByteStream, filename: &str) -> Result<(), aws_sdk_s3::primitives::ByteStreamError> {
        let response = self.client
            .put_object()
            .bucket(&self.bucket)
            .key(filename)
            .body(bytes)
            .send()
            .await;

        match response {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{:?}", err);
            }
        }
        Ok(())
    }

    pub async fn delete_bucket_object(&self, object: &aws_sdk_s3::types::Object) -> Result<(), s3::Error> {
        let response = self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(object.key().unwrap())
            .send()
            .await;

        match response {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{:?}", err);
            }
        }
        Ok(())
    }

    pub async fn multipart_upload(
        &self,
        key: &str,
    ) -> Result<(), s3::Error> {
        let start_time = std::time::Instant::now();
        let multipart_init_response = self.client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await;

        let upload_id = multipart_init_response?.upload_id.unwrap();
        let mut completed_parts: Vec<aws_sdk_s3::types::CompletedPart> = Vec::new();
        let (chunks, mut part_number) = (chunkify(key).await.unwrap(), 1);

        for chunk in chunks {
            let upload_part_response = self.client
                .upload_part()
                .body(chunk.into())
                .bucket(&self.bucket)
                .key(key)
                .part_number(part_number)
                .upload_id(&upload_id)
                .send()
                .await
                .unwrap();

            let part = aws_sdk_s3::types::CompletedPart::builder()
                .set_part_number(Some(part_number))
                .set_e_tag(Some(upload_part_response.e_tag.unwrap()))
                .build();

            completed_parts.push(part);
            part_number += 1;
        }

        let completed_multipart_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();

        let multipart_close_response = self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(completed_multipart_upload)
            .send()
            .await;

        match multipart_close_response {
            Ok(_) => {
                println!(
                    "success: {key} -> {:?} | time elapsed: {:?}",
                    &self.bucket,
                    start_time.elapsed()
                );
            }
            Err(err) => {
                eprintln!("error: {:?}", err);
            }
        }
        Ok(())
    }
}

const MB_5: usize = 5_242_880;

async fn chunkify(file: &str) -> Result<Vec<ByteStream>, ()> {
        let bytestream = ByteStream::from_path(file).await.unwrap();
        let data = bytestream.collect().await.unwrap().into_bytes();
        let size = data.len(); 
        if size < MB_5 {
            return Err(());
        }

        let amount: i32 = size as i32 / MB_5 as i32;
        let mut counter = 0;
        let mut chunks: Vec<ByteStream> = Vec::new();

        for current_chunk in 0..=amount {
            if current_chunk != amount {
                let chunk = &data[counter..counter + MB_5].to_vec();
                let chunk_bytestream = ByteStream::from(chunk.to_owned());
                chunks.push(chunk_bytestream);
                counter += MB_5
            } else {
                let chunk = &data[counter..].to_vec();
                let chunk_bytestream = ByteStream::from(chunk.to_owned());
                chunks.push(chunk_bytestream);
            }
        }

       Ok(chunks)
    }


