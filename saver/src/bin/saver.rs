
use aws_sdk_s3::config::Region;
use parquet::data_type::ByteArray;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::{FileWriter, SerializedFileWriter};
use parquet::schema::parser::parse_message_type;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message as KafkaMessage;
use rdkafka::{ClientConfig, TopicPartitionList};
use std::time::Duration;
use std::fs::File;
use std::sync::Arc;
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use tokio;
use saver::models::messages::Message;
use dotenv::dotenv;

/// Transform the JSON file into a Parquet file
async fn save_to_parquet_on_minio(messages: Vec<Message>, parquet_file: &str, bucket: String, key: &str, endpoint: String, access_key: String, secret_key: String) -> Result<(), Box<dyn std::error::Error>> {

    // Define the Parquet schema
    let message_type = "
        message schema {
            REQUIRED BINARY driver_id (UTF8);
            REQUIRED BINARY first_name (UTF8);
            REQUIRED BINARY last_name (UTF8);
            REQUIRED BINARY email (UTF8);
            REQUIRED BINARY phone (UTF8);
            REQUIRED BINARY truck_id (UTF8);
            REQUIRED BINARY immatriculation;
            REQUIRED BINARY start_time (UTF8);
            REQUIRED BINARY end_time (UTF8);
            REQUIRED BINARY rest_time (UTF8);
            REQUIRED DOUBLE latitude_start;
            REQUIRED DOUBLE longitude_start;
            REQUIRED BINARY timestamp_start (UTF8);
            REQUIRED DOUBLE latitude_end;
            REQUIRED DOUBLE longitude_end;
            REQUIRED BINARY timestamp_end (UTF8);
            REQUIRED DOUBLE latitude_rest;
            REQUIRED DOUBLE longitude_rest;
            REQUIRED BINARY timestamp_rest (UTF8);
        }
    ";
    let schema = Arc::new(parse_message_type(message_type)?);

    // Create the Parquet file
    let file = File::create(parquet_file)?;
    let props = Arc::new(WriterProperties::builder().build());
    let mut writer = SerializedFileWriter::new(file, schema, props)?;

    // Add each message to the Parquet file
    {
        let mut row_group_writer = writer.next_row_group()?;
        
        macro_rules! write_column {
            ($column_writer:expr, $values:expr, $type:path) => {
                if let Some(mut col_writer) = $column_writer {
                    if let $type(ref mut typed_writer) = col_writer {
                        typed_writer.write_batch(&$values, None, None)?;
                    }
                    row_group_writer.close_column(col_writer)?;
                }
            };
        }

        // Write each column
        let driver_ids: Vec<ByteArray> = messages.iter().map(|m| ByteArray::from(m.driver_id.as_str())).collect();
        write_column!(row_group_writer.next_column()?, driver_ids, parquet::column::writer::ColumnWriter::ByteArrayColumnWriter);

        let first_names: Vec<ByteArray> = messages.iter().map(|m| ByteArray::from(m.first_name.as_str())).collect();
        write_column!(row_group_writer.next_column()?, first_names, parquet::column::writer::ColumnWriter::ByteArrayColumnWriter);

        let last_names: Vec<ByteArray> = messages.iter().map(|m| ByteArray::from(m.last_name.as_str())).collect();
        write_column!(row_group_writer.next_column()?, last_names, parquet::column::writer::ColumnWriter::ByteArrayColumnWriter);

        let emails: Vec<ByteArray> = messages.iter().map(|m| ByteArray::from(m.email.as_str())).collect();
        write_column!(row_group_writer.next_column()?, emails, parquet::column::writer::ColumnWriter::ByteArrayColumnWriter);

        let phones: Vec<ByteArray> = messages.iter().map(|m| ByteArray::from(m.phone.as_str())).collect();
        write_column!(row_group_writer.next_column()?, phones, parquet::column::writer::ColumnWriter::ByteArrayColumnWriter);

        let truck_ids: Vec<ByteArray> = messages.iter().map(|m| ByteArray::from(m.truck_id.as_str())).collect();
        write_column!(row_group_writer.next_column()?, truck_ids, parquet::column::writer::ColumnWriter::ByteArrayColumnWriter);

        let immatriculations: Vec<ByteArray> = messages.iter().map(|m| ByteArray::from(m.immatriculation.as_str())).collect();
        write_column!(row_group_writer.next_column()?, immatriculations, parquet::column::writer::ColumnWriter::ByteArrayColumnWriter);

        let start_times: Vec<ByteArray> = messages.iter().map(|m| ByteArray::from(m.start_time.as_str())).collect();
        write_column!(row_group_writer.next_column()?, start_times, parquet::column::writer::ColumnWriter::ByteArrayColumnWriter);

        let end_times: Vec<ByteArray> = messages.iter().map(|m| ByteArray::from(m.end_time.as_str())).collect();
        write_column!(row_group_writer.next_column()?, end_times, parquet::column::writer::ColumnWriter::ByteArrayColumnWriter);

        let rest_times: Vec<ByteArray> = messages.iter().map(|m| ByteArray::from(m.rest_time.as_str())).collect();
        write_column!(row_group_writer.next_column()?, rest_times, parquet::column::writer::ColumnWriter::ByteArrayColumnWriter);

        let latitude_starts: Vec<f64> = messages.iter().map(|m| m.latitude_start).collect();
        write_column!(row_group_writer.next_column()?, latitude_starts, parquet::column::writer::ColumnWriter::DoubleColumnWriter);

        let longitude_starts: Vec<f64> = messages.iter().map(|m| m.longitude_start).collect();
        write_column!(row_group_writer.next_column()?, longitude_starts, parquet::column::writer::ColumnWriter::DoubleColumnWriter);

        let timestamp_starts: Vec<ByteArray> = messages.iter().map(|m| ByteArray::from(m.timestamp_start.as_str())).collect();
        write_column!(row_group_writer.next_column()?, timestamp_starts, parquet::column::writer::ColumnWriter::ByteArrayColumnWriter);

        let latitude_ends: Vec<f64> = messages.iter().map(|m| m.latitude_end).collect();
        write_column!(row_group_writer.next_column()?, latitude_ends, parquet::column::writer::ColumnWriter::DoubleColumnWriter);

        let longitude_ends: Vec<f64> = messages.iter().map(|m| m.longitude_end).collect();
        write_column!(row_group_writer.next_column()?, longitude_ends, parquet::column::writer::ColumnWriter::DoubleColumnWriter);

        let timestamp_ends: Vec<ByteArray> = messages.iter().map(|m| ByteArray::from(m.timestamp_end.as_str())).collect();
        write_column!(row_group_writer.next_column()?, timestamp_ends, parquet::column::writer::ColumnWriter::ByteArrayColumnWriter);

        let latitude_rests: Vec<f64> = messages.iter().map(|m| m.latitude_rest).collect();
        write_column!(row_group_writer.next_column()?, latitude_rests, parquet::column::writer::ColumnWriter::DoubleColumnWriter);

        let longitude_rests: Vec<f64> = messages.iter().map(|m| m.longitude_rest).collect();
        write_column!(row_group_writer.next_column()?, longitude_rests, parquet::column::writer::ColumnWriter::DoubleColumnWriter);

        let timestamp_rests: Vec<ByteArray> = messages.iter().map(|m| ByteArray::from(m.timestamp_rest.as_str())).collect();
        write_column!(row_group_writer.next_column()?, timestamp_rests, parquet::column::writer::ColumnWriter::ByteArrayColumnWriter);

        writer.close_row_group(row_group_writer)?;
    }

    writer.close()?;
    upload_to_minio(bucket.clone(), parquet_file, key, endpoint.clone(), access_key.clone(), secret_key.clone()).await?;
    Ok(())
}

/// Upload the Parquet file to MinIO
async fn upload_to_minio(
    bucket: String,
    file_path: &str,
    key: &str,
    endpoint: String,
    access_key: String,
    secret_key: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = aws_sdk_s3::Config::builder()
        .endpoint_url(endpoint)
        .credentials_provider(aws_sdk_s3::config::Credentials::new(access_key, secret_key, None, None, "static"))
        .region(Region::new("us-east-1"))
        .build();
    let client = Client::from_conf(config);
    let parquet_data = tokio::fs::read(file_path).await?;
    let byte_stream = ByteStream::from(parquet_data);

    // Upload on MinIO
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(byte_stream)
        .send()
        .await?;

    println!("File uploaded to MinIO: {}", key);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    // Kafka configuration
    let consumer: BaseConsumer = ClientConfig::new()
        .set("group.id", "report_topic_group")
        .set("bootstrap.servers", "127.0.0.1:19092")
        .set("enable.auto.commit", "false") // Disable auto-commit
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Failed to create consumer");

    let _ = consumer.subscribe(&["report_topic"]);

    println!("Consumer started for topic 'report_topic'");

    let mut batch: Vec<Message> = Vec::new();

    loop {
        match consumer.poll(Duration::from_secs(1)) {
            Some(Ok(message)) => {
                if let Some(payload) = message.payload_view::<str>() {
                    match payload {
                        Ok(payload_str) => {
                            let msg: Message = serde_json::from_str(payload_str).unwrap();
                            batch.push(msg);

                            // All 10 messages are received
                            if batch.len() == 10 {
                                let parquet_file = "messages.parquet";
                                save_to_parquet_on_minio(
                                    batch.clone(),
                                    parquet_file,
                                    std::env::var("MINIO_BUCKET").unwrap(),
                                    format!("{}/{}_partition_{}.parquet", std::env::var("MINIO_BUCKET").unwrap(), message.partition(), chrono::Utc::now().timestamp_millis()).as_str(),
                                    std::env::var("MINIO_ENDPOINT").unwrap(),
                                    std::env::var("MINIO_ACCESS_KEY").unwrap(),
                                    std::env::var("MINIO_SECRET_KEY").unwrap(),
                                ).await.expect("Failed to save batch");
                                let mut tpl = TopicPartitionList::new();
                                for _msg in &batch {
                                    let partition = message.partition();
                                    let _ = tpl.add_partition_offset("report_topic", partition, rdkafka::Offset::Offset(message.offset() + 1));
                                }
                                consumer.commit(&tpl, rdkafka::consumer::CommitMode::Async)?;

                                batch.clear();
                            }
                        }
                        Err(e) => eprintln!("Failed to parse message payload: {:?}", e),
                    }
                }
            }
            Some(Err(e)) => {
                eprintln!("Error while polling messages: {:?}", e);
            }
            None => {
                println!("Waiting for messages...");
            }
        }
    }
}
