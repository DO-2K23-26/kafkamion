
use aws_sdk_s3::config::Region;
use parquet::data_type::ByteArray;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::{FileWriter, SerializedFileWriter};
use parquet::schema::parser::parse_message_type;
use std::fs::File;
use std::sync::Arc;
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use tokio;
use saver::models::messages::Message;

/// Transform the JSON file into a Parquet file
fn save_to_parquet(json_file: &str, parquet_file: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Read the JSON file
    let json_data = std::fs::read_to_string(json_file)?;
    let messages: Vec<Message> = serde_json::from_str(&json_data)?;

    // Define the Parquet schema
    let message_type = "
        message schema {
            REQUIRED BINARY driver_id (UTF8);
            REQUIRED BINARY first_name (UTF8);
            REQUIRED BINARY last_name (UTF8);
            REQUIRED BINARY email (UTF8);
            REQUIRED BINARY phone (UTF8);
            REQUIRED BINARY truck_id (UTF8);
            REQUIRED INT32 immatriculation;
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

        let immatriculations: Vec<i32> = messages.iter().map(|m| m.immatriculation as i32).collect();
        write_column!(row_group_writer.next_column()?, immatriculations, parquet::column::writer::ColumnWriter::Int32ColumnWriter);

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
    println!("Parquet file saved to {}", parquet_file);
    Ok(())
}

/// Upload the Parquet file to MinIO
async fn upload_to_minio(
    bucket: &str,
    file_path: &str,
    key: &str,
    endpoint: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = aws_sdk_s3::Config::builder()
        .endpoint_url(endpoint)
        .credentials_provider(aws_sdk_s3::config::Credentials::new(access_key, secret_key, None, None, "static"))
        .region(Region::new("francecentral"))
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
    let json_file = "messages.json"; 
    let parquet_file = "messages.parquet"; 

    // Step 1: Convertir le fichier JSON en fichier Parquet
    save_to_parquet(json_file, parquet_file)?;

    // Step 2: Envoyer le fichier Parquet sur MinIO
    let bucket = "kafkamion";
    let key = "kafkamion/messages.parquet";
    let endpoint = "http://localhost:9000";
    let access_key = "xtwFTDGnt7SreXdJMKqy";
    let secret_key = "B0BsWZ3SkoNPu9h6qJnhdxGd4jUHhyqG2kciypbz";

    upload_to_minio(bucket, parquet_file, key, endpoint, access_key, secret_key).await?;
    Ok(())
}
