# Kafkamion

## Table of content

1. [Architecture](#architecture)
2. [Topics](#topics)
3. [Topic description](#topic-description)
4. [Agregation](#agregation)
5. [Topic result](#topic-result)
6. [How to run](#how-to-run)

## Architecture

![System Architecture](image.png)

## Topics

1. Entity registration

```json
{
    "type": "string",
    "driver_id": "string",
    "first_name": "string",
    "last_name": "string",
    "email": "string",
    "phone": "string"
}
```

or

```json
{
    "type": "string",
    "truck_id": "string",
    "immatriculation": "string"
}
```

2. Time Registration `time_registration`

```json
{
    "type": "string",
    "timestamp": "string",
    "driver_id": "string",
    "truck_id": "string"
}
```

3. Position

```json
{
    "truck_id": "string",
    "latitude": "number",
    "longitude": "number",
    "timestamp": "string"
}
```

## Topic description

Each Topic is defined as follow :

1. **Topic 1**
The type can be `driver` or `truck` and will be handled differently. The key is the `driver_id` or the `truck_id` and the value is the information of the driver or the truck.
This topic is called `entity_topic`

2. **Topic 2**
This topic is used to store the time registration of the driver. The key is the type of the time registration and the value is the timestamp.
This topic is called `time_registration_topic`

3. **Topic 3**
This topic is used to store the position of the driver. The key is the `truck_id` and the value is the position of the driver.
This topic is called `position_topic`

## Agregation

The aggregation is done by the `driver_id` between the first topic and the second. The aggregation is done by the `truck_id` for the second and the third topic.

## Topic result

The result of the merge, called `report`, will be a json flat topic of our three producers topics.
The topic is called `report_topic`

```json
{
    "driver_id": "string",
    "first_name": "string",
    "last_name": "string",
    "email": "string",
    "phone": "string",
    "truck_id": "string",
    "immatriculation": "string",
    "start_time": "string",
    "end_time": "string",
    "rest_time": "string",
    "latitude_start": "number",
    "longitude_start": "number",
    "timestamp_start": "string",
    "latitude_end": "number",
    "longitude_end": "number",
    "timestamp_end": "string",
    "latitude_rest": "number",
    "longitude_rest": "number",
    "timestamp_rest": "string"
}
```

## How to run
We use docker-compose to run our system. To run the stack, you need to run the following command:

1. Up the stack
```bash
docker-compose up -d
```

2. Connect to a bash container
```bash
docker exec --workdir /opt/kafka/bin/ -it <shaKafka> sh
```
**Note**: `<shaKafka>` is the sha of the kafka container. You can get it by running `docker ps`

3. Create the topics
```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic position_topic --partitions 1 --replication-factor 1
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic time_registration_topic --partitions 1 --replication-factor 1
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic report_topic --partitions 1 --replication-factor 1
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic entity_topic --partitions 1 --replication-factor 1
```

4. In another terminal, run the producer
```bash
./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic time_registration_topic
```
**Note**: You can replace `time_registration_topic` by `position_topic` or `entity_topic`