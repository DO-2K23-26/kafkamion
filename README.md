# Kafkamion

## Table of content

1. [Architecture](#architecture)
2. [Topics](#topics)
3. [Topic description](#topic-description)

## Architecture

![System Architecture](image.png)

## Topics

1. Driver

```json
{
    "driver_id": "string",
    "first_name": "string",
    "last_name": "string",
    "email": "string",
    "phone": "string"
}
```

2. Time Registration `time_registration`

```json
{
    "type": "string",
    "timestamp": "string"
}
```

3. Position

```json
{
    "latitude": "number",
    "longitude": "number",
    "timestamp": "string"
}
```

## Topic description

Each Topic is defined as follow :

1. **Topic 1**
This topic is used to store the driver information. The key is the driver_id and the values are the driver information.

2. **Topic 2**
This topic is used to store the time registration of the driver. The key is the type of the time registration and the value is the timestamp.

3. **Topic 3**
This topic is used to store the position of the driver. The key is the truck_id and the value is the position of the driver.
