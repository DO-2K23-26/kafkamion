# Kafkamion

## Table of content

1. [Architecture](#architecture)
2. [Topic description](#topic-description)

## Architecture

![System Architecture](image.png)

### Topics

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

1. Topic 1
