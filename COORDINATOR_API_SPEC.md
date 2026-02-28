## POST `/worker`
Registers a worker with the coordinator

### Request
```json
{
    "max_upload": 100,
    "max_download": 100,
    "threads": 8
}
```
- `max_upload` - max upload speed in mbps
- `max_download` - max download speed in mbps
- `threads` - the number of available threads

### Response
```json
{
    "token": "5e4bb24a-ea14-4ec8-8956-dfc551a9c2ff"
}
```

## GET `/chunks`
Gets chunks for the worker to download concurrently

### Request Headers
- `authorization` - Set to `Bearer TOKEN` where `TOKEN` is the recieved response from `/register`

### Response
```json
[
    {
        "chunks_id": "9b38d89a-f7b5-4676-af33-d2c54f16fa50",
        "url": "https://myrient.erista.me/files/No-Intro/Ouya%20-%20Ouya/iMech%20Online%20%28World%29%20%28v1.2.05%29.zip",
        "range": [0, 242422],
        "destination": "https://uploader.hackerdude.tech/upload"
    }
]
```
- Array of `chunk` objects
    - `chunk_id` a uuid to represent the chunk
    - `url` is the target URL to download
    - `range` is the target section of data to download [inclusive, exclusive]
    - `destination` is the URL to the uploaded to upload files to (see `hyperscrape_reciever`)


## PUT `/status`
### Request
```json
{
    "9b38d89a-f7b5-4676-af33-d2c54f16fa50": {
        "downloaded": 100345,
        "uploaded": 0
    }
}
```
- `current_progress` is a JSON array with the current chunks and their progress, the numbers are measured in bytes

### Request Headers
- `authorization` - Set to `Bearer TOKEN` where `TOKEN` is the recieved response from `/register`

### Response
No content, returns status code `200`

# For Recievers
## POST `/register_reciever`
Registers a reciever with the coordinator

### Request
```json
{
    "max_upload": 100,
    "reciever_token": "c363b4fe-2c75-44a7-a3df-28064ef99630"
}
```
- `max_upload` - max upload speed in mbps
- `reciever_token` - Secret token used to ensure recievers are authorized

### Response
```json
{
    "token": "5e4bb24a-ea14-4ec8-8956-dfc551a9c2ff"
}
```

## GET `/files`
Get all files currently handled by this reciever

### Request Headers
- `authorization` - Set to `Bearer TOKEN` where `TOKEN` is the recieved response from `/register`

### Response
```json
{
    [
        "file_uuid": "bf1ad24f-a0df-4dd2-ba56-e732a6c7d0f9",
        "file_path": "/No-Intro/wjhatever",
        "total_size": 34323,
        "url": "https://myrient.erista.me/files/No-Intro/Ouya%20-%20Ouya/iMech%20Online%20%28World%29%20%28v1.2.05%29.zip",
        "chunks": {
            "9b38d89a-f7b5-4676-af33-d2c54f16fa50": [0, 242422]
        }
    ]
}
```
- Returns a map of chunk IDs to their info
- NOTE: Only returns chunks assigned to this reciever

## GET `/worker_info?token=TOKEN`
Get a worker's info from a token

### Request Headers
- `authorization` - Set to `Bearer TOKEN` where `TOKEN` is the recieved response from `/register_worker`

### Request
- `TOKEN` - The worker's token

### Response
```json
{
    "max_upload": 100,
    "max_download": 100,
    "threads": 8,
    "status": {
        "9b38d89a-f7b5-4676-af33-d2c54f16fa50": {
            "downloaded": 100345,
            "uploaded": 0
        }
    }
}
```

## PUT `/status`
Tell the coordinator this reciever is still alive and also uploads some basic data

### Request
```json
{
    "bf1ad24f-a0df-4dd2-ba56-e732a6c7d0f9":
    {
        "uploaded": 0,
        "chunks": {
            "9b38d89a-f7b5-4676-af33-d2c54f16fa50": false
        }
    }
}
```
- Tells the coordinator how much of each file has been uploaded
- Also tells the coordinator whether each chunk is recieved or not

### Request Headers
- `authorization` - Set to `Bearer TOKEN` where `TOKEN` is the recieved response from `/register_worker`

### Response
returns no content, 200