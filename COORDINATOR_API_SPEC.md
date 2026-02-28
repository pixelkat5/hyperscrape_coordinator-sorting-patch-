## POST `/workers`
Registers a worker with the coordinator

### Request
```json
{
    "version": 1,
    "max_upload": 100,
    "max_download": 100,
    "max_per_file_speed": 4,
    "threads": 8
}
```
- `version` - the worker version
- `max_upload` - max upload speed in mbps
- `max_download` - max download speed in mbps
- `max_per_file_speed` - max per-file download speed in mbps
- `threads` - the number of available threads

### Response
```json
{
    "token": "5e4bb24a-ea14-4ec8-8956-dfc551a9c2ff"
}
```

## GET `/chunks?n=NUM`
Gets chunks for the worker to download concurrently

### Request
- `n` - The number of chunks to retrieve

### Request Headers
- `authorization` - Set to `Bearer TOKEN` where `TOKEN` is the received response from `/register`

### Response
```json
{
    "9b38d89a-f7b5-4676-af33-d2c54f16fa50": {
        "url": "https://myrient.erista.me/files/No-Intro/Ouya%20-%20Ouya/iMech%20Online%20%28World%29%20%28v1.2.05%29.zip",
        "range": [0, 242422],
        "destination": "https://uploader.hackerdude.tech/upload"
    }
}
```
- Map of `chunk` objects from their id to their info
    - `url` is the target URL to download
    - `range` is the target section of data to download [inclusive, exclusive]
    - `destination` is the URL to the uploaded to upload files to (see `hyperscrape_receiver`)


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

### Request Headers
- `authorization` - Set to `Bearer TOKEN` where `TOKEN` is the received response from `/register`

### Response
No content, returns status code `200`

# For Receivers
## POST `/receivers`
Registers a receiver with the coordinator

### Request
```json
{
    "url": "https://minerva-receiver.hackerdude.tech/upload"
    "max_upload": 100,
    "receiver_token": "c363b4fe-2c75-44a7-a3df-28064ef99630",
    "hostname": "archbook"
}
```
- `url` - The upload endpoint
- `max_upload` - max upload speed in mbps
- `receiver_token` - Secret token used to ensure receivers are authorized
- `hostname` - The hostname of the receiver

### Response
```json
{
    "token": "5e4bb24a-ea14-4ec8-8956-dfc551a9c2ff"
}
```

## GET `/files`
Get all files currently handled by this receiver

### Request Headers
- `authorization` - Set to `Bearer TOKEN` where `TOKEN` is the received response from `/register`

### Response
```json
{
    "bf1ad24f-a0df-4dd2-ba56-e732a6c7d0f9": {
        "file_path": "/No-Intro/wjhatever",
        "total_size": 34323,
        "url": "https://myrient.erista.me/files/No-Intro/Ouya%20-%20Ouya/iMech%20Online%20%28World%29%20%28v1.2.05%29.zip",
        "chunks": {
            "9b38d89a-f7b5-4676-af33-d2c54f16fa50": [0, 242422]
        }
    }
}
```
- Returns a map of file IDs to their info
- NOTE: Only returns chunks assigned to this receiver

## GET `/worker_info?token=TOKEN`
Get a worker's info from a token

### Request Headers
- `authorization` - Set to `Bearer TOKEN` where `TOKEN` is the received response from `/register_worker`

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
Tell the coordinator this receiver is still alive and also uploads some basic data

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
- Also tells the coordinator whether each chunk is received or not

### Request Headers
- `authorization` - Set to `Bearer TOKEN` where `TOKEN` is the received response from `/register_worker`

### Response
returns no content, 200