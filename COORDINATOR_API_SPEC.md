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
    "auth_token": "5e4bb24a-ea14-4ec8-8956-dfc551a9c2ff",
    "worker_id": "a7f84a03-32ff-44de-8640-077bcffb05be"
}
```

## GET `/chunks?n=NUM`
Gets chunks for the worker to download concurrently

### Request
- `n` - The number of chunks to retrieve

### Request Headers
- `authorization` - Set to `Bearer TOKEN` where `TOKEN` is the received auth token from `/register`

### Response
```json
{
    "9b38d89a-f7b5-4676-af33-d2c54f16fa50": {
        "url": "https://myrient.erista.me/files/No-Intro/Ouya%20-%20Ouya/iMech%20Online%20%28World%29%20%28v1.2.05%29.zip",
        "range": [0, 242422]
    }
}
```
- Map of `chunk` objects from their id to their info
    - `url` is the target URL to download
    - `range` is the target section of data to download [inclusive, exclusive]


## PUT `/status`
### Request
```json
{
    "9b38d89a-f7b5-4676-af33-d2c54f16fa50": {
        "downloaded": 100345
    }
}
```

Tell the coordinator how much this worker has downloaded so far  

### Note
- `/status` **MUST** be called at an interval lower than the configured coordinator timeout interval.
- `/status` **MUST** be called once before sending the file to `/upload`

## PUT `/upload?chunk_id=CHUNK_ID&file_id=FILE_ID`
Uploads a file from a worker for `CHUNK_ID` of `FILE_ID`

Worker just uploads the file here