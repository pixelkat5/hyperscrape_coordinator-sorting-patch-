import time
from threading import Lock

class WorkerChunkStatus():
    def __init__(self, downloaded: int, uploaded: int):
        self.downloaded = downloaded
        self.uploaded = uploaded

class Worker():
    def __init__(self, worker_id: str, ip: str, auth_nonce: str, max_upload: int, max_download: int, max_per_file_speed: int, threads: int):
        self.worker_id = worker_id
        self.ip = ip
        self.auth_nonce = auth_nonce
        self.max_upload = max_upload
        self.max_download = max_download
        self.max_per_file_speed = max_per_file_speed
        self.threads = threads
        self.last_seen = time.time()

    def update_last_seen(self):
        self.last_seen = time.time()