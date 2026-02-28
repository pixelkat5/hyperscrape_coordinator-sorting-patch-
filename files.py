from uuid import uuid4

from receivers import Receiver

class AssignedChunks():
    def __init__(self):
        self._assigned_chunks: dict[str, str] = {}
        self._chunk_workers: dict[str, list[str]] = {} # Reverse mapping
    
    def assign_chunk(self, worker_id, chunk_id):
        self._assigned_chunks[worker_id] = chunk_id
        if (not chunk_id in self._chunk_workers):
            self._chunk_workers = []
        self._chunk_workers[chunk_id].append(worker_id)

    def remove_worker(self, worker_id):
        self._chunk_workers[self._assigned_chunks[worker_id]].remove(worker_id)
        self._assigned_chunks[worker_id]

class WorkerStatus():
    def __init__(self, downloaded: int, uploaded: int):
        self.downloaded = downloaded
        self.uploaded = uploaded

###
# We store files and chunks separately because I like, hate myself
# It's also more efficient
###
class HyperscrapeChunk():
    def __init__(self, start: int, end: int):
        self.start = start
        self.end = end
        self.uploaded = False
        self.worker_status: dict[str, WorkerStatus] = {}

class HyperscrapeFile():
    def __init__(self, file_id: str, file_path: str, total_size: int, url: str, chunk_size: int):
        self.file_id = file_id
        self.file_path = file_path
        self.total_size = total_size # In bytes
        self.url = url
        self.chunk_size = chunk_size
        self.chunks: list[str] = []
        self.complete = False # Whether or not the file is totally complete or not
        self.receiver: str|None = None