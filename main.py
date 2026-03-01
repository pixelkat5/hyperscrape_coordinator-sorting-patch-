import os
from uuid import uuid4
from files import HyperscrapeFile, WorkerStatus
import state
from flask import Flask, request
import hashlib

from helpers import get_auth_token, get_request_ip, get_url_size, get_worker

print("=========================")
print("=  HYPERSCRAPE SERVER   =")
print("= Created By Hackerdude =")
print("=========================")

app = Flask(__name__)

@app.route("/")
def root():
    return 'Hyperscrape Coordinator - Created by <a href="https://hackerdude.tech">Hackerdude</a>'

@app.route("/workers", methods=['POST'])
def register_worker():
    ip = get_request_ip()
    if (ip in state.banned_ips):
        return {"error": "Could not connect to worker"}, 403
    #for worker_id in list(state.workers.keys()):
    #    if (state.workers[worker_id].ip == ip):
    #        state.remove_worker(worker_id)
    data = request.json
    if (data == None or
        (not "version" in data) or
        (not "max_upload" in data) or 
        (not "max_download" in data) or
        (not "max_per_file_speed" in data) or
        (not "threads" in data)):
        return {"error": "Invalid Request"}, 400
    if (data["version"] > state.config['general']['version']):
        return {"error": f"Version mismatch, expected {state.config['general']['version']}"}, 400
    auth_token = state.add_worker(ip, data["max_upload"], data["max_download"], data["max_per_file_speed"], data["threads"])
    return {
        "worker_id": auth_token.id,
        "auth_token": auth_token.as_token()
    }

@app.route("/chunks", methods=['GET'])
def get_chunks():
    worker = get_worker()
    if (not worker):
        return {"error": "Invalid token!"}, 403
    
    chunks_to_get = int(request.args.get("n", worker.max_download/worker.max_per_file_speed))
    # Get files with high worker counts
    # So the entire network is working together for a single file essenially
    chunks_to_download = []
    file_download_candidate_offset = 0
    while len(chunks_to_download) < chunks_to_get and file_download_candidate_offset < len(state.sorted_downloadable_files):
        files_to_download_candidates = state.sorted_downloadable_files[file_download_candidate_offset:file_download_candidate_offset+chunks_to_get]
        for file_id in files_to_download_candidates:
            downloading_file_already = False
            # Ensure the worker isn't currently downloading this file
            for chunk_id in state.files[file_id].chunks:
                # Cleanup workers that have not uploaded in a while
                state.cleanup_chunk_workers(chunk_id)
                if (worker.worker_id in state.chunks[chunk_id].worker_status and not state.chunks[chunk_id].worker_status[worker.worker_id].complete):
                    downloading_file_already = True # If worker is CURRENTLY downloading this FILE then we skip the entire file
                    break
            
            if (downloading_file_already):
                continue

            highest_chunk_id = None
            for chunk_id in state.files[file_id].chunks:
                # Get the chunk in this file with the highest number of downloaders under trust_count
                if (len(state.chunks[chunk_id].worker_status) >= state.config["general"]["trust_count"]):
                    continue
                if (worker.worker_id in state.chunks[chunk_id].worker_status):
                    continue # If worker has already downloaded THIS chunk then we skip it from candidates
                if (highest_chunk_id == None or len(state.chunks[chunk_id].worker_status) > len(state.chunks[highest_chunk_id].worker_status)):
                    highest_chunk_id = chunk_id
            if (highest_chunk_id != None):
                chunks_to_download.append(highest_chunk_id)
        file_download_candidate_offset += chunks_to_get

    ###
    # We now have a list of chunks to download
    ###
    response = {}
    for chunk_id in chunks_to_download:
        chunk = state.chunks[chunk_id]
        file = state.files[state.chunk_to_file[chunk_id]]
        state.chunks[chunk_id].worker_status[worker.worker_id] = WorkerStatus()
        response[chunk_id] = {
            "url": file.url,
            "range": [
                chunk.start,
                chunk.end
            ]
        }
    return response

@app.route("/status", methods=['PUT'])
def put_status():
    worker = get_worker()
    if (not worker):
        return {"error": "Invalid token!"}, 403
    data = request.json
    for chunk_id in data:
        if (not worker.worker_id in state.chunks[chunk_id].worker_status):
            continue
        chunk = state.chunks[chunk_id]
        worker_status = chunk.worker_status[worker.worker_id]
        worker_status.downloaded = data[chunk_id]["downloaded"]
        worker_status.mark_updated()
    return {"ok": "ok"}, 200

@app.route("/ping", methods=['GET'])
def still_alive():
    worker = get_worker()
    if (not worker):
        return {"error": "Invalid token!"}, 403
    worker.update_last_seen()


@app.route("/upload", methods=["PUT"])
def upload_file():
    worker = get_worker()
    if (not worker):
        return {"error": "Invalid token!"}, 403
    chunk_id = request.args.get("chunk_id", None)
    file_id = request.args.get("file_id", None) # @TODO

    if (chunk_id != None):
        # Ensure the chunk exists
        if (not chunk_id in state.chunks):
            return {"error": "Unknown chunk"}, 400
        if (not worker.worker_id in state.chunks[chunk_id].worker_status):
            return {"error": "Chunk not requested"}, 400
        if (state.chunks[chunk_id].worker_status[worker.worker_id].complete):
            return {"error": "Chunk already complete"}, 400

        chunk = state.chunks[chunk_id]
        worker_status = chunk.worker_status[worker.worker_id]
        # Handle chunk uploading
        chunk_file_object = state.files[state.chunk_to_file[chunk_id]]
        temp_storage_folder = os.path.join(state.config["paths"]["chunk_temp_path"], chunk_file_object.file_path)
        os.makedirs(temp_storage_folder, exist_ok=True)
        storage_path = os.path.join(temp_storage_folder, f"chunk_{chunk.chunk_id}_{worker.worker_id}.bin")
        with open(storage_path, "wb") as file:
            chunk_hash = hashlib.md5()
            worker_status.uploaded = 0
            stream_data = request.stream.read()
            while (len(stream_data) > 0):
                worker_status.uploaded += len(stream_data)
                worker_status.mark_updated()
                file.write(stream_data)
                chunk_hash.update(stream_data)
                stream_data = request.stream.read()
        if (os.path.exists(storage_path) and os.stat(storage_path).st_size == worker_status.downloaded):
            worker_status.mark_complete(chunk_hash.hexdigest()) # This chunk is now complete
        else:
            if (os.path.exists(storage_path)):
                os.remove(storage_path)
            del chunk.worker_status[worker.worker_id]
            return {"error": "Error processing chunk"}, 500

        # Check that this hash matches the others that are complete
        chunk_hashes = {}
        for worker_id in chunk.worker_status:
            if (worker_id == worker.worker_id):
                continue # it's us lol

            worker_status = chunk.worker_status[worker_id]
            if (not worker_status.complete):
                continue
            chunk_hashes[worker_status.hash] = chunk_hashes.get(worker_status.hash, 0) + 1

        if (len(chunk_hashes) > 1): # There are mismatched hashes!
            most_popular_hash = None
            for hash in chunk_hashes:
                if (most_popular_hash == None or chunk_hashes[most_popular_hash] < chunk_hashes[hash]):
                    most_popular_hash = hash
            for worker_id in list(chunk.worker_status.keys()):
                worker_status = chunk.worker_status[worker_id]
                if (not worker_status.complete):
                    continue
                if (worker_status.hash != most_popular_hash):
                    # Delete mismatched workers from chunk stuff
                    del chunk.worker_status[worker_id]
                    os.remove(os.path.join(temp_storage_folder, f"chunk_{chunk.chunk_id}_{worker_id}.bin")) # Remove the chunk this worker downloaded
            return {"result": "Upload had a mismatched hash, you can ignore this"}, 200 # We've processed the upload from the client, don't come back regardless of what happened
        
        # If the hashes weren't mismatched...
        if (len(chunk.worker_status) < state.config["general"]["trust_count"]): # Check that we have all the chunks responses we need
            return {"ok": "Upload looks good so far"}, 200
        for worker_id in chunk.worker_status: # Check that they're all complete
            worker_status = chunk.worker_status[worker_id]
            if (not worker_status.complete):
                return {"ok": "Upload looks good so far"}, 200 # If any of the workers aren't complete we just skip this
        
        # So all the hashes are good
        # AND we have responses that are complete for every response for this chunk?
        # We can remove the other chunks and just keep ours
        worker_ids = list(chunk.worker_status)
        for worker_id in worker_ids[1:]: # Delete all but 1
            os.remove(os.path.join(temp_storage_folder, f"chunk_{chunk.chunk_id}_{worker_id}.bin"))
        os.rename(os.path.join(temp_storage_folder, f"chunk_{chunk.chunk_id}_{worker_ids[0]}.bin"), os.path.join(temp_storage_folder, f"chunk_{chunk.start}.bin"))

        # Check if the whole file is complete
        if (not state.check_file_complete(chunk_file_object.file_id)):
            return {"ok": "This chunk is validated"}, 200 # We're not yet done with the whole file despite being done with this chunk!
        # If we are done though, then we should construct and move the entire file
        chunk_files = []
        for chunk_id in chunk_file_object.chunks:
            chunk = state.chunks[chunk_id]
            chunk_files.append(os.path.join(temp_storage_folder, f"chunk_{chunk.start}.bin"))

        # Now we construct the final file!
        md5_hash = hashlib.md5()
        sha1_hash = hashlib.sha1()
        sha256_hash = hashlib.sha256()
        destination_path = os.path.join(state.config["paths"]["storage_path"], chunk_file_object.file_path)
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)
        with open(destination_path, 'wb') as main_file:
            for chunk_file_path in chunk_files:
                with open(chunk_file_path, 'rb') as chunk_file_stream:
                    read_size = 1024**2 * 10
                    data = chunk_file_stream.read(read_size) # Read 10MB at a time
                    while (len(data) > 0):
                        main_file.write(data)
                        md5_hash.update(data)
                        sha1_hash.update(data)
                        sha256_hash.update(data)
                        data = chunk_file_stream.read(read_size)
                os.remove(chunk_file_path)
        # write hashes to file
        state.file_hashes[chunk_file_object.file_path] = {
            "md5": md5_hash.hexdigest(),
            "sha1": sha1_hash.hexdigest(),
            "sha256": sha256_hash.hexdigest()
        }
        state.save_file_hashes()
        state.sorted_downloadable_files.remove(chunk_file_object.file_id) # We don't want to download this again
        
        return {"ok": "Upload entire file complete!"}, 200

###
# FOR DEBUGGING ONLY!!!!
# @TODO @FIXMe
###
state.files = {}
state.chunks = {}
state.chunk_to_file = {}
state.sorted_downloadable_files = []
state.file_worker_counts = {}
test_urls = [
#    ("https://file-examples.com/storage/fe3c7a89a169a3cde95f28c/2017/04/file_example_MP4_1920_18MG.mp4", "file_example_MP4_1920_18MG.mp4"),
#    ("https://file-examples.com/storage/fe3c7a89a169a3cde95f28c/2017/04/file_example_MP4_480_1_5MG.mp4", "file_example_MP4_480_1_5MG.mp4"),
    ("https://myrient.erista.me/files/No-Intro/ACT%20-%20Apricot%20PC%20Xi/%5BBIOS%5D%20MS-DOS%202.11%20%28Europe%29%20%28v3.1%29%20%28Disk%201%29%20%28OS%29.zip", "[BIOS] MS-DOS 2.11 (Europe) (v3.1) (Disk 1) (OS).zip"),
    ("https://www.hackerdude.tech/vault/805653383-%e5%9b%9b%e8%b6%b3%e6%9c%ba%e5%99%a8%e4%ba%baSpot%e5%bd%bb%e5%ba%95%e6%8b%86%e8%a7%a3%e6%8a%a5%e5%91%8a.pdf", "805653383-四足机器人Spot彻底拆解报告.pdf")
]
for test_url in test_urls:
    state.add_file(HyperscrapeFile(
        str(uuid4()),
        test_url[1],
        get_url_size(test_url[0]),
        test_url[0],
        (1024*1024)*2
    ))

if __name__ == "__main__":
    from waitress import serve
    state.console.print(f'Listening on {state.config["server"]["port"]}')
    state.console.start()
    serve(app, host="0.0.0.0", port=state.config["server"]["port"], threads=state.config["server"]["threads"], backlog=4096)