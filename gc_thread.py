import state
import time

def gc():
    last_save = time.time()
    while True:
        current = time.time()
        with state.workers_lock:
            for worker_id in list(state.workers.keys()):
                if (current - state.workers[worker_id].get_last_seen() > state.config["general"]["worker_timeout"]):
                    del state.workers[worker_id]
        if (current - last_save > 600): # Save every 10 minutes
            state.save_data_files()
            last_save = current
        time.sleep(state.config["general"]["worker_timeout"])