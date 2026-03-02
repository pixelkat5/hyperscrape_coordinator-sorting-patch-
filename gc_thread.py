import state
import time

def gc():
    last_save = time.time()
    while True:
        current = time.time()
        if (current - last_save > 600): # Save every 10 minutes
            state.save_data_files()
            last_save = current
        state.order_leaderboard()
        time.sleep(10) # Run this only every 5 seconds tops