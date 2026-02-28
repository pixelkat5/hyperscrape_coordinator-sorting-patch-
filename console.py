import os
import sys
from threading import Thread
import time
from typing import Callable

import state


class Console():
    def __init__(self):
        self._should_run = True
        self._thread = Thread(target=self.main_thread)
        self._commands = {
            "help": [self.help, "Print information about available commands"],
            "list-receivers": [self.list_receivers, "List receivers"],
            "list-workers": [self.list_workers, "List workers"],
            "list-files": [self.list_file_chunks, "List workers"],
            "list-chunks": [self.list_file_chunks, "List chunks for a file (specify file id as second arg)"],
            "get-chunk": [self.get_chunk, "Get chunk info (specify chunk id as second arg)"],
            "quit": [self.quit, "Shutdown the server"]
        }

    def print(self, string):
        print(string)

    def start(self):
        self._thread.start()

    def main_thread(self):
        print("[INITIALISING MAIN CONSOLE]")
        while True:
            argv = input(">>> ").split(' ')
            command = argv[0]
            if (command in self._commands):
                try:
                    self._commands[command][0](argv)
                except Exception as e:
                    print(f"Error:\n{repr(e)}\n")
            else:
                print(f"Unknown command {command} - Use 'help' to list available commands")
            
    def help(self, argv):
        print("REGISTERED COMMANDS:")
        for command in self._commands:
            print(f"{command}\t-\t{self._commands[command][1]}")

    def list_receivers(self, argv):
        print("Receivers:")
        self.dynamic_list(list(state.receivers.keys()), displayFunction=lambda index, receiver_id: f"{index}. {receiver_id}\t-\t{state.receivers[receiver_id].hostname} ({state.receivers[receiver_id].url})")

    def list_workers(self, argv):
        print("Workers:")
        self.dynamic_list(list(state.workers.keys()), displayFunction=lambda index, worker_id: f"{index}. {worker_id}\t-\t{state.workers[worker_id].ip} (last seen {round(time.time() - state.workers[worker_id].last_seen, 2)}s ago)")

    def list_files(self, argv):
        print("Files (path, complete):")
        self.dynamic_list(list(state.files.keys()), displayFunction=lambda index, file_id: f"{index}. {file_id}\t-\t{state.files[file_id].file_path} ({state.files[file_id].complete})")

    def list_file_chunks(self, argv):
        file_id = argv[1]
        print(f"Chunks for {file_id}:")
        chunks = state.files[file_id].chunks
        self.dynamic_list(chunks, displayFunction=lambda index, chunk_id: f"{index}. {chunk_id}\t-\tUP: {state.chunks[chunk_id].uploaded} WORKERS: ({len(state.chunks[chunk_id].worker_status)})")

    def get_chunk(self, argv):
        chunk = state.chunks[argv[1]]
        print(f"Chunk: {argv[1]}")
        print(f"Uploaded: {chunk.uploaded}")
        print(f"Range: [{chunk.start}, {chunk.end}]")
        print("Workers:")
        for worker_id in chunk.worker_status:
            print(f"- {worker_id} D: {chunk.worker_status[worker_id].downloaded}\tU: {chunk.worker_status[worker_id].uploaded}")

    def dynamic_list(self, list: list[object], displayFunction: Callable[[int, object], str]):
        list_index = 0
        list_size = 10
        while self._should_run:
            list_elements = list[list_index:list_index+list_size]
            i = 0
            for element in list_elements:
                print(displayFunction(list_index+i, element))
                i += 1
            print(f"({(list_index//list_size)+1}/{(len(list)//list_size)+1})")
            command = input("(U)p, (D)own, (M)ore, (L)ess, (Q)uit >>>").upper()
            if (command == "U"):
                list_index = max(0, list_index - list_size)
            elif (command == "D"):
                list_index = min(max(len(list)-list_size, 0), list_index + list_size)
            elif (command == "M"):
                list_size += 10
            elif (command == "L"):
                list_size = max(10, list_size - 10)
            elif (command == "Q"):
                break
            else:
                print("ERR: Command must be one of U, D, M, L or Q")

    def quit(self, argv):
        self._should_run = False
        os._exit(0)