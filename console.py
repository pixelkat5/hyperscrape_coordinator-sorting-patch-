import os
import sys
from threading import Thread
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
            "quit": [self.quit, "Shutdown the server"]
        }

    def print(self, string):
        print(string)

    def start(self):
        self._thread.start()

    def main_thread(self):
        print("[INITIALISING MAIN CONSOLE]")
        while True:
            user_input = input(">>> ").split(' ')
            command = user_input[0]
            if (command in self._commands):
                self._commands[command][0]()
            else:
                print(f"Unknown command {command} - Use 'help' to list available commands")
            
    def help(self):
        print("REGISTERED COMMANDS:")
        for command in self._commands:
            print(f"{command}\t-\t{self._commands[command][1]}")

    def list_receivers(self):
        print("Receivers:")
        self.dynamic_list(list(state.receivers.keys()), displayFunction=lambda index, receiver_id: f"{index}. {receiver_id}\t-\t{state.receivers[receiver_id].hostname} ({state.receivers[receiver_id].url})")
    def list_workers(self):
        print("Workers:")
        self.dynamic_list(list(state.workers.keys()), displayFunction=lambda index, worker_id: f"{index}. {worker_id}\t-\t{state.workers[worker_id].ip} ({state.workers[worker_id].ip})")

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

    def quit(self):
        self._should_run = False
        os._exit(0)