#!/usr/bin/env python3

import hashlib
import logging
import os

from multiprocessing.connection import Client
from threading import Condition, Thread
from time import sleep
from sys import argv


root_dir = os.path.abspath(argv[1])
domain = "127.0.0.1"
port = 8081

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s: [%(name)s] [%(levelname)s] %(message)s"
)

main_logger = logging.getLogger("Main")

check_files_queue = set()
send_files_queue = set()

check_files_queue_cv = Condition()
send_files_queue_cv = Condition()


def file_enqueue(queue: set, queue_cv: Condition, path: str) -> None:
    with queue_cv:
        queue.add(path)
        queue_cv.notify(1)


def file_dequeue(queue: set, queue_cv: Condition) -> str:
    with queue_cv:
        queue_cv.wait_for(lambda: len(queue) > 0)
        return queue.pop()


def fs_crawler(root: str) -> None:
    fs_logger = logging.getLogger("Crawler")
    fs_logger.info("File System Crawler started")
    with os.scandir(root) as iter:
        for entry in iter:
            if entry.is_file() and not entry.name.endswith(".md5"):
                file_enqueue(check_files_queue, check_files_queue_cv, entry.path)
                fs_logger.info(f"Added '{entry.path}' to files queue")
    fs_logger.info("Finished listing files")


def checksum_checker(id: int) -> None:
    checker_logger = logging.getLogger(f"Checker#{id}")
    checker_logger.info("Checker started")

    while True:
        file_path = file_dequeue(check_files_queue, check_files_queue_cv)
        if file_path in send_files_queue:
            # File is checked before and waiting to be sent to server.
            file_enqueue(check_files_queue, check_files_queue_cv, file_path)
            continue
        if os.path.exists(file_path + ".md5"):
            hash = hashlib.md5()
            with open(file_path, "rb") as file:
                while chunk := file.read(4096):
                    hash.update(chunk)
            
            with open(file_path + ".md5", "r") as hash_file:
                hash_file_content = hash_file.readline()
            
            hash_hex = hash.hexdigest()
            if hash_file_content != hash_hex:
                checker_logger.warning(
                    f"Incorrect hash for file '{file_path}'. Expected: '{hash_hex}', Actual: '{hash_file_content}'"
                )
                file_enqueue(send_files_queue, send_files_queue_cv, "ERR " + file_path)
            else:
                checker_logger.info(f"Hash file matched for '{file_path}'")
                continue
        else:
            checker_logger.info(f"Hash file does not exist for '{file_path}'")
        # Hash file does not exist or curropted. Send to server to (re)calculate hash.
        file_enqueue(send_files_queue, send_files_queue_cv, file_path)
        file_enqueue(check_files_queue, check_files_queue_cv, file_path)
        


main_logger.info(f"Waiting for server at {domain}:{port} to accept connection...")

with Client((domain, port)) as server:
    server.send("commander")
    main_logger.info("Connected to server")

    Thread(target=fs_crawler, args=[root_dir]).start()

    for i in range(5):
        Thread(target=checksum_checker, args=[i + 1]).start()

    while True:
        server.send("waiting")

        main_logger.info("Waiting for server to ask for file paths...")
        message = server.recv()

        main_logger.info(f"Message received from server: '{message}'")
        if message == "terminate":
            break
        elif message == "get path":
            path = file_dequeue(send_files_queue, send_files_queue_cv)
            server.send(path)
            main_logger.info(f"Sent '{path}' to the server")
        else:
            server.send("invalid message")
            main_logger.error("Message was invalid")

    server.send("terminated")
    main_logger.info("Terminating commander...")
