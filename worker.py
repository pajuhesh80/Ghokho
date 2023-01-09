#!/usr/bin/env python3

import hashlib
from multiprocessing.connection import Client
import logging
from os import path

domain = "127.0.0.1"
port = 8081

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s: [%(levelname)s] %(message)s"
)


def generate_hash_file(file_path: str) -> None:
    hash = hashlib.md5()

    with open(file_path, "rb") as file:
        while chunk := file.read(4096):
            hash.update(chunk)

    with open(file_path + ".md5", "w") as hash_file:
        hash_file.write(hash.hexdigest())


logging.info(f"Waiting for server at {domain}:{port} to accept connection...")

with Client((domain, port)) as server:
    server.send("worker")
    logging.info("Connected to server")

    while True:
        server.send("waiting")

        logging.info("Waiting for server message...")
        message = server.recv()

        logging.info("Message received from server")
        if message == "terminate":
            break
        elif isinstance(message, list):
            task_result = []

            for file_path in message:
                try:
                    file_path = path.abspath(file_path)
                    logging.info(
                        f"Calculating MD5 hash for file: '{file_path}'")
                    generate_hash_file(file_path)
                    logging.info(f"MD5 hash file created: '{file_path}.md5'")
                    task_result.append("done")
                except FileNotFoundError:
                    logging.error(f"Invalid file path: '{file_path}'")
                    task_result.append("not found")
                except TypeError:
                    logging.error(
                        f"Invalid path type. Expected 'str', got '{type(file_path).__name__}'")
                    task_result.append("invalid type")

            server.send(task_result)
            logging.info("Sent task results to server")
        else:
            server.send("invalid message")
            logging.error("Message was invalid")

    server.send("terminated")
    logging.info("Terminating worker...")
