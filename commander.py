#!/usr/bin/env python3

import hashlib
from multiprocessing.connection import Client
import logging
from pathlib import Path

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
    logging.info("Connected to server.")

    while True:
        server.send("waiting")

        logging.info("Waiting for server to ask for file paths...")
        message = server.recv()

        logging.info("Message received from server.")
        if message == "terminate":
            break
        elif message == "get path list":
            files = []

            paths = Path("test_data").rglob()
            for file in paths:
                if file.is_file and file.suffix != '.md5':
                    files.append(str(file.absolute()))

            server.send(tuple(files))
            logging.info('Sent list of file paths to the server.')
        else:
            server.send("invalid message")
            logging.error('Message was invalid.')

    server.send("terminated")
    logging.info("Terminating commander...")
