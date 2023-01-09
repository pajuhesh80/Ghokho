#!/usr/bin/env python3

from multiprocessing.connection import Connection, Listener
from pathlib import Path
from threading import Thread, Lock
import logging

domain = '127.0.0.1'
port = 8081

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s: [%(name)s] [%(levelname)s] %(message)s"
)

main_logger = logging.getLogger(name="Server")

workers = set()
commanders = set()
file_paths_queue = []

file_paths_queue_lock = Lock()
rm_worker_lock = Lock()

paths = Path("test_data").rglob('*')
for file in paths:
    if file.is_file and file.suffix != '.md5':
        file_paths_queue.append(str(file.absolute()))


def file_enqueue(file_path: str) -> None:
    file_paths_queue_lock.acquire()
    file_paths_queue.insert(0, file_path)
    file_paths_queue_lock.release()


def file_dequeue(max_count: int) -> list[str]:
    paths = []

    file_paths_queue_lock.acquire()
    count = min(max_count, len(file_paths_queue))
    if count == 0:
        # Let the main thread add items to file_paths.
        file_paths_queue_lock.release()
        # Wait until new item is available.
        while True:
            while not file_paths_queue_lock.locked():
                # Wait for another thread to take lock.
                pass
            file_paths_queue_lock.acquire()
            if len(file_paths_queue_lock) > 0:
                # New items added.
                break
            else:
                # No new items. Probably another worker thread took the lock.
                file_paths_queue_lock.release()
        paths.append(file_paths_queue.pop())
    else:
        while count > 0:
            paths.append(file_paths_queue.pop())
            count -= 1

    file_paths_queue_lock.release()

    return paths


def remove_worker(worker: Connection) -> None:
    rm_worker_lock.acquire()
    workers.remove(worker)
    rm_worker_lock.release()


def worker_handler(worker: Connection, address: tuple[str, int]) -> None:
    worker_logger = logging.getLogger(
        name=f"Worker@{address[0]}:{address[1]}")
    worker_logger.info("Worker thread started")

    while not worker.closed:
        while worker.recv() != "waiting":
            pass

        paths = file_dequeue(5)
        worker.send(paths)
        sent_count = len(paths)
        worker_logger.info(f"Sent {sent_count} paths to worker")

        results = worker.recv()

        if results == "invalid message":
            worker_logger.error(
                "Worker reported invalid message. Terminating worker...")
            worker.send('terminate')
            worker.close()
        else:
            for i in range(sent_count):
                if results[i] == "done":
                    worker_logger.info(f"Hash file created for '{paths[i]}'")
                else:
                    worker_logger.warning(
                        f"Worker did not create hash file for '{paths[i]}' and returned this error: '{results[i]}'")

    remove_worker(worker)
    worker_logger.info("Worker disconnected")


main_logger.info(f"Starting server at {domain}:{port}...")

with Listener((domain, port)) as listener:
    main_logger.info(
        "Server started. Accepting commander and worker connections...")

    while True:
        client = listener.accept()
        addr = listener.last_accepted
        main_logger.info(f"New connection from {addr[0]}:{addr[1]}")
        if client.recv() == 'worker':
            workers.add(client)
            worker_thread = Thread(
                target=worker_handler, args=(client, addr))
            worker_thread.start()
