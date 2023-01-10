#!/usr/bin/env python3

from multiprocessing.connection import Connection, Listener
from pathlib import Path
from subprocess import Popen, DEVNULL
from threading import Thread, Lock, Condition
import logging

domain = '127.0.0.1'
port = 8081

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s: [%(name)s] [%(levelname)s] %(message)s"
)

main_logger = logging.getLogger("Server")

workers = set()
commanders = set()
file_paths_queue = set()

workers_lock = Lock()
commanders_lock = Lock()
file_paths_queue_cv = Condition()


def file_enqueue(file_path: str) -> None:
    with file_paths_queue_cv:
        file_paths_queue.add(file_path)
        file_paths_queue_cv.notify(1)


def file_dequeue(max_count: int) -> list[str]:
    paths = []

    with file_paths_queue_cv:
        count = min(max_count, len(file_paths_queue))
        if count == 0:
            # Wait until new item is available.
            file_paths_queue_cv.wait_for(lambda: len(file_paths_queue) > 0)
            paths.append(file_paths_queue.pop())
        else:
            while count > 0:
                paths.append(file_paths_queue.pop())
                count -= 1

    return paths


def start_and_keep_worker(id: int) -> None:
    wt_logger = logging.getLogger(f"Worker keep-alive thread#{id}")
    wt_logger.info("Thread started")
    while True:
        wt_logger.info("Starting new worker...")
        Popen(args=["./worker.py"], stdout=DEVNULL, stderr=DEVNULL).wait()
        wt_logger.warning("Worker terminated")


def add_worker(worker: Connection) -> None:
    workers_lock.acquire()
    workers.add(worker)
    workers_lock.release()


def remove_worker(worker: Connection) -> None:
    workers_lock.acquire()
    workers.remove(worker)
    workers_lock.release()


def worker_handler(worker: Connection, address: tuple[str, int]) -> None:
    worker_logger = logging.getLogger(f"Worker@{address[0]}:{address[1]}")
    worker_logger.info("Worker thread started")

    try:
        with worker:
            while True:
                while worker.recv() != "waiting":
                    pass

                paths = file_dequeue(5)
                worker.send(paths)
                sent_count = len(paths)
                worker_logger.info(
                    f"Sent {sent_count} path{'s'[:sent_count ^ 1]} to worker: {paths}"
                )

                results = worker.recv()

                if results == "invalid message":
                    worker_logger.error(
                        "Worker reported invalid message. Terminating worker..."
                    )
                    worker.send('terminate')
                    break
                else:
                    for i in range(sent_count):
                        if results[i] == "done":
                            worker_logger.info(
                                f"Hash file created for '{paths[i]}'"
                            )
                        else:
                            worker_logger.warning(
                                f"Worker did not create hash file for '{paths[i]}' and returned this error: '{results[i]}'"
                            )
    except EOFError:
        worker_logger.error("Lost connection to worker")

    remove_worker(worker)
    worker_logger.info("Worker disconnected")


def add_commander(commander: Connection) -> None:
    commanders_lock.acquire()
    commanders.add(commander)
    commanders_lock.release()


def remove_commander(commander: Connection) -> None:
    commanders_lock.acquire()
    commanders.remove(commander)
    commanders_lock.release()


def commander_handler(commander: Connection, address: tuple[str, int]) -> None:
    commander_logger = logging.getLogger(
        f"Commander@{address[0]}:{address[1]}")
    commander_logger.info("Commander thread started")

    try:
        with commander:
            while True:
                while commander.recv() != "waiting":
                    pass

                commander.send("get path")
                commander_logger.info(
                    "Requested commander to send list of file paths"
                )

                result = commander.recv()
                if result == "invalid message":
                    commander_logger.error(
                        "Commander reported invalid message. Terminating Commander..."
                    )
                    commander.send('terminate')
                    break
                else:
                    file_enqueue(result)
                    commander_logger.info(f"Added '{result}' to files queue")
    except EOFError:
        commander_logger.error("Lost connection to commander")

    remove_commander(commander)
    commander_logger.info("Commander disconnected")


main_logger.info(f"Starting server at {domain}:{port}...")

with Listener((domain, port)) as listener:
    main_logger.info(
        "Server started. Accepting commander and worker connections..."
    )

    for i in range(5):
        Thread(target=start_and_keep_worker, args=(i + 1, )).start()

    while True:
        client = listener.accept()
        addr = listener.last_accepted
        main_logger.info(f"New connection from {addr[0]}:{addr[1]}")
        match(client.recv()):
            case "worker":
                add_worker(client)
                Thread(target=worker_handler, args=(client, addr)).start()
            case "commander":
                add_commander(client)
                Thread(target=commander_handler, args=(client, addr)).start()
            case other:
                main_logger.warning(
                    f"Invalid client@{addr[0]}:{addr[1]}. Closing connection..."
                )
                client.close()
