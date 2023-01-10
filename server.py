#!/usr/bin/env python3

import logging

from multiprocessing.connection import Connection, Listener
from pathlib import Path
from subprocess import Popen, DEVNULL
from threading import Thread, Lock, Condition


domain = '127.0.0.1'
port = 8081

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s: [%(name)s] [%(levelname)s] %(message)s"
)

main_logger = logging.getLogger("Server")

worker_pid: dict[Connection, int] = {}
worker_proc: list[Popen] = []
commanders = set()
file_paths_queue = set()
last_hashed_by: dict[str, Connection] = {}
penalties: dict[Connection, int] = {}

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


def add_worker_proc(process: Popen) -> None:
    workers_lock.acquire()
    worker_proc.append(process)
    workers_lock.release()


def start_and_keep_worker(id: int) -> None:
    wt_logger = logging.getLogger(f"Worker keep-alive thread#{id}")
    wt_logger.info("Thread started")
    while True:
        wt_logger.info("Starting new worker...")
        proc = Popen(args=["./worker.py"], stdout=DEVNULL, stderr=DEVNULL)
        add_worker_proc(proc)
        proc.wait()
        wt_logger.warning("Worker terminated")


def add_worker(worker: Connection, pid: int) -> None:
    workers_lock.acquire()
    worker_pid[worker] = pid
    workers_lock.release()


def remove_worker(worker: Connection) -> None:
    workers_lock.acquire()
    pid = worker_pid[worker]
    worker.close()
    for proc in worker_proc:
        if proc.pid == pid:
            proc.kill()
            break
    worker_pid.pop(worker)
    penalties.pop(worker)
    workers_lock.release()


def worker_handler(worker: Connection, address: tuple[str, int]) -> None:
    worker_logger = logging.getLogger(f"Worker@{address[0]}:{address[1]}")
    worker_logger.info("Worker thread started")
    penalties[worker] = 0
    paths = []

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
                            last_hashed_by[paths[i]] = worker
                        else:
                            worker_logger.warning(
                                f"Worker did not create hash file for '{paths[i]}' and returned this error: '{results[i]}'"
                            )
                    paths.clear()
    except (EOFError, OSError):
        worker_logger.error("Lost connection to worker")
    finally:
        # Some files were going to be processed but worker has been terminated. Recover them.
        for path in paths:
            file_enqueue(path)

    # Wait if remove_worker is running
    workers_lock.acquire()

    if worker in worker_pid:
        workers_lock.release()
        # Worked terminated by itself. Cleanup...
        remove_worker(worker)
    else:
        workers_lock.release()
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

                result:str = commander.recv()
                if result == "invalid message":
                    commander_logger.error(
                        "Commander reported invalid message. Terminating Commander..."
                    )
                    commander.send('terminate')
                    break
                elif result.startswith("ERR "):
                    file_path = result[4:]
                    commander_logger.warning(f"Reported bad hash for '{file_path}'")

                    bad_worker = last_hashed_by[file_path]
                    if bad_worker in penalties:
                        penalties[bad_worker] += 1

                        if penalties[bad_worker] > 2:
                            pid = worker_pid[bad_worker]
                            commander_logger.warning(
                                f"Worker with pid '{pid}' exceeded allowed number of penalties. Terminating it..."
                            )
                            remove_worker(bad_worker)
                else:
                    file_enqueue(result)
                    commander_logger.info(f"Added '{result}' to files queue")
    except (EOFError, OSError):
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
                pid = client.recv()
                add_worker(client, pid)
                Thread(target=worker_handler, args=(client, addr)).start()
            case "commander":
                add_commander(client)
                Thread(target=commander_handler, args=(client, addr)).start()
            case other:
                main_logger.warning(
                    f"Invalid client@{addr[0]}:{addr[1]}. Closing connection..."
                )
                client.close()
