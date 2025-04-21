import socket
import sys
import multiprocessing

import numpy as np


def read_config() -> list[tuple[str, int]]:
    nodes = []

    with open("config.txt", "r") as f:
        for line in f:
            line = line.strip()
            parts = line.split()

            ip = parts[0]
            port = int(parts[1])
            nodes.append((ip, port))

    return nodes


def send_matrix(n: int, rows: int, address: tuple[str, int]):
    matrix = np.random.randint(1, 101, size=(rows, n))

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(address)

    sock.sendall(f"{n}".encode())
    sock.sendall(matrix.tobytes())

    ack = sock.recv(1024).decode()
    print(ack)

    sock.close()


def master(n: int):
    nodes = read_config()
    t = len(nodes)

    rows_per_node = n // t
    remainder = n % t

    processes = []
    start_row = 0

    for i in range(t):
        node_rows = rows_per_node + (1 if i < remainder else 0)

        process = multiprocessing.Process(
            target=send_matrix, args=(n, node_rows, nodes[i])
        )
        processes.append(process)
        process.start()

        start_row += node_rows

    for process in processes:
        process.join()


def slave(p: int, s: int) -> None:
    print(f"Running as SLAVE node (ID: {s})")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("", p))
    sock.listen(1)

    conn: socket.socket
    conn, addr = sock.accept()

    n: int = int(conn.recv(1024).decode())

    buffer = b""
    while True:
        data: bytes = conn.recv(4096)
        if not data:
            break

        buffer += data

    matrix = np.frombuffer(buffer).reshape(-1, n)

    print(f"Received matrix shape: {matrix.shape}")
    conn.sendall(f"ack from {socket.gethostname()}, port {p}".encode())

    conn.close()
    sock.close()


def main() -> None:
    if len(sys.argv) == 2:
        n = int(sys.argv[1])
        master(n)
    else:
        p = int(sys.argv[1])
        s = int(sys.argv[2])
        slave(p, s)


if __name__ == "__main__":
    main()
