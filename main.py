import server
import client
import time


def main():
    main_server = server.Server(8000)
    server_thread = main_server.launch_startup_thread()

    clients = [
        client.Client('127.0.0.1', 8000),
        client.Client('127.0.0.1', 8000),
        client.Client('127.0.0.1', 8000),
        client.Client('127.0.0.1', 8000),
    ]

    print("Starting clients...")
    time.sleep(5)
    start_time = time.time()

    threads = [c.launch_startup_thread() for c in clients]

    for t in threads:
        t.join()

    end_time = time.time()
    server_thread.join()


    total_time = end_time - start_time

    print(f"All clients finished! Total time taken: {total_time:.4f} seconds.")
if __name__ == "__main__":
    main()