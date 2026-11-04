import server
import client
import threading

def launch_server():
    main_server = server.Server(8000)
    main_server.start_server()

def main():
    server_thread = threading.Thread(target=launch_server)
    server_thread.daemon = True
    server_thread.start()

    client_one = client.Client('127.0.0.1', 8000)
    client_one.start_client()

if __name__ == "__main__":
    main()