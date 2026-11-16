import server
import client

def main():
    main_server = server.Server(8000)
    main_server.start_server()

    client_one = client.Client('127.0.0.1', 8000)
    client_one.start_client()
    client_two = client.Client('127.0.0.1', 8000)
    client_two.start_client()
    client_three = client.Client('127.0.0.1', 8000)
    client_three.start_client()
    client_four = client.Client('127.0.0.1', 8000)
    client_four.start_client()

if __name__ == "__main__":
    main()