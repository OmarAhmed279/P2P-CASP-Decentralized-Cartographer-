import socket
import pickle
import threading
from enum import IntEnum
import protocol
from peer_details import peer_details as pd
import time


class Client:
    SERVER_IP = '127.0.0.1'
    SERVER_PORT = 8000
    peers_list: list[pd] = []
    chunks= {}
    def __init__(self, ip: str, port: int):
        self.SERVER_IP = ip
        self.SERVER_PORT = port

    class MessageType(IntEnum):
        JOIN = 0
        PEER_LIST = 1
        PEER_HEARTBEAT = 2
        CHUNK_BITMAP = 3
        CHUNK_REQ = 4
        CHUNK_DATA = 5
        MAP_COMPLETE = 6
        HEARTBEAT_RESPONSE = 7
        LEAVE = 8
        ERROR = 9

    def start_client(self):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        msg = protocol.pack_header(1, self.MessageType.JOIN, 0, 0, 0, 0)
        client_socket.sendto(msg, (self.SERVER_IP, self.SERVER_PORT))

        print(f"CLIENT: Client sent JOIN message to SERVER:({self.SERVER_IP}:{self.SERVER_PORT})")

        while True:
            msg, address = client_socket.recvfrom(1200)
            print(f"CLIENT: received a message from server!")

            protocol_id, version, message_type, sequence_number, peer_id, payload_length, timestamp = protocol.unpack_header(msg)
            if message_type == self.MessageType.PEER_LIST:
                self.peers_list = pickle.loads(msg[protocol.HEADER_SIZE:])
                break
            else:
                continue

        main_thread = threading.Thread(target=self.mainloop, args=(client_socket,))
        rtt_thread = threading.Thread(target=self.rtt_measurment, args=(client_socket,))
        rtt_thread.start()
        main_thread.start()

    def rtt_measurment(self, client_socket):
        while True:
            time.sleep(30)
            for peer in self.peers_list:
                send_time = time.time_ns()
                head = protocol.pack_header(1, self.MessageType.PEER_HEARTBEAT, 0, peer.id, 0, send_time)
                client_socket.sendto(head, peer.address)

                msg, address = client_socket.recvfrom(1200)
                protocol_id, version, message_type, sequence_number, peer_id, payload_length, timestamp = protocol.unpack_header(msg)
                if address == peer.address and message_type == self.MessageType.HEARTBEAT_RESPONSE:
                    receive_time = time.time_ns()
                    rtt = receive_time - send_time
                    peer.latest_rtt = rtt

if __name__ == "__main__":
    client = Client('127.0.0.1', 8000)
    client.start_client()