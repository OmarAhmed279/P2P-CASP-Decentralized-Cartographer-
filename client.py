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
    id = -1
    bitmap = 0b0
    peers_list: dict[int, pd] = {} #key = id, value = peer_details
    rtt_list: list[pd] = []
    chunks = []
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
            msg, address = client_socket.recvfrom(65535)
            print(f"CLIENT: received a message from server!")

            protocol_id, version, message_type, sequence_number, peer_id, payload_length, timestamp = protocol.unpack_header(msg)
            if message_type == self.MessageType.PEER_LIST:

                self.peers_list = pickle.loads(msg[protocol.HEADER_SIZE:])
                self.id = max(self.peers_list.keys())
                print(f"CLIENT: received a peer list!\n{self.peers_list}")
                break
            else:
                continue

        req_thread = threading.Thread(target=self.request, args=(client_socket,))
        rtt_thread = threading.Thread(target=self.rtt_measurement, args=(client_socket,))
        bitmap_adv_thread = threading.Thread(target=self.bitmap_adv, args=(client_socket,))
        listen_thread = threading.Thread(target=self.listen, args=(client_socket,))
        rtt_thread.start()
        bitmap_adv_thread.start()
        listen_thread.start()
        req_thread.start()

    def request(self, client_socket): #TODO: finish this, and map complete
        return

    def rtt_measurement(self, client_socket):
        while True:
            time.sleep(10)
            for peer in self.peers_list.values():
                send_time = int(time.time())
                head = protocol.pack_header(1, self.MessageType.PEER_HEARTBEAT, 0, self.id, 32, send_time)
                client_socket.sendto(head, peer.address)

                msg, address = client_socket.recvfrom(1200)
                protocol_id, version, message_type, sequence_number, peer_id, payload_length, timestamp = protocol.unpack_header(msg)
                if address == peer.address and message_type == self.MessageType.HEARTBEAT_RESPONSE:
                    receive_time = int(time.time())
                    rtt = receive_time - send_time
                    peer.latest_rtt = rtt

            self.rtt_list = sorted(self.peers_list.values(), key=lambda x: x.latest_rtt)[:4]

    def bitmap_adv(self, client_socket):
        while True:
            time.sleep(5)
            for peer in self.peers_list.values():
                head = protocol.pack_header(1, self.MessageType.CHUNK_BITMAP, 0, self.id, 32, int(int(time.time())))
                msg = pickle.dumps(self.bitmap)
                client_socket.sendto((head + msg), peer.address)

    def listen(self, client_socket):
        while True:
            msg, address = client_socket.recvfrom(1200)

            protocol_id, version, message_type, sequence_number, peer_id, payload_length, timestamp = protocol.unpack_header(msg)

            match message_type:
                case self.MessageType.CHUNK_BITMAP:
                    bitmap = pickle.loads(msg[protocol.HEADER_SIZE:])
                    self.peers_list[peer_id].latest_bitmap = bitmap

                case self.MessageType.PEER_HEARTBEAT:
                    header = protocol.pack_header(1, self.MessageType.HEARTBEAT_RESPONSE, 0, self.id, 0, int(time.time()))
                    client_socket.sendto(header, address)

                case self.MessageType.CHUNK_DATA:
                    chunk = pickle.loads(msg[protocol.HEADER_SIZE:])
                    self.chunks.append(chunk)

                case self.MessageType.PEER_LIST:
                    peer = pickle.loads(msg[protocol.HEADER_SIZE:])
                    self.peers_list[peer.id] = peer

                case self.MessageType.CHUNK_REQ: #TODO: finish this
                    continue
                    #sending-> responding to req
if __name__ == "__main__":
    client = Client('127.0.0.1', 8000)
    client.start_client()