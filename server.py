import hashlib
import socket
import pickle
import struct
import time
from collections import Counter
from enum import IntEnum
import threading
import protocol
from peer_details import peer_details as pd

class Server:
    SERVER_PORT = 8000
    peers_list: list[pd] = []
    payload = 0
    id = 0
    sequence_number=0
    def __init__(self, port: int):
        self.SERVER_PORT = port

    class MessageType(IntEnum):
        JOIN = 0
        PEER_LIST = 1
        PEER_HEARTBEAT = 2
        CHUNK_BITMAP = 3
        CHUNK_REQ = 4
        CHUNK_DATA = 5
        MAP_COMPLETE = 6
        COMPLETE_ACK = 7
        LEAVE = 8
        ERROR = 9

    def start_server(self):
        self.payload = generate_synthetic_payload()
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server_socket.bind(('', self.SERVER_PORT))
        print(f"SERVER: Listening on port {self.SERVER_PORT}.")
        main_thread = threading.Thread(target=self.mainloop, args=(server_socket,))
        main_thread.start()
        seeding_thread = threading.Thread(target=self.seedloop, args=(server_socket,))
        seeding_thread.start()

    def seedloop(self, server_socket):
        while True:
            time.sleep(0.01024)
            if len(self.peers_list) > 0:
                poorest_client = min(self.peers_list, key=lambda peer: peer.latest_bitmap.bit_count())
                bit_counts = Counter()
                max_bit_length = max(peer.latest_bitmap.bit_length() for peer in self.peers_list)

                # Count frequencies at each position
                for peer in self.peers_list:
                    num = peer.latest_bitmap
                    for pos in range(max_bit_length):
                        if (num >> pos) & 1:
                            bit_counts[pos] += 1

                # Find the position with the lowest count (ignoring completely unused positions)
                rarest_chunk = min(bit_counts, key=bit_counts.get)

                msg = self.payload[rarest_chunk * 64: rarest_chunk * 64 + 64]
                header = protocol.pack_header(1, self.MessageType.CHUNK_DATA, 0, poorest_client.id,0, time.time_ns())
                server_socket.sendto((header + msg), poorest_client)

    def mainloop(self, server_socket):
        while True:
            message, address = server_socket.recvfrom(1200)

            if len(message) < protocol.HEADER_SIZE:
                continue

            protocol_id, version, msg_type, sequence_number, payload_length = protocol.unpack_header(message)
            print("SERVER: received a message from client!")

            match msg_type:
                case self.MessageType.JOIN:
                    print("SERVER: peer has joined the server!")
                    if address not in self.peers_list:
                        self.id = self.id + 1
                        self.peers_list.append(pd(self.id, address[0], address[1]))
                        msg = pickle.dumps(self.peers_list)
                        
                        header = protocol.pack_header(1, self.MessageType.PEER_LIST, sequence_number,0,1,0)  # TODO: bad header, need to actually define an id, version, sequence and payload length
                        sequence_number += 1
                        server_socket.sendto((header + msg), address)
                        print("SERVER: peers list was sent!")

def generate_synthetic_payload(size_bytes: int, chunk_size: int = 64, seed: int = 42) -> bytes:  #5 seconds limit of uploading to 4 peers and the 50kbps limit 
        """
        Generates a deterministic pseudo-random payload of a specific size.
        :param size_bytes: Total size of the payload to generate in bytes.
        :param chunk_size: Size of each generated chunk (default 1KB).
        :param seed: Integer seed to ensure determinism.
        """
        payload = bytearray()
        generated = 0
        state = seed

        while generated < size_bytes:
            hasher = hashlib.sha256(state.to_bytes(32, byteorder='big'))
            chunk_pool = hasher.digest()  # 123

            while len(chunk_pool) < chunk_size:
                hasher = hashlib.sha256(chunk_pool)
                chunk_pool += hasher.digest()

            bytes_to_add = min(chunk_size, size_bytes - generated)
            payload.extend(chunk_pool[:bytes_to_add])
            generated += bytes_to_add
            state += 1

        return bytes(payload)

if __name__ == "__main__":
    server = Server(Server.SERVER_PORT)
    server.start_server()