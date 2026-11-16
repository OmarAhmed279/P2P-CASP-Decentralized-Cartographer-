import hashlib
import socket
import pickle
import struct
from collections import Counter
from enum import IntEnum
import threading
import protocol 

class Server:
    #HEADER_SIZE = 6  # temp
    #HEADER_FORMAT = '!BBHH'  # 6 bytes #TODO we need to add the rest of the headers and the payload
    SERVER_PORT = 8000
    peers_list = []
    peers_bitmaps = {}
    payload = 0

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
        main_thread = threading.Thread(target=self.mainloop(server_socket))
        main_thread.start()
        seeding_thread = threading.Thread(target=self.seedloop(server_socket))
        seeding_thread.start()

    def seedloop(self, server_socket):
        while True:
            if len(self.peers_list) > 0:
                poorest_client = min(self.peers_bitmaps, key=lambda k: self.peers_bitmaps[k].bit_count()) #lets assume for now the bitmap is a string
                #poorest client should be a tuple (ip, port)
                bit_counts = Counter()
                max_bit_length = max(b.bit_length() for b in self.peers_bitmaps)

                # Count frequencies at each position
                for num in self.peers_bitmaps:
                    for pos in range(max_bit_length):
                        if (num >> pos) & 1:
                            bit_counts[pos] += 1

                # Find the position with the lowest count (ignoring completely unused positions)
                rarest_chunk = min(bit_counts, key=bit_counts.get)
                msg = self.payload[rarest_chunk]
                header = self.pack_header(1, 1, self.MessageType.CHUNK_DATA, 1,0)
                server_socket.sendto((header + msg), poorest_client)






    def mainloop(self, server_socket):
        while True:
            message, address = server_socket.recvfrom(1200)

            if len(message) < self.HEADER_SIZE:
                continue

            protocol_id, version, msg_type, sequence_number, payload_length = self.unpack_header(message)
            print("SERVER: received a message from client!")

            match msg_type:
                case self.MessageType.JOIN:
                    print("SERVER: peer has joined the server!")
                    if address not in self.peers_list:
                        self.peers_list.append(address)
                        msg = pickle.dumps(self.peers_list)
                        header = self.pack_header(1, 1, self.MessageType.PEER_LIST, 1,
                                            0)  # TODO: bad header, need to actually define an id, version, sequence and payload length
                        server_socket.sendto((header + msg), address)
                        print("SERVER: peers list was sent!")

    # def pack_header(self, protocol_id, version, msg_type, sequence_number, payload_length):  # TODO make it pack / unpack not exclusive to headers
    #     version_message_type = (version << 4) + msg_type
    #     header_bytes = struct.pack(self.HEADER_FORMAT, protocol_id, version_message_type, sequence_number, payload_length)
    #     return header_bytes

    # def unpack_header(self, raw_bytes):
    #     protocol_id, version_message_type, sequence_number, payload_length = struct.unpack_from(self.HEADER_FORMAT, raw_bytes)  # unpackFROM doesnt give error and unpack according to the format without it needing to match the size
    #     version = version_message_type >> 4
    #     message_type = version_message_type & 0xF  # both are 4 bits so this code to get them out the 8 bit char the share
    #     return protocol_id, version, message_type, sequence_number, payload_length

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