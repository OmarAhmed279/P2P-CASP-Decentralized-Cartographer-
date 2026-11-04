import hashlib
import socket
import pickle
import struct
from enum import IntEnum

class Server:
    HEADER_SIZE = 6  # temp
    HEADER_FORMAT = '!BBHH'  # 6 bytes #TODO we need to add the rest of the headers and the payload
    SERVER_PORT = 8000
    peers_list = []

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
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server_socket.bind(('', self.SERVER_PORT))
        print(f"SERVER: Listening on port {self.SERVER_PORT}.")
        while True: #TODO: we need to move this main loop into its own function
            message, address = server_socket.recvfrom(1200)

            if len(message) < self.HEADER_SIZE:
                continue

            protocol_id, version, msg_type, sequence_number, payload_length = self.unpack_header(message)
            print("SERVER: received a message from client!")

            if msg_type == self.MessageType.JOIN:
                print("SERVER: peer has joined the server!")
                if address not in self.peers_list:
                    self.peers_list.append(address)
                    msg = pickle.dumps(self.peers_list)
                    header = self.pack_header(1, 1, self.MessageType.PEER_LIST, 1,
                                         0)  # TODO: bad header, need to actually define an id, version, sequence and payload length
                    server_socket.sendto((header + msg), address)
                    print("SERVER: peers list was sent!")

    def pack_header(self, protocol_id, version, msg_type, sequence_number, payload_length):  # TODO make it pack / unpack not exclusive to headers
        version_message_type = (version << 4) + msg_type
        header_bytes = struct.pack(self.HEADER_FORMAT, protocol_id, version_message_type, sequence_number, payload_length)
        return header_bytes

    def unpack_header(self, raw_bytes):
        protocol_id, version_message_type, sequence_number, payload_length = struct.unpack_from(self.HEADER_FORMAT, raw_bytes)  # unpackFROM doesnt give error and unpack according to the format without it needing to match the size
        version = version_message_type >> 4
        message_type = version_message_type & 0xF  # both are 4 bits so this code to get them out the 8 bit char the share
        return protocol_id, version, message_type, sequence_number, payload_length

def generate_synthetic_payload(size_bytes: int, chunk_size: int = 1024, seed: int = 42) -> bytes:
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