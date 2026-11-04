import hashlib
import socket
import pickle
import struct
from enum import IntEnum

from server import Server


class Client:
    HEADER_SIZE = 6  #temp
    HEADER_FORMAT = '!BBHH'  # 6 bytes #TODO we need to add the rest of the headers and the payload
    SERVER_IP = '127.0.0.1'
    SERVER_PORT = 8000
    peers_list = []

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
        COMPLETE_ACK = 7
        LEAVE = 8
        ERROR = 9

    def start_client(self):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        msg = self.pack_header(1, 1, Server.MessageType.JOIN, 0, 0)
        client_socket.sendto(msg, (self.SERVER_IP, self.SERVER_PORT))
        print(f"CLIENT: Client sent JOIN message to SERVER:({self.SERVER_IP}:{self.SERVER_PORT})")
        while True: #TODO: we need to move this main loop into its own function
            message, address = client_socket.recvfrom(1200)
            if len(message) < self.HEADER_SIZE:
                continue
            protocol_id, version, msg_type, sequence_number, payload_length = self.unpack_header(message) #TODO: currently doesn't care about the header
            print("CLIENT: received a message from server!")


    def pack_header(self, protocol_id, version, msg_type, sequence_number, payload_length):  # TODO make it pack / unpack not exclusive to headers
        version_message_type = (version << 4) + msg_type
        header_bytes = struct.pack(self.HEADER_FORMAT, protocol_id, version_message_type, sequence_number, payload_length)
        return header_bytes

    def unpack_header(self, raw_bytes):
        protocol_id, version_message_type, sequence_number, payload_length = struct.unpack_from(self.HEADER_FORMAT, raw_bytes)  # unpackFROM doesnt give error and unpack according to the format without it needing to match the size
        version = version_message_type >> 4
        message_type = version_message_type & 0xF  # both are 4 bits so this code to get them out the 8 bit char the share
        return protocol_id, version, message_type, sequence_number, payload_length

if __name__ == "__main__":
    client = Client('127.0.0.1', 8000)
    client.start_client()