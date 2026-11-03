import hashlib    
import socket
import pickle
import struct

from enum import IntEnum

class Message_type(IntEnum):
    JOIN =0
    PEER_LIST=1
    PEER_HEARTBEAT=2
    CHUNK_BITMAP=3
    CHUNK_REQ=4
    CHUNK_DATA=5
    MAP_COMPLETE=6
    COMPLETE_ACK=7
    LEAVE=8
    ERROR=9

#HEADER_SIZE =12
HEADER_FORMAT = '!BBHH'   #'!BBBHHH' #9 bytes #TODO we need to add the rest of the headers and the payload
SERVER_PORT = 8000
peers_list = []

#TODO: actually make a class and make this humane

def start_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.bind(('', SERVER_PORT))
    print(f"Listening on port {SERVER_PORT}.")
    while True:
            
        message, address = server_socket.recvfrom(1200)
            
        if len(message) < HEADER_SIZE:
            continue
        protocol_id, version,message_type, sequence_number, payload_length = unpack_header(message) #TODO: currently doesn't care about the header
        
            
        if message_type == Message_type.JOIN  :
            if address not in peers_list:
            
                peers_list.append(address)
                msg = pickle.dumps(peers_list)
                header = pack_header(1,1,Message_type.PEER_LIST ,1,1)
                server_socket.sendto(header+msg, address)
        #join both arrays and send them



def pack_header(protocol_id, version, message_type, sequence_number, payload_length): #TODO make it pack / unpack not exclusive to headers
    version_message_type=(version<<4) +message_type 
    header_bytes = struct.pack(HEADER_FORMAT, protocol_id, version_message_type, sequence_number, payload_length)
    return header_bytes

def unpack_header(raw_bytes):
    protocol_id, version_message_type, sequence_number, payload_length = struct.unpack_from(HEADER_FORMAT, raw_bytes ) # unpackFROM doesnt give error and unpack according to the format without it needing to match the size
    
    version = version_message_type >> 4
    message_type= version_message_type & 0xF #both are 4 bits so this code to get them out the 8 bit char the share
    
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
        # Deterministically derive the next block using SHA-256
        hasher = hashlib.sha256(state.to_bytes(32, byteorder='big'))
        chunk_pool = hasher.digest() #123
        # Expand chunk_pool to match requested chunk_size if needed
        while len(chunk_pool) < chunk_size:
            hasher = hashlib.sha256(chunk_pool)
            chunk_pool += hasher.digest()
        # Slice the needed chunk size or the remaining bytes
        bytes_to_add = min(chunk_size, size_bytes - generated)
        payload.extend(chunk_pool[:bytes_to_add])
        generated += bytes_to_add
        state += 1  # Advance state deterministically
    return bytes(payload)

if __name__ == "__main__":
    start_server()