import struct
import time

PROTO_ID = 0x67
CHUNK_SIZE = 64
NUM_CHUNKS = 256
HEADER_SIZE = 12
HEADER_FORMAT = '!BBHHHI'


def pack_header(version, msg_type, sequence_number, peer_id, payload_length, timestamp = time.time_ns(), protocol_id = PROTO_ID ):  # TODO make it pack / unpack not exclusive to headers
    version_message_type = (version << 4) + msg_type
    header_bytes = struct.pack(HEADER_FORMAT, protocol_id, version_message_type, sequence_number, peer_id, payload_length, timestamp)
    return header_bytes

def unpack_header(raw_bytes):
    protocol_id, version_message_type, sequence_number, peer_id, payload_length, timestamp  = struct.unpack_from(HEADER_FORMAT, raw_bytes)  # unpackFROM doesnt give error and unpack according to the format without it needing to match the size
    version = version_message_type >> 4
    message_type = version_message_type & 0xF  # both are 4 bits so this code to get them out the 8 bit char the share
    return protocol_id, version, message_type, sequence_number, peer_id, payload_length, timestamp

def has(bm , chunk):
    pass
    
    
def set_bit(bm , chunk):
    pass