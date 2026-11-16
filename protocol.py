import struct
PROTO_ID = 0xBC
CHUNK_SIZE = 64
NUM_CHUNKS = 256

HEADER_FORMAT = '!BBHHHH'  # 6 bytes #TODO we need to add the rest of the headers and the payload


def pack_header(self, version, msg_type, sequence_number,Peer_ID, payload_length,Timestamp , protocol_id = PROTO_ID ):  # TODO make it pack / unpack not exclusive to headers
    version_message_type = (version << 4) + msg_type
    header_bytes = struct.pack(self.HEADER_FORMAT, protocol_id, version_message_type, sequence_number, Peer_ID, payload_length,Timestamp )
    return header_bytes

def unpack_header(self, raw_bytes):
    protocol_id, version_message_type, sequence_number,Peer_ID, payload_length,Timestamp  = struct.unpack_from(self.HEADER_FORMAT, raw_bytes)  # unpackFROM doesnt give error and unpack according to the format without it needing to match the size
    version = version_message_type >> 4
    message_type = version_message_type & 0xF  # both are 4 bits so this code to get them out the 8 bit char the share
    return protocol_id, version_message_type, sequence_number,Peer_ID, payload_length,Timestamp 

def has(self ,bm , chunk):
    pass
    
    
def set_bit(self , bm , chunk):
    pass