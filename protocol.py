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

def pack_packet(
    version,
    msg_type,
    sequence_number,
    peer_id,
    payload=b'',
    timestamp=time.time_ns(),
    protocol_id=PROTO_ID
):
    """Pack a complete packet containing a header and payload."""

    if not isinstance(payload, bytes):
        raise TypeError("Payload must be bytes")

    header = pack_header(
        version,
        msg_type,
        sequence_number,
        peer_id,
        len(payload),
        timestamp,
        protocol_id
    )

    return header + payload


def unpack_packet(raw_bytes):
    """Unpack a complete packet into its header and payload."""

    if len(raw_bytes) < HEADER_SIZE:
        raise ValueError("Packet is smaller than the header")

    header = unpack_header(raw_bytes)

    payload_length = header[4]
    payload = raw_bytes[HEADER_SIZE:]

    if len(payload) != payload_length:
        raise ValueError(
            f"Payload length mismatch: expected {payload_length}, "
            f"received {len(payload)}"
        )

    return header, payload

def _validate_chunk_id(chunk: int) -> None:
    if not isinstance(chunk, int) or isinstance(chunk, bool):
        raise TypeError("chunk must be an integer")
    if not 0 <= chunk < NUM_CHUNKS:
        raise ValueError("Invalid chunk ID")


def has(bm: int, chunk: int) -> bool:
    _validate_chunk_id(chunk)
    return bool((bm >> chunk) & 1)


def set_bit(bm: int, chunk: int) -> int:
    _validate_chunk_id(chunk)
    return bm | (1 << chunk)