import hashlib
import socket
import pickle
import time
from collections import Counter, defaultdict
import threading
import protocol
from peer_details import peer_details as pd

class Server: #TODO: check overall file again
    SERVER_PORT = 8000
    peers_list: dict[int, pd] = {}
    payload = 0
    id_counter = 0
    sequence_number = 0
    shutdown_event = threading.Event()

    def __init__(self, port: int):
        self.SERVER_PORT = port

    def launch_startup_thread(self):
        startup_thread = threading.Thread(target=self.start_server)
        startup_thread.start()
        return startup_thread

    def start_server(self):
        self.payload = generate_synthetic_payload(protocol.NUM_CHUNKS * protocol.CHUNK_SIZE) #TODO: move this to before server starts, or save it and reuse
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server_socket.bind(('', self.SERVER_PORT))
        server_socket.settimeout(0.5)
        print(f"SERVER: Listening on port {self.SERVER_PORT}. {(time.ctime(time.time()))}")
        main_thread = threading.Thread(target=self.mainloop, args=(server_socket,))
        seeding_thread = threading.Thread(target=self.seedloop, args=(server_socket,))
        main_thread.start()
        seeding_thread.start()
        main_thread.join()
        seeding_thread.join()
        server_socket.close()

    def seedloop(self, server_socket): #TODO: calculate upload speed and update it automatically
        sent_chunks = defaultdict(list)
        active_peers = []

        while not self.shutdown_event.wait(0.01024):
            iteration_time = time.time_ns()
            peers = list(self.peers_list.values())

            if len(self.peers_list) > 0:
                active_peers = [peer for peer in peers if peer.done == False]

            if len(self.peers_list) > 0 and not active_peers:
                print("SERVER: All clients finished! Shutting down server...")
                self.shutdown_event.set()
                break

            if not active_peers:
                continue

            active_ids = [peer.id for peer in active_peers]
            print(f"SERVER: current ACTIVE PEERS: {active_ids}")

            poorest_client = min(
                active_peers,
                key=lambda peer: peer.latest_bitmap.bit_count()
            )

            bit_counts = Counter()

            for peer in peers:
                for pos in range(protocol.NUM_CHUNKS):
                    if (peer.latest_bitmap >> pos) & 1:
                        bit_counts[pos] += 1

            sorted_chunks = [chunk for chunk in sorted(range(protocol.NUM_CHUNKS), key=lambda x: bit_counts[x]) if chunk not in sent_chunks[poorest_client.id]]

            if not sorted_chunks:
                sent_chunks[poorest_client.id].clear()
                continue

            target = None
            for peer in sorted(active_peers, key=lambda p: p.latest_bitmap.bit_count()):
                for chunk in sorted_chunks:
                    if not (peer.latest_bitmap >> chunk) & 1 and chunk not in sent_chunks[peer.id]:
                        target = (peer, chunk)
                        break
                if target:
                    break

            if target is None:
                continue

            peer, chunk = target
            start = chunk * protocol.CHUNK_SIZE
            msg = pickle.dumps((chunk, self.payload[start:start + protocol.CHUNK_SIZE]))
            header = protocol.pack_header(1, protocol.MessageType.CHUNK_DATA, 0, 0, len(msg))
            server_socket.sendto(header + msg, peer.address)
            sent_chunks[peer.id].append(chunk)
            print(f"SERVER: sent chunk {chunk} to poorest client {peer.id}")
            print(f"{time.time_ns() - iteration_time} milliseconds elapsed")

    def mainloop(self, server_socket):
        while not self.shutdown_event.is_set():
            try:
                message, address = server_socket.recvfrom(1200)
            except socket.timeout:
                continue

            if len(message) < protocol.HEADER_SIZE:
                continue

            protocol_id, version, message_type, sequence_number, peer_id, payload_length, timestamp = protocol.unpack_header(message)
            print("SERVER: received a message from client!")

            match message_type:
                case protocol.MessageType.JOIN:
                    print("SERVER: peer has joined the server!")
                    if not any(peer.address == address for peer in self.peers_list.values()):
                        self.id_counter = self.id_counter + 1
                        new_peer = pd(address[0], address[1], id=self.id_counter)
                        msg = pickle.dumps(new_peer)
                        header = protocol.pack_header( 1, protocol.MessageType.PEER_LIST, sequence_number, 0, len(msg))
                        for peer in self.peers_list.values():
                            server_socket.sendto((header + msg), peer.address)

                        self.peers_list[self.id_counter] = new_peer
                        msg = pickle.dumps(self.peers_list)

                        server_socket.sendto((header + msg), address)
                        print("SERVER: peers list was sent!")
                        
                case protocol.MessageType.MAP_COMPLETE:
                    print(f"SERVER: Client {peer_id} has completed the map!") #TODO: verify that map is actually done
                    self.peers_list[peer_id].done = True
                    for peer in self.peers_list.values():
                        if not self.peers_list[peer.id].done:
                            msg = pickle.dumps(self.peers_list[peer_id])
                            header = protocol.pack_header(1, protocol.MessageType.PEER_LIST, sequence_number, 0, len(msg))
                            server_socket.sendto(header + msg, peer.address)

                    header = protocol.pack_header(1, protocol.MessageType.COMPLETE_ACK, sequence_number, 0, 0)
                    server_socket.sendto(header, address)
                    
                case protocol.MessageType.CHUNK_BITMAP:
                    bitmap = pickle.loads(message[protocol.HEADER_SIZE:])
                    if peer_id in self.peers_list:
                        self.peers_list[peer_id].latest_bitmap = bitmap


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