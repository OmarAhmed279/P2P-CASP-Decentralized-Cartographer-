import socket
import pickle
import threading

import protocol
from peer_details import peer_details as pd
import time

class Client: #TODO: check overall file again
    def __init__(self, ip: str, port: int):
        self.SERVER_IP = ip
        self.SERVER_PORT = port
        self.id = -1
        self.bitmap = 0
        self.peers_list: dict[int, pd] = {}
        self.rtt_list: list[pd] = []
        self.chunks = {}
        self.ping_sent = {}
        self.shutdown_event = threading.Event()

    def launch_startup_thread(self):
        startup_thread = threading.Thread(target=self.start_client)
        startup_thread.start()
        return startup_thread

    def start_client(self):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client_socket.settimeout(0.5)
        msg = protocol.pack_header(1, protocol.MessageType.JOIN, 0, 999, 0) #client hasn't been assigned an id yet so id = 999
        client_socket.sendto(msg, (self.SERVER_IP, self.SERVER_PORT))

        print(f"CLIENT ID = {self.id}: sent JOIN message to SERVER:({self.SERVER_IP}:{self.SERVER_PORT})! ({time.ctime(time.time())})\n")

        while True:
            try:
                msg, address = client_socket.recvfrom(1200)
            except socket.timeout:
                continue
            print(f"CLIENT ID = {self.id}: received a message from SERVER! ({time.ctime(time.time())})\n")

            protocol_id, version, message_type, sequence_number, peer_id, payload_length, timestamp = protocol.unpack_header(msg)
            if message_type == protocol.MessageType.PEER_LIST:
                self.peers_list = pickle.loads(msg[protocol.HEADER_SIZE:])
                self.id = max(self.peers_list.keys())
                print(f"CLIENT ID = {self.id}: received a PEER LIST from SERVER! ({time.ctime(time.time())})\n{self.peers_list}")
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
        
        rtt_thread.join()
        bitmap_adv_thread.join()
        listen_thread.join()
        req_thread.join()
        client_socket.close()
        
        print(f"CLIENT ID = {self.id} ID = {self.id}: Shutting down, I am finished! ({time.ctime(time.time())})\n")

    def request(self, client_socket):
        while True:
            if self.shutdown_event.wait(timeout=0.01):
                break
            for chunk_id in range(protocol.NUM_CHUNKS): #TODO: only iterate on the remaining chunks

                # Skip chunks that the client already has
                if protocol.has(self.bitmap, chunk_id):
                    continue

                # Find peers that have this chunk
                available_peers = [
                    peer for peer in self.rtt_list
                    if protocol.has(peer.latest_bitmap, chunk_id) and peer.done == False
                ]

                if not available_peers:
                    continue

                # Select the peer with the lowest RTT
                selected_peer = min(
                    available_peers,
                    key=lambda peer: peer.latest_rtt
                )

                # Send chunk request
                payload = pickle.dumps(chunk_id)

                header = protocol.pack_header(
                    1,
                    protocol.MessageType.CHUNK_REQ,
                    0,
                    self.id,
                    len(payload)
                )

                client_socket.sendto(
                    header + payload,
                    selected_peer.address
                )

                print(f"CLIENT ID = {self.id}: sent a REQUEST to {selected_peer.id} for {chunk_id}! ({time.ctime(time.time())})\n")

    def rtt_measurement(self, client_socket):
        while not self.shutdown_event.is_set():
            others = [p for p in list(self.peers_list.values()) if p.id != self.id]
            self.rtt_list = sorted(others, key=lambda p: p.latest_rtt)[:4]
            for peer in others:
                self.ping_sent[peer.id] = time.perf_counter()
                head = protocol.pack_header(1, protocol.MessageType.PEER_HEARTBEAT, 0, self.id, 0)
                client_socket.sendto(head, peer.address)
            if self.shutdown_event.wait(2):  # was 10
                break

    def bitmap_adv(self, client_socket):
        server_addr = (self.SERVER_IP, self.SERVER_PORT)
        while not self.shutdown_event.is_set():
            targets = [p.address for p in list(self.peers_list.values()) if p.id != self.id]
            targets.append(server_addr)
            msg = pickle.dumps(self.bitmap)
            head = protocol.pack_header(1, protocol.MessageType.CHUNK_BITMAP, 0, self.id, len(msg), int(time.time()))
            for addr in targets:
                client_socket.sendto(head + msg, addr)
            if self.shutdown_event.wait(0.5):
                break

    def listen(self, client_socket):
        while not self.shutdown_event.is_set():
            try:
                msg, address = client_socket.recvfrom(1200)
            except socket.error:
                continue

            protocol_id, version, message_type, sequence_number, peer_id, payload_length, timestamp = protocol.unpack_header(msg)

            match message_type:
                case protocol.MessageType.CHUNK_BITMAP:
                    if peer_id in self.peers_list:
                        self.peers_list[peer_id].latest_bitmap = pickle.loads(msg[protocol.HEADER_SIZE:])

                case protocol.MessageType.PEER_HEARTBEAT:
                    header = protocol.pack_header(1, protocol.MessageType.HEARTBEAT_ACK, 0, self.id, 0, int(time.time()))
                    client_socket.sendto(header, address)
                    print(f"CLIENT ID = {self.id}: received a HEARTBEAT from {peer_id}! ({time.ctime(time.time())})\n")

                case protocol.MessageType.CHUNK_DATA:
                    chunk_id, chunk = pickle.loads(msg[protocol.HEADER_SIZE:])

                    if not protocol.has(self.bitmap, chunk_id):
                        self.chunks[chunk_id] = chunk
                        self.bitmap = protocol.set_bit(self.bitmap, chunk_id)

                        print(f"CLIENT ID = {self.id}: Received CHUNK: {chunk_id} from {peer_id if peer_id != 0 else "SERVER"}! ({time.ctime(time.time())})\n")

                        if self.bitmap.bit_count() == protocol.NUM_CHUNKS:
                            print(f"CLIENT ID = {self.id}: MAP completed! ({time.ctime(time.time())})\n")
                            header = protocol.pack_header(1, protocol.MessageType.MAP_COMPLETE, sequence_number, self.id, 0)
                            client_socket.sendto(header, (self.SERVER_IP, self.SERVER_PORT))

                case protocol.MessageType.PEER_LIST:
                    peer = pickle.loads(msg[protocol.HEADER_SIZE:])
                    if peer.id in self.peers_list:
                        self.peers_list[peer.id].done = peer.done
                        self.peers_list[peer.id].latest_bitmap = peer.latest_bitmap
                        print(f"CLIENT ID = {self.id}: received an old PEER from SERVER! SETTING IT TO DONE!({time.ctime(time.time())})\n")
                    else:
                        self.peers_list[peer.id] = peer
                        print(f"CLIENT ID = {self.id}: received a new PEER from SERVER! ({time.ctime(time.time())})\n")

                case protocol.MessageType.COMPLETE_ACK:
                    print(f"CLIENT ID = {self.id}: SERVER acknowledged MAP completion! ({time.ctime(time.time())})\n")
                    self.shutdown_event.set()
                
                case protocol.MessageType.CHUNK_REQ:
                    chunk_id = pickle.loads(msg[protocol.HEADER_SIZE:])
                    if protocol.has(self.bitmap, chunk_id):
                        chunk = self.chunks[chunk_id]
                        payload = pickle.dumps((chunk_id, chunk))
                        header = protocol.pack_header(1, protocol.MessageType.CHUNK_DATA, sequence_number, self.id, len(payload))
                        client_socket.sendto(header + payload, address)
                    print(f"CLIENT ID = {self.id}: received a REQUEST from {peer_id} for CHUNK: {chunk_id}! ({time.ctime(time.time())})\n")

                case protocol.MessageType.HEARTBEAT_ACK:
                    sent = self.ping_sent.get(peer_id)
                    if sent is not None and peer_id in self.peers_list:
                        self.peers_list[peer_id].latest_rtt = time.perf_counter() - sent

if __name__ == "__main__":
    client = Client('127.0.0.1', 8000)
    client.start_client()