class peer_details:
    def __init__(self, id, ip, port, latest_bitmap = 0, latest_rtt = -1):
        self.id = id
        self.address = (ip, port)
        self.latest_bitmap = latest_bitmap
        self.latest_rtt = latest_rtt