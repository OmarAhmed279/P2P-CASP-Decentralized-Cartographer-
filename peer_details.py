class peer_details:
    def __init__(self, ip, port, latest_bitmap = 0b0, latest_rtt = -1, id = -1):
        self.id = id
        self.address = (ip, port)
        self.latest_bitmap = latest_bitmap
        self.latest_rtt = latest_rtt