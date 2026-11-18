class peer_details:
    def __init__(self, ip, port, latest_bitmap = 0, latest_rtt = -1, id = -1, done = False): #TODO: make it so done close all threads in client but listen thread
        self.id = id
        self.address = (ip, port)
        self.latest_bitmap = latest_bitmap
        self.latest_rtt = latest_rtt
        self.done = done