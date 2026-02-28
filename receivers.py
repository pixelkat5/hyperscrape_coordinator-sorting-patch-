class Receiver():
    def __init__(self, url: str, max_upload: int, receiver_token: str, hostname: str):
        self.url = url
        self.max_upload: int
        self.receiver_token = receiver_token
        self.hostname = hostname