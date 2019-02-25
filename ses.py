import requests
import time
from config import MY_LOGGER


class GetExceptionFailed(Exception):
    pass


class Requester:

    def __init__(self):
        self.session = requests.session()
        self.attempts = 1

    def reset_session(self):
        self.session = requests.session()

    def reset_attemps(self):
        self.attempts = 1

    def get(self, url):
        if self.attempts > 3:
            MY_LOGGER.error(f"Failed to get {url}")
            raise GetExceptionFailed(f"Failed to get {url}")
        response = self.session.get(url)
        if response.status_code != 200:
            self.attempts += 1
            time.sleep(1)
            self.reset_session()
            self.get(url)
        self.reset_attemps()
        return response


if __name__ == "__main__":
    r = Requester()

    r.get("http://azor.weps.cz")
