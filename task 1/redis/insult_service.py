import redis
import time
import random

from multiprocessing import Process

INSULT_LIST_KEY = "insults"
CHANNEL = "insult_broadcast"

class InsultService:
    def __init__(self) -> None:
        self.r = redis.Redis(decode_responses=True)

    def add_insult(self, insult):
        if insult not in self.r.lrange(INSULT_LIST_KEY, 0, -1):
            self.r.rpush(INSULT_LIST_KEY, insult)
            return True
        else:
            return False

    def get_insults(self):
        return self.r.lrange(INSULT_LIST_KEY, 0, -1)

    def start_broadcast(self):
        while True:
            n_subscribers = self.r.pubsub_numsub(CHANNEL).get(CHANNEL, 0)
            if n_subscribers > 0:
                insults = self.get_insults()
                if insults:
                    self.r.publish(CHANNEL, random.choice(insults))
            time.sleep(5)

if __name__ == "__main__":
    print("Redis InsultService running...")
    service = InsultService()
    p = Process(target=service.start_broadcast)
    p.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        p.terminate()
        p.join()