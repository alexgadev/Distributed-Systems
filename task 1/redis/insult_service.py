import redis
import time
import random

from multiprocessing import Process

INSULT_LIST_KEY = "insults"
CHANNEL = "insult_broadcast"

def start_broadcast():
    r = redis.Redis(decode_responses=True)
    while True:
        n_subscribers = dict(r.pubsub_numsub(CHANNEL)).get(CHANNEL, 0)
  
        # should only send insults if there is at least one subscriber
        if n_subscribers > 0:
            insults = r.lrange(INSULT_LIST_KEY, 0, -1)
            if insults:
                r.publish(CHANNEL, random.choice(insults))
        time.sleep(5)    

class InsultService:
    def __init__(self) -> None:
        self.r = redis.Redis(decode_responses=True)
        self.broadcast_proc = None

    def add_insult(self, insult):
        # avoid duplication by checking first if it already exists
        if insult not in self.r.lrange(INSULT_LIST_KEY, 0, -1):
            self.r.rpush(INSULT_LIST_KEY, insult) 
            return True
        else:
            return False

    def get_insults(self):
        return self.r.lrange(INSULT_LIST_KEY, 0, -1)

    def start_broadcaster(self):
        if self.broadcast_proc and self.broadcast_proc.is_alive():
            return False
        # create broadcast process
        self.broadcast_proc = Process(target=start_broadcast)
        self.broadcast_proc.start()
        return True

    def stop_broadcaster(self):
        if self.broadcast_proc and self.broadcast_proc.is_alive():
            self.broadcast_proc.terminate()
            self.broadcast_proc.join()
            self.broadcast_proc = None
            return True
        return False

if __name__ == "__main__":
    service = InsultService()
    service.start_broadcaster() # the downpart of this is that we must run an instance of the service
                            # instead of being able to just call its functions from the client in order 
                            # not to create multiple broadcaster processes if multiple clients exist at once
    print("Redis InsultService running...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Closing Redis InsultService...")
        service.stop_broadcaster()