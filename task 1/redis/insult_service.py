import redis
import time
import random

from multiprocessing import Process

INSULT_LIST = "insult_list"
BROADCAST_EXCHANGE = "insult_pubsub"

def start_broadcast():
    r = redis.Redis(decode_responses=True)
    while True:
        n_subscribers = dict(r.pubsub_numsub(BROADCAST_EXCHANGE)).get(BROADCAST_EXCHANGE, 0)
  
        # should only send insults if there is at least one subscriber
        if n_subscribers > 0:
            insults = list(r.smembers(INSULT_LIST))
            if insults:
                r.publish(BROADCAST_EXCHANGE, random.choice(insults))
        time.sleep(5)    

class InsultService:
    """As Redis also is able to store data, there's no need to treat any of the incoming insults.
        Clients will only need to add them to the respective list and retrieve them from the same list,
        no need to involve the service for anything else than the broadcaster.
    """

    def __init__(self) -> None:
        self.r = redis.Redis(decode_responses=True)
        self.broadcast_proc = None

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