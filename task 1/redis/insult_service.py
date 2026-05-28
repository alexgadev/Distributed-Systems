import redis
import time
import random

from multiprocessing import Process

r = redis.Redis(decode_responses=True)

INSULT_LIST_KEY = "insults"
CHANNEL = "insult_broadcast"

def add_insult(insult):
    if insult not in r.lrange(INSULT_LIST_KEY, 0, -1):
        r.rpush(INSULT_LIST_KEY, insult)
        return True
    else:
        return False

def get_insults():
    return r.lrange(INSULT_LIST_KEY, 0, -1)

def start_broadcast():
    while True:
        n_subscribers = r.pubsub_numsub(CHANNEL).get(CHANNEL, 0)
        if n_subscribers > 0:
            insults = get_insults()
            if insults:
                r.publish(CHANNEL, random.choice(insults))
        time.sleep(5)

if __name__ == "__main__":
    print("Redis InsultService running...")
    p = Process(target=start_broadcast)
    p.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        p.terminate()
        p.join()