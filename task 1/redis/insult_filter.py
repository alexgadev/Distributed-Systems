import redis


INSULT_QUEUE = "insult_queue"
FILTERED_LIST_KEY = "insult_filtered"

if __name__ == "__main__":
    r = redis.Redis(decode_responses=True)
    

    print("Redis InsultFilter listening...")
    try:
        while True:
            _, text = r.blpop(INSULT_QUEUE) # blocks until a job arrives
            INSULTS = r.lrange("insults", 0, -1)
            for insult in INSULTS:
                text = text.replace(insult, "CENSORED")

            # store filtered text into the result list
            r.rpush(FILTERED_LIST_KEY, text)
    except KeyboardInterrupt:
        print("Closing Redis InsultFilter...")