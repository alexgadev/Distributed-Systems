import redis

INSULT_LIST = "insult_list"
FILTER_QUEUE = "filter_queue"
FILTERED_RESULTS_QUEUE = "filtered_results_queue"

if __name__ == "__main__":
    r = redis.Redis(decode_responses=True)

    print("Redis InsultFilter listening...")
    try:
        while True:
            _, text = r.blpop(FILTER_QUEUE) # blocks until a job arrives
            INSULTS = r.lrange(INSULT_LIST, 0, -1)
            for insult in INSULTS:
                text = text.replace(insult, "CENSORED")

            # store filtered text into the result list
            r.rpush(FILTERED_RESULTS_QUEUE, text)
    except KeyboardInterrupt:
        print("Closing Redis InsultFilter...")