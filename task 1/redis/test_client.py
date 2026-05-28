import redis, time

from insult_service import InsultService, CHANNEL
from insult_filter import INSULT_QUEUE, FILTERED_LIST_KEY

if __name__ == "__main__":
    r = redis.Redis(decode_responses=True)

    # fresh start
    r.delete("insults", INSULT_QUEUE, FILTERED_LIST_KEY)

    service = InsultService()

    # send insults
    print("Sending insults...")
    service.add_insult("idiot")
    service.add_insult("stupid")

    # filter a text with one of the insults sent in it
    r.rpush(INSULT_QUEUE, "I tried to code something in the library but this idiot wouldn't let me work by myself.")

    time.sleep(2)

    # retrieve insult list
    print("\nRetrieving list of insults...")
    for insult in service.get_insults():
        print("Insult retrieved: " + insult)

    # obtain the filtered text(s)
    print(r.lrange(FILTERED_LIST_KEY, 0, -1))

    print("Listening to Redis insult broadcast:")
    sub = r.pubsub()
    sub.subscribe(CHANNEL)
    try:
        for msg in sub.listen():
            if msg['type'] == 'message':
                print("[Client] Received broadcast:", msg['data'])
    except KeyboardInterrupt:
        print("Closing client...")