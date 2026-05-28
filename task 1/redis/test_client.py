import redis

from insult_service import InsultService
from insult_filter import *


if __name__ == "__main__":
    r = redis.Redis(decode_responses=True)
    service = InsultService()

    # send insults
    print("Sending insults...")
    service.add_insult("idiot")
    service.add_insult("stupid")

    # filter a text with one of the insults sent in it
    submit_text("I tried to code something in the library but this idiot wouldn't let me work by myself.")

    # retrieve insult list
    print("\nRetrieving list of insults...")
    for insult in service.get_insults():
        print("Insult retrieved: " + insult)

    # obtain the filtered text(s)
    print(get_filtered())

    print("Listening to Redis insult broadcast:")
    sub = r.pubsub()
    sub.subscribe("insult_broadcast")
    try:
        for msg in sub.listen():
            if msg['type'] == 'message':
                print("[Client] Received broadcast:", msg['data'].decode())
    except KeyboardInterrupt:
        print("Closing client...")