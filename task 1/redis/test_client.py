import redis

from insult_service import *
from insult_filter import *

r = redis.Redis(decode_responses=True)
sub = r.pubsub()
sub.subscribe("insult_broadcast")

# send insults
print("Sending insults...")
add_insult("idiot")
add_insult("stupid")

# filter a text with one of the insults sent in it
submit_text("I tried to code something in the library but this idiot wouldn't let me work by myself.")

# retrieve insult list
print("\nRetrieving list of insults...")
for insult in get_insults():
    print("Insult retrieved: " + insult)

# obtain the filtered text(s)
print(get_filtered())

print("Listening to Redis insult broadcast:")
try:
    for msg in sub.listen():
        if msg['type'] == 'message':
            print("[Client] Received broadcast:", msg['data'].decode())
except KeyboardInterrupt:
    print("Closing client...")