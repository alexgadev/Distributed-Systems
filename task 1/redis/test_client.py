import redis

r = redis.Redis()
sub = r.pubsub()
sub.subscribe("insult_channel")

print("Listening to Redis insult broadcast:")
try:
    for msg in sub.listen():
        if msg['type'] == 'message':
            print("[Client] Received broadcast:", msg['data'].decode())
except KeyboardInterrupt:
    pass