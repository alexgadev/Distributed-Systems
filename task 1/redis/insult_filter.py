import redis

r = redis.Redis(decode_responses=True)
pubsub = r.pubsub()

INPUT = "insult_broadcast"
OUTPUT = "insult_filtered"

INSULTS = ["idiot", "stupid", "nerd"]

def submit_text(text):
    for insult in INSULTS:
        text = text.replace(insult, "CENSORED")
    r.rpush("filtered", text)
    return text

if __name__ == "__main__":
    pubsub.subscribe(INPUT)
    print("Filter listenning...")

    for message in pubsub.listen():
        if message["type"] == "message":
            insult = message["data"]
            filtered = submit_text(insult)
            r.publish(OUTPUT, filtered)