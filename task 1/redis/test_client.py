import redis, time

INSULT_LIST = "insult_list"
BROADCAST_EXCHANGE = "insult_pubsub"
FILTER_QUEUE = "filter_queue"
FILTERED_RESULTS_QUEUE = "filtered_results_queue"


class InsultClient:
    """Helper class to ease the calls to redis' queues and lists
    """

    def __init__(self):
        self.r = redis.Redis(decode_responses=True)

        # fresh start
        #self.r.delete(INSULT_LIST, FILTER_QUEUE, FILTERED_RESULTS_QUEUE)

    def add_insult(self, insult):
        return bool(self.r.sadd(INSULT_LIST, insult))

    def get_insults(self):
        return list(self.r.smembers(INSULT_LIST))

    def submit_text(self, text):
        self.r.rpush(FILTER_QUEUE, text)

    def get_filtered_results(self):
        return self.r.lrange(FILTERED_RESULTS_QUEUE, 0, -1)

    def listen_broadcast(self):
        """Subscribes to the broadcast pubsub 
        """

        print("Listening to Redis insult broadcast:")
        sub = self.r.pubsub()
        sub.subscribe(BROADCAST_EXCHANGE)
        try:
            for msg in sub.listen():
                if msg['type'] == 'message':
                    print("[Client] Received broadcast:", msg['data'])
        except KeyboardInterrupt:
            print("Closing client...")


if __name__ == "__main__":
    client = InsultClient()

    # send insults
    print("Sending insults...")
    client.add_insult("idiot")
    client.add_insult("stupid")

    # filter a text with one of the insults sent in it
    client.submit_text("I tried to code something in the library but this idiot wouldn't let me work by myself.")

    time.sleep(2)

    # retrieve insult list
    print("\nRetrieving list of insults...")
    for insult in client.get_insults():
        print("Insult retrieved: " + insult)

    # obtain the filtered text(s)
    print(client.get_filtered_results())

    # subscribe to broadcaster
    client.listen_broadcast()