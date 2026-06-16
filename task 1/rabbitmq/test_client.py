import pika
import json
import uuid
import time

INSULT_QUEUE = "insult_queue"
FILTER_QUEUE = "insult_filter_queue"
FILTERED_RESULTS_QUEUE = "insult_filtered_results"
BROADCAST_EXCHANGE = "insult_broadcast"


class InsultClient:
    """Helper class to ease working with RabbitMQ's queuess
    """

    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()

        self.channel.queue_declare(queue=FILTER_QUEUE, durable=True)
        self.channel.queue_declare(queue=FILTERED_RESULTS_QUEUE, durable=True)

        # declare a queue to get the response
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self._on_rpc_response,
            auto_ack=True
        )

        self.rpc_response = None
        self.corr_id = None

    def _on_rpc_response(self, ch, method, props, body):
        """Callback function, checks for the same correlation id in case of missrouting
            when having multiple filters and saves response
        """

        if self.corr_id == props.correlation_id:
            self.rpc_response = body

    def _call(self, request):
        """Defines the basic workflow to send a job to the insult queue
        """

        self.rpc_response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key=INSULT_QUEUE,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id
            ),
            body=json.dumps(request)
        )
        while self.rpc_response is None:
            self.connection.process_data_events()
        return json.loads(self.rpc_response)

    def add_insult(self, insult):
        return self._call({'action': 'add_insult', 'insult': insult})

    def get_insults(self):
        return self._call({'action': 'get_insults'}).get('result', [])

    def submit_filter(self, text):
        """Defines the workflow to send a job to the filter queue
        """

        self.channel.basic_publish(
            exchange='',
            routing_key=FILTER_QUEUE,
            properties=pika.BasicProperties(delivery_mode=2),
            body=text
        )

    def get_filtered_results(self):
        """Accesses the filtered result queue to obtain all filtered texts
        """

        results = []
        while True:
            method_frame, _, body = self.channel.basic_get(queue=FILTERED_RESULTS_QUEUE, auto_ack=True)
            if not method_frame:
                break
            results.append(body.decode())
        return results

    def listen_broadcast(self):
        """Subscribes to the broadcast pubsub and defines the method callback for every message received
        """

        self.channel.exchange_declare(exchange=BROADCAST_EXCHANGE, exchange_type='fanout')
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.channel.queue_bind(exchange=BROADCAST_EXCHANGE, queue=result.method.queue)

        def on_broadcast(ch, method, props, body):
            print("[Client] Received broadcast:", body.decode())

        self.channel.basic_consume(queue=result.method.queue, on_message_callback=on_broadcast, auto_ack=True)
        print("Listening to RabbitMQ insult broadcast:")
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print("Closing client...")
            self.connection.close()


if __name__ == "__main__":
    client = InsultClient()

    # send insults
    print("Sending insults...")
    client.add_insult("idiot")
    client.add_insult("stupid")

    # filter a text with one of the insults sent in it
    client.submit_filter("I tried to code something in the library but this idiot wouldn't let me work by myself.")

    time.sleep(2)

    # retrieve insult list
    print("\nRetrieving list of insults...")
    for insult in client.get_insults():
        print("Insult retrieved:", insult)

    # obtain the filtered text(s)
    print("\nFiltered results:")
    for text in client.get_filtered_results():
        print(text)

    # subscribe to broadcaster
    client.listen_broadcast()
