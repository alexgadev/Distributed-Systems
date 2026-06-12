import pika
import json
import uuid

INSULT_QUEUE = "insult_queue"
FILTER_QUEUE = "insult_filter_queue"
FILTERED_RESULTS_QUEUE = "insult_filtered_results"


class InsultFilter:
    """
    Class that provides the workflows to treat jobs coming to the filter queue

    """

    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()

        self.channel.queue_declare(queue=FILTER_QUEUE, durable=True)
        self.channel.queue_declare(queue=FILTERED_RESULTS_QUEUE, durable=True)

        # separate connection for RPC calls to the service — calling process_data_events()
        # on the same connection from within a basic_consume callback causes re-entrancy
        # issues in pika's BlockingConnection and the response is never dispatched
        self.rpc_connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.rpc_channel = self.rpc_connection.channel()

        result = self.rpc_channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        self.rpc_channel.basic_consume(
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

    def _get_insults(self):
        """RPC call to insult_service to obtain updated insult list
        """

        self.rpc_response = None
        self.corr_id = str(uuid.uuid4())
        self.rpc_channel.basic_publish(
            exchange='',
            routing_key=INSULT_QUEUE,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id
            ),
            body=json.dumps({'action': 'get_insults'})
        )
        while self.rpc_response is None:
            self.rpc_connection.process_data_events()
        return json.loads(self.rpc_response).get('result', [])

    def on_filter_request(self, ch, method, props, body):
        """Actual callback function for the filter queue. Sends filtered texts to a filtered queue.
        """

        text = body.decode()
        for insult in self._get_insults():
            text = text.replace(insult, "CENSORED")
        self.channel.basic_publish(
            exchange='',
            routing_key=FILTERED_RESULTS_QUEUE,
            properties=pika.BasicProperties(delivery_mode=2),
            body=text
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        """Starts consuming on the filter queue
        """

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=FILTER_QUEUE, on_message_callback=self.on_filter_request)
        self.channel.start_consuming()


if __name__ == "__main__":
    worker = InsultFilter()

    print("RabbitMQ InsultFilter listening...")
    try:
        worker.run()
    except KeyboardInterrupt:
        print("Closing RabbitMQ InsultFilter...")
        worker.connection.close()
        worker.rpc_connection.close()
