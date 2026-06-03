import pika
import json
import uuid

INSULT_QUEUE = "insult_queue"
FILTER_QUEUE = "insult_filter_queue"
FILTERED_RESULTS_QUEUE = "insult_filtered_results"


class InsultFilter:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()

        self.channel.queue_declare(queue=FILTER_QUEUE, durable=True)
        self.channel.queue_declare(queue=FILTERED_RESULTS_QUEUE, durable=True)

        # declare a callback queue so the insult_service has somewhere to send the result to
        # that we can use to treat the response
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self._on_rpc_response,
            auto_ack=True
        )

        self.rpc_response = None
        self.corr_id = None

    # callback function, checks for the same correlation id in case of misrouting
    # when having multiple filters and saves response
    def _on_rpc_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.rpc_response = body

    # RPC call to insult_service to obtain updated insult list
    def _get_insults(self):
        self.rpc_response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key=INSULT_QUEUE,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id
            ),
            body=json.dumps({'action': 'get_insults'})
        )
        while self.rpc_response is None:
            self.connection.process_data_events()
        return json.loads(self.rpc_response).get('insults', [])

    # actual callback function for the filter queue
    # sends filtered texts to a filtered queue
    def on_filter_request(self, ch, method, props, body):
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
