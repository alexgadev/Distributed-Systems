import pika
import time
import random
import json
from multiprocessing import Process, Manager

INSULT_QUEUE = "insult_queue"
BROADCAST_EXCHANGE = "insult_broadcast"


def start_broadcast(insult_list):
    """Starts the broadcaster if not already started and sends an insult to every subscriber every 5 seconds

    Parameters
    ----------
    insults : Manager().list()
        The list containing the insults
    """

    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    
    # declare a pubsub exchange
    channel.exchange_declare(exchange=BROADCAST_EXCHANGE, exchange_type='fanout')

    while True:
        if insult_list:
            insult = random.choice(list(insult_list))
            channel.basic_publish(exchange=BROADCAST_EXCHANGE, routing_key='', body=insult)
        time.sleep(5)

class InsultService:
    """
    Class that provides the funcionalities needed to get and send insults to a Pyro server


    Attributes
    ----------
    insults : Manager().list()
        a list of string insults that persist through processes
    broadcast_proc : Process()
        the broadcast process instance
    connection : pika.BlockingConnection()
        the connection to the RabbitMQ broker
    channel : pika.BlockingConnection()
        the channel definition

    Methods
    -------
    add_insult(insult) -> bool:
        Adds an insult to the insult list
    get_insults() -> Manager().list()
        Returns the current state of the insult list
    start_broadcaster() -> bool
        Starts the broadcaster process calling start_broadcast()
    stop_broadcaster() -> bool
        Stops the broadcaster process
    request_callback(ch, method, props, body) -> None
        The callback function to every job arriving to the insult queue
    run() -> None
        Starts consuming on the insult queue for jobs
    """

    def __init__(self) -> None:
        manager = Manager()
        self.insults = manager.list()
        self.broadcast_proc = None 

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()

        self.channel.queue_declare(queue=INSULT_QUEUE, durable=True)

    def add_insult(self, insult):
        """Adds the insult to the insult list

        If it already exists in the list, does nothing and returns false

        Parameters
        ----------
        insult : str
            The insult to be added to the list
        """

        # avoid duplication by checking first if it already exists
        if insult not in self.insults:
            self.insults.append(insult)
            return True
        else:
            return False

    def get_insults(self):
        """Gets the insult list
        """

        return list(self.insults)

    def start_broadcaster(self):
        """Starts the broadcast process

        """

        if self.broadcast_proc and self.broadcast_proc.is_alive():
            return False
        # create broadcast process
        self.broadcast_proc = Process(target=start_broadcast, args=(self.insults, ))
        self.broadcast_proc.start()
        return True

    def stop_broadcaster(self):
        """Stops the broadcast process

        """

        if self.broadcast_proc and self.broadcast_proc.is_alive():
            self.broadcast_proc.terminate()
            self.broadcast_proc.join()
            self.broadcast_proc = None
            return True
        return False
    
    def request_callback(self, ch, method, props, body):
        """The callback function to every job arriving to the insult queue

        """

        req = json.loads(body)
        action = req.get('action')

        # create response depending on the action on the insult queue
        if action == 'add_insult':
            result = self.add_insult(req['insult'])
            response = json.dumps({'result': result})
        elif action == 'get_insults':
            response = json.dumps({'result': self.get_insults()})
        else:
            response = json.dumps({'error': 'Unkown action'})

        ch.basic_publish(
            exchange='',
            routing_key=props.reply_to,
            properties=pika.BasicProperties(correlation_id=props.correlation_id),
            body=response
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        """Starts consuming on the insult queue for jobs

        """

        self.channel.basic_qos(prefetch_count=1) # only one item at a time per worker
        self.channel.basic_consume(queue=INSULT_QUEUE, on_message_callback=self.request_callback)
        self.channel.start_consuming()

if __name__ == "__main__":
    service = InsultService()

    service.start_broadcaster() 

    print("RabbitMQ InsultService running...")
    try:
        service.run()
    except KeyboardInterrupt:
        print("Closing RabbitMQ InsultService...")
        service.stop_broadcaster()
        service.connection.close()