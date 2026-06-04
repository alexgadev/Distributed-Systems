import pika
import time
import random
import json
from multiprocessing import Process, Manager

INSULT_QUEUE = "insult_queue"
BROADCAST_EXCHANGE = "insult_broadcast"


def start_broadcast(insult_list):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    
    # declare a pubsub exchange
    channel.exchange_declare(queue=BROADCAST_EXCHANGE, exchange_type='fanout')

    while True:
        if insult_list:
            insult = random.choice(list(insult_list))
            channel.basic_publish(exchange=BROADCAST_EXCHANGE, routing_key='', body=insult)
        time.sleep(5)

class InsultService:
    def __init__(self) -> None:
        manager = Manager()
        self.insults = manager.list()
        self.broadcast_proc = None 

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()

        self.channel.queue_declare(queue=INSULT_QUEUE, durable=True)

    def add_insult(self, insult):
        # avoid duplication by checking first if it already exists
        if insult not in self.insults:
            self.insults.append(insult)
            return True
        else:
            return False

    def get_insults(self):
        return list(self.insults)

    def start_broadcaster(self):
        if self.broadcast_proc and self.broadcast_proc.is_alive():
            return False
        # create broadcast process
        self.broadcast_proc = Process(target=start_broadcast)
        self.broadcast_proc.start()
        return True

    def stop_broadcaster(self):
        if self.broadcast_proc and self.broadcast_proc.is_alive():
            self.broadcast_proc.terminate()
            self.broadcast_proc.join()
            self.broadcast_proc = None
            return True
        return False
    
    def request_callback(self, ch, method, props, body):
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