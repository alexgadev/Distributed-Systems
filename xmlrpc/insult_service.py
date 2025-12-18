from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler

from multiprocessing import Process
import random, time, signal, sys

import xmlrpc.client

broadcast_server = None

def start_broadcast(insults, subscribers: list, running):
    while running:
        if insults and len(subscribers) > 0:
            for subscriber in subscribers:
                try:
                    subscriber.receive_broadcast(random.choice(insults))
                except Exception as e:
                    subscribers.remove(subscriber)
        elif not len(subscribers) > 0: # in case subscribers shut down
            running = False
            if broadcast_server.is_alive():
                broadcast_server.terminate()
                broadcast_server.join()
            break
        time.sleep(5)

def shutdown_server(server):
    server.shutdown()
    if broadcast_server.is_alive():
        broadcast_server.terminate()
        broadcast_server.join()
    sys.exit(0)

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class InsultService:
    def __init__(self):
        self.insults = []
        self.running = False
        self.subscribers = []

    def add_insult(self, insult):
        if insult not in self.insults:
            self.insults.append(insult)
            return True
        return False

    def get_insults(self):
        return self.insults

    def subscribe_broadcaster(self, port):
        self.subscribers.append(xmlrpc.client.ServerProxy("http://localhost:" + port))
        if not self.running: # only create one thread to subscribe to
            self.running = True
            broadcast_server = Process(target=start_broadcast(self.insults, self.subscribers, self.running))
            broadcast_server.start()
        return True


if __name__ == "__main__":
    with SimpleXMLRPCServer(('localhost', 8000),
                            requestHandler=RequestHandler) as server:
        server.register_introspection_functions()

        # Register the InsultService instance; all methods of the instance are
        # published as XML-RPC methods
        server.register_instance(InsultService())
        
        # Run the server's main loop
        print("InsultService XMLRPC running on port 8000")
    
        signal.signal(signal.SIGINT, shutdown_server) # close broadcast process when closing the server
    
        server.serve_forever()