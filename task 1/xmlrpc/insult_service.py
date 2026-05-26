from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler

from multiprocessing import Process, Manager
import random, time, signal, sys

import xmlrpc.client

broadcast_server = None

def start_broadcast(insults, subscribers):
    while True:
        if insults and subscribers:
            for url in subscribers:
                try:
                    xmlrpc.client.ServerProxy(url).receive_broadcast(random.choice(insults))
                except Exception as e:
                    try:
                        subscribers.remove(url)
                    except ValueError:
                        pass
        elif not subscribers: # in case subscribers shut down
            break
        time.sleep(5)

def shutdown_server(signum, frame):
    global broadcast_server
    if broadcast_server and broadcast_server.is_alive():
        broadcast_server.terminate()
        broadcast_server.join()
    sys.exit(0)

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class InsultService:
    def __init__(self, insults, subscribers):
        self.insults = insults
        self.subscribers = subscribers
        self.running = False

    def add_insult(self, insult):
        if insult not in self.insults:
            self.insults.append(insult)
            return True
        return False

    def get_insults(self):
        return self.insults

    def subscribe_broadcaster(self, port):
        url = "http://localhost:" + port
        if url not in self.subscribers:
            self.subscribers.append(url)
        if not self.running: # only create one thread to subscribe to
            self.running = True
            global broadcast_server
            broadcast_server = Process(target=start_broadcast, args=(self.insults, self.subscribers))
            broadcast_server.start()
        return True


if __name__ == "__main__":
    manager = Manager()
    insults = manager.list()
    subscribers = manager.list()

    with SimpleXMLRPCServer(('localhost', 8000),
                            requestHandler=RequestHandler) as server:
        server.register_introspection_functions()

        # Register the InsultService instance; all methods of the instance are
        # published as XML-RPC methods
        server.register_instance(InsultService(insults, subscribers))
        
        # Run the server's main loop
        print("InsultService XMLRPC running on port 8000")
    
        signal.signal(signal.SIGINT, shutdown_server) # close broadcast process when closing the server
    
        server.serve_forever()