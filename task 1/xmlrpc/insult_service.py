import random
import signal
import sys
import time

from multiprocessing import Process, Manager

import xmlrpc.client

from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler


broadcast_server = None # the broadcast process

def start_broadcast(insults, subscribers):
    """Starts the broadcaster if not already started and sends an insult to every subscriber every 5 seconds

    Parameters
    ----------
    insults : Manager().list()
        The list containing the insults
    subscribers : Manager().list()
        A list containing the URLs of the subscribers
    """

    while True:
        if insults and subscribers:
            for url in list(subscribers):
                try:
                    # Connect to each client to send the broadcast
                    xmlrpc.client.ServerProxy(url).receive_broadcast(random.choice(insults))
                except ConnectionRefusedError:
                    print("Tried to broadcast a message but client isn't ready yet")
                except Exception:
                    try:
                        subscribers.remove(url)
                    except ValueError:
                        pass
        if not subscribers: # in case subscribers shut down
            break
        time.sleep(5)

def shutdown_server(signum, frame):
    """Shuts down the broadcast process and exits execution of the service
    """

    global broadcast_server
    if broadcast_server and broadcast_server.is_alive():
        broadcast_server.terminate()
        broadcast_server.join()
    sys.exit(0)


class RequestHandler(SimpleXMLRPCRequestHandler):
    """
    XMLRPC Request handler
    """
    rpc_paths = ('/RPC2',)

class InsultService:
    """
    Class that provides the funcionalities needed to get and send insults to an XMLRPC server


    Attributes
    ----------
    insults : Manager().list()
        a list of string insults that persist through processes
    subscribers : Manager().list()
        a list of subscriber URLs that persist through processes
    running : bool
        the current state of the broadcaster of insults

    Methods
    -------
    add_insult(insult) -> bool:
        Adds an insult to the insult list
    get_insults() -> Manager().list()
        Returns the current state of the insult list
    subscribe_broadcaster(port) -> bool
        Appends the specified port to the subscriber list and starts the broadcast server if not already started
    """

    def __init__(self, insults, subscribers):
        """
        Parameters
        ----------
        insults : Manager().list()
            a list of string insults that persist through processes
        subscribers : Manager().list()
            a list of subscriber URLs that persist through processes
        """

        self.insults = insults
        self.subscribers = subscribers
        self.running = False # keep the state of the broadcaster

    def add_insult(self, insult):
        """Adds the insult to the insult list

        If it already exists in the list, does nothing and returns false

        Parameters
        ----------
        insult : str
            The insult to be added to the list
        """

        if insult not in self.insults:
            self.insults.append(insult)
            return True
        return False

    def get_insults(self):
        """Gets the insult list
        """

        return list(self.insults)

    def subscribe_broadcaster(self, port):
        """Subscribes the port (appended to the base URL) to the insult broadcaster

        Parameters
        ----------
        port : int
            The port to which the broadcaster will attempt to make the connection to broadcast an insult
        """

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