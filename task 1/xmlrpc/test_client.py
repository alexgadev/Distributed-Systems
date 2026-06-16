import sys
import time
import signal

from multiprocessing import Process

import xmlrpc.client

from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler


def start_client_server():
    """Helper function to provide funcionality for the server to broadcast messages
    """

    class RequestHandler(SimpleXMLRPCRequestHandler):
        """
        XMLRPC Request handler
        """
        rpc_paths = ('/RPC2',)
    
    server = SimpleXMLRPCServer(('localhost', 9000), # this port is only hardcoded for the basic implementation
                            requestHandler=RequestHandler, 
                            logRequests=False,
                            allow_none=True)
    server.register_introspection_functions() # not really needed

    def receive_broadcast(message):
        """Prints a message sent by the broadcaster

        Parameters
        ----------
        message : str
            Message to be printed in the client console
        """

        print("Broadcast received: ", message)
        sys.stdout.flush()
        return True

    server.register_function(receive_broadcast)

    signal.signal(signal.SIGINT, shutdown_server) # graceful shutdown of the server

    server.serve_forever()


def shutdown_server(signum, frame): 
    """Exits execution of the client
    """
    sys.exit(0)


if __name__ == "__main__":
    insult_srv = xmlrpc.client.ServerProxy("http://localhost:8000")
    filter_srv = xmlrpc.client.ServerProxy("http://localhost:8001")

    # send insults
    print("Sending insults...")
    insult_srv.add_insult("idiot")
    insult_srv.add_insult("stupid")

    # filter a text with one of the insults sent in it
    filter_srv.submit_text("I tried to code something in the library but this idiot wouldn't let me work by myself.")

    filter_srv.submit_text("No politics here")

    # retrieve insult list
    print("\nRetrieving list of insults...")
    retrieved = insult_srv.get_insults()
    print(retrieved)

    # obtain the filtered text(s)
    time.sleep(2)
    print(filter_srv.get_filtered())

    # subscribe to broadcaster
    print("\nAttempting to subscribe to broadcaster...")
    p = Process(target=start_client_server)
    p.start()
    insult_srv.subscribe_broadcaster("9000")

    # leave client running to retrieve some more insults from broadcast
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Closing client...")