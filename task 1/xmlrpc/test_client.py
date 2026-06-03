import xmlrpc.client

from multiprocessing import Process

import time, sys

def start_client_server():
    """
    Helper function to provide funcionality for the server to broadcast messages
    """
    from xmlrpc.server import SimpleXMLRPCServer
    from xmlrpc.server import SimpleXMLRPCRequestHandler

    class RequestHandler(SimpleXMLRPCRequestHandler):
        rpc_paths = ('/RPC2',)
    
    server = SimpleXMLRPCServer(('localhost', 9000),
                            requestHandler=RequestHandler, 
                            logRequests=False,
                            allow_none=True)
    server.register_introspection_functions() # not really needed

    def receive_broadcast(message):
        print("Broadcast received: ", message)
        sys.stdout.flush()
        return True

    server.register_function(receive_broadcast)

    server.serve_forever()


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