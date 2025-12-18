import Pyro4

from multiprocessing import Process

import time

uri_broadcast_receiver = None

@Pyro4.expose
class Broadcast_Receiver:
    def receive_broadcast(self, message):
        print("Broadcast received: ", message)
        return True

def start_client_server():
    daemon = Pyro4.Daemon()
    uri = daemon.register(Broadcast_Receiver)
    print("Broadcast_Receiver URI: ", uri)

    daemon.requestLoop()


if __name__ == "__main__":
    INSULT_SERVICE_URI = input("Enter InsultService URI: ")
    insult_srv = Pyro4.Proxy(INSULT_SERVICE_URI)
    
    INSULT_FILTER_URI = input("Enter InsultFilter URI: ")
    filter_srv = Pyro4.Proxy(INSULT_FILTER_URI)

    # send insults
    print("Sending insults...")
    insult_srv.add_insult("idiot")
    insult_srv.add_insult("stupid")

    # filter a text with one of the insults sent in it
    filter_srv.submit_text("I tried to code something in the library but this idiot wouldn't let me work by myself.")

    # retrieve insult list
    print("\nRetrieving list of insults...")
    for insult in insult_srv.get_insults():
        print("Insult retrieved: " + insult)

    # obtain the filtered text(s)
    print(filter_srv.get_filtered())

    # subscribe to broadcaster
    print("\nAttempting to subscribe to broadcaster...")
    p = Process(target=start_client_server)
    p.start()
    insult_srv.subscribe_broadcaster()

    # leave client running to retrieve some more insults from broadcast
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Closing client...")