import Pyro4

from multiprocessing import Process, Manager

import time, json

@Pyro4.expose
class Broadcast_Receiver:
    def receive_broadcast(self, message):
        print("Broadcast received: ", message)
        return True

def start_client_server(shared_pos):
    daemon = Pyro4.Daemon()
    uri = daemon.register(Broadcast_Receiver)
    with open("task 1/pyro/settings.json", "r+") as file:
        data = json.load(file)
        shared_pos.value = data['n_clients']
        print("client_uri_pos = " + str(shared_pos.value))
        data['client_uri'].append(str(uri))
        file.seek(0)
        data['n_clients'] = shared_pos.value + 1
        json.dump(data, file, indent=4)
        file.truncate()

    daemon.requestLoop()


if __name__ == "__main__":
    #INSULT_SERVICE_URI = input("Enter InsultService URI: ")
    #INSULT_FILTER_URI = input("Enter InsultFilter URI: ")
    with open("task 1/pyro/settings.json", "r+") as file:
        data = json.load(file)
        INSULT_SERVICE_URI = data['service_uri']
        INSULT_FILTER_URI = data['filter_uri']

    insult_srv = Pyro4.Proxy(INSULT_SERVICE_URI)
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
    with Manager() as manager:
        shared_pos = manager.Value('i', -1)
        p = Process(target=start_client_server, args=(shared_pos,))
        p.start()
        time.sleep(5)
        insult_srv.subscribe_broadcaster(shared_pos.value)

    # leave client running to retrieve some more insults from broadcast
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Closing client...")