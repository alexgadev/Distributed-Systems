import Pyro4

from multiprocessing import Process, Manager

import time, json

uri = None

@Pyro4.expose
class Broadcast_Receiver:
    def receive_broadcast(self, message):
        print("Broadcast received: ", message)
        return True

def start_client_server(shared_pos, uri):
    daemon = Pyro4.Daemon()
    uri.value = daemon.register(Broadcast_Receiver)
    with open("task 1/pyro/settings.json", "r+") as file:
        data = json.load(file)
        shared_pos.value = data['n_clients']

        client = {
            "uri": str(uri.value)
        }
        
        data['client_uri'].append(client)
        file.seek(0)
        data['n_clients'] = shared_pos.value + 1
        json.dump(data, file, indent=4)
        file.truncate()

    daemon.requestLoop()


def sanitize_closeup(uri):
    with open("task 1/pyro/settings.json", "r+") as file:
        data = json.load(file)
        n_clients = data['n_clients']
        data['n_clients'] = n_clients - 1

        for i, client in enumerate(data["client_uri"]): 
            if client["uri"] == str(uri):
                data['client_uri'].remove({"uri": str(uri)})
                break

        file.seek(0)
        json.dump(data, file, indent=4)
        file.truncate()


if __name__ == "__main__":
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
        # variables to communicate and modify through different processes
        shared_pos = manager.Value('i', -1)
        shared_uri = manager.Value('c', "")

        # create process to listen to broadcast
        p = Process(target=start_client_server, args=(shared_pos, shared_uri))
        p.start()

        # wait to populate all variables before accessing them (could do this in a better way tho)
        time.sleep(5)

        uri = shared_uri.value # save uri to be able to remove it from settings later

        insult_srv.subscribe_broadcaster(shared_pos.value)

    # leave client running to retrieve some more insults from broadcast
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Closing client...")
        if p.is_alive():
            p.terminate()
            p.join()
        sanitize_closeup(uri)