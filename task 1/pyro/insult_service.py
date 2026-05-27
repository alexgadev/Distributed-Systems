import Pyro4

from multiprocessing import Process
import random, time, signal, sys, json

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

#def shutdown_server(daemon):
#    daemon.shutdown()
#    if broadcast_server.is_alive():
#        broadcast_server.terminate()
#        broadcast_server.join()
#    sys.exit(0)

@Pyro4.expose
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
    
    def subscribe_broadcaster(self, client_uri_pos):
        with open("task 1/pyro/settings.json", "r+") as file:
            data = json.load(file)
            try:
                print("client_uri_pos = " + str(client_uri_pos))
                client_uri = data['client_uri'][client_uri_pos]
            except Exception as e:
                print(e)
                return True

        #client_uri = input("Enter InsultService URI: ")
        self.subscribers.append(Pyro4.Proxy(client_uri))
        if not self.running: # only create one thread to subscribe to
            self.running = True
            broadcast_server = Process(target=start_broadcast(self.insults, self.subscribers, self.running))
            broadcast_server.start()
        return True

if __name__ == "__main__":
    daemon = Pyro4.Daemon()
    uri = daemon.register(InsultService)
    #print("InsultService PyRO URI: ", uri)

    with open("task 1/pyro/settings.json", "r+") as file:
        data = json.load(file)
        data['service_uri'] = str(uri)
        file.seek(0)
        json.dump(data, file, indent=4)
        file.truncate()

    #signal.signal(signal.SIGINT, shutdown_server(daemon))

    daemon.requestLoop()