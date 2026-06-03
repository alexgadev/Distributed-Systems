import Pyro4

from multiprocessing import Process, Manager
import random, time, signal, sys, json

broadcast_server = None

def start_broadcast(insults, subscribers):
    while True:
        if insults and subscribers:
            for uri in list(subscribers):
                try:
                    Pyro4.Proxy(uri).receive_broadcast(random.choice(insults))
                except Pyro4.errors.CommunicationError:
                    pass
                except Exception as e:
                    subscribers.remove(uri)
        if not subscribers: # in case subscribers shut down
            break
        time.sleep(5)

def sanitize_closeup():
    with open("task 1/pyro/settings.json", "r+") as file:
        data = json.load(file)
        data['service_uri'] = ""
        file.seek(0)
        json.dump(data, file, indent=4)
        file.truncate()

def shutdown_server(signum, frame):
    global broadcast_server
    if broadcast_server and broadcast_server.is_alive():
        broadcast_server.terminate()
        broadcast_server.join()
    sanitize_closeup()
    sys.exit(0)

@Pyro4.expose
class InsultService:
    def __init__(self, insults, subscribers):
        self.insults = insults
        self.running = False
        self.subscribers = subscribers
    
    def add_insult(self, insult):
        if insult not in self.insults:
            self.insults.append(insult)
            return True
        return False
    
    def get_insults(self):
        return list(self.insults)
    
    def subscribe_broadcaster(self, client_uri_pos):
        with open("task 1/pyro/settings.json", "r+") as file:
            data = json.load(file)
            client_uri = data['client_uri'][client_uri_pos]["uri"]

        if client_uri not in self.subscribers:
            self.subscribers.append(client_uri)
            if not self.running: # only create one thread to subscribe to
                self.running = True
                global broadcast_server
                broadcast_server = Process(target=start_broadcast, args=(self.insults, self.subscribers))
                broadcast_server.start()
            return True
        else:
            return False

if __name__ == "__main__":
    manager = Manager()
    insults = manager.list()
    subscribers = manager.list()

    daemon = Pyro4.Daemon()
    uri = daemon.register(InsultService(insults, subscribers))

    with open("task 1/pyro/settings.json", "r+") as file:
        data = json.load(file)
        data['service_uri'] = str(uri)
        file.seek(0)
        json.dump(data, file, indent=4)
        file.truncate()

    signal.signal(signal.SIGINT, shutdown_server)

    daemon.requestLoop()