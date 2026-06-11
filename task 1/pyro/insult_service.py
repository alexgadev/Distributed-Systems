import sys
import json
import time
import random
import signal

import Pyro4

from multiprocessing import Process, Manager


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
            for uri in list(subscribers):
                try:
                    # Connect to each client to send the broadcast
                    Pyro4.Proxy(uri).receive_broadcast(random.choice(insults))
                except Pyro4.errors.CommunicationError:
                    print("Tried to broadcast a message but client isn't ready yet")
                except Exception as e:
                    try:
                        subscribers.remove(uri)
                    except ValueError:
                        pass
        if not subscribers: # in case subscribers shut down
            break
        time.sleep(5)

def sanitize_closeup():
    """Cleans the settings file for future executions
    """

    with open("task 1/pyro/settings.json", "r+") as file:
        data = json.load(file)
        data['service_uri'] = ""
        file.seek(0)
        json.dump(data, file, indent=4)
        file.truncate()

def shutdown_server(signum, frame):
    """Shuts down the broadcast process and exits execution of the service
    """

    global broadcast_server
    if broadcast_server and broadcast_server.is_alive():
        broadcast_server.terminate()
        broadcast_server.join()
    sanitize_closeup()
    sys.exit(0)

@Pyro4.expose
class InsultService:
    """
    Class that provides the funcionalities needed to get and send insults to a Pyro server


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
        self.running = False # keep the current state of the broadcaster
    
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
    
    def subscribe_broadcaster(self, client_uri_pos):
        """Subscribes the port (appended to the base URL) to the insult broadcaster

        Parameters
        ----------
        client_uri_pos : int
            The position of the uri in the settings array
        """

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

    # save service uri in settings file
    with open("task 1/pyro/settings.json", "r+") as file:
        data = json.load(file)
        data['service_uri'] = str(uri)
        file.seek(0)
        json.dump(data, file, indent=4)
        file.truncate()

    # Run the server's main loop
    print("InsultService Pyro running")

    signal.signal(signal.SIGINT, shutdown_server) # close broadcast process when closing the server

    daemon.requestLoop()