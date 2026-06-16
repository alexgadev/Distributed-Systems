import sys
import json
import pathlib
import signal

import Pyro4

from multiprocessing import Process, Queue, Manager

import sys, signal, json, os

NUM_WORKERS = int(os.environ.get("INSULT_FILTER_WORKERS", "1"))
workers = []


def worker_loop(service_uri, task_queue, filtered):
    """Main loop of each worker where they will wait for a job to arrive, get the 
        current list of insults and replace every insult for a CENSORED

    Parameters
    ----------
    service_uri : str
        service uri string
    task_queue : Queue()
        a queue containing all the jobs to be done
    filtered : Manager().list()
        a list of filtered texts
    """

    while True:
        try:
            text = task_queue.get(block=True) # blocks until new job
            for insult in Pyro4.Proxy(service_uri).get_insults():
                if insult in text:
                    text = text.replace(insult, "CENSORED")
            filtered.append(text)
        except (KeyboardInterrupt, EOFError):
            break


def sanitize_closeup():
    """Cleans the settings file for future executions
    """

    with open(pathlib.Path(__file__).parent / "settings.json", "r+") as file:
        data = json.load(file)
        # empty uri string
        data['filter_uri'] = ""
        # re-store again
        file.seek(0)
        json.dump(data, file, indent=4)
        file.truncate()

def shutdown_server(signum, frame):
    """Shuts down the worker processes sanitizes the settings file and exits 
        execution of the filter
    """

    for worker in workers: 
        if worker.is_alive():
            worker.terminate()
            worker.join()
    sanitize_closeup() 
    sys.exit(0)

@Pyro4.expose
class InsultFilter:
    """
    Class that provides the funcionalities needed to submit texts to a work queue to be censored


    Methods
    -------
    submit_text(text) -> bool:
        Submits a text to be treated by the workers
    get_filtered() -> Manager().list()
        Returns the current state of the filtered text list
    """

    def __init__(self, task_queue, filtered):
        """
        Parameters
        ----------
        task_queue : Queue()
            a queue containing all the jobs to be done
        filtered : Manager().list()
            a list of filtered texts
        """

        self.task_queue = task_queue
        self.filtered = filtered

    def submit_text(self, text):
        """Pushes a text to the working queue

        Parameters
        ----------
        text : str
            Text to be treated by the workers
        """

        self.task_queue.put(text)
        return text
    
    def get_filtered(self):
        """Gets the filtered text list
        """

        return list(self.filtered)



if __name__ == "__main__":
    task_queue = Queue()
    manager = Manager()
    filtered = manager.list() 

    # obtain service server uri
    with open(pathlib.Path(__file__).parent / "settings.json", "r+") as file:
        data = json.load(file)
        service_uri = data['service_uri']

    for _ in range(NUM_WORKERS):
        p = Process(target=worker_loop, args=(service_uri, task_queue, filtered))
        p.start()
        workers.append(p)

    daemon = Pyro4.Daemon()
    uri = daemon.register(InsultFilter(task_queue, filtered))

    # registers filter uri in settings file
    with open(pathlib.Path(__file__).parent / "settings.json", "r+") as file:
        data = json.load(file)
        data['filter_uri'] = str(uri)
        file.seek(0)
        json.dump(data, file, indent=4)
        file.truncate()

    # Run the server's main loop
    print("InsultFilter Pyro running")

    signal.signal(signal.SIGINT, shutdown_server)
    
    daemon.requestLoop()