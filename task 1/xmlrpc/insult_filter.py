import sys
import signal

from multiprocessing import Process, Queue, Manager

import xmlrpc.client

from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler


NUM_WORKERS = 1
workers = []


def worker_loop(task_queue, filtered):
    """Main loop of each worker where they will wait for a job to arrive, get the 
        current list of insults and replace every insult for a CENSORED

    Parameters
    ----------
    task_queue : Queue()
        a queue containing all the jobs to be done
    filtered : Manager().list()
        a list of filtered texts
    """

    url = "http://localhost:8000" # connection to the service to retrieve updated list of insults
    while True:
        text = task_queue.get() # blocks until new job
        for insult in xmlrpc.client.ServerProxy(url).get_insults():
            if insult in text:
                text = text.replace(insult, "CENSORED")
        filtered.append(text)

def shutdown_server(signum, frame):
    """Shuts down the worker processes and exits execution of the filter
    """

    for worker in workers:
        if worker.is_alive():
            worker.terminate()
            worker.join()
    sys.exit(0)

class RequestHandler(SimpleXMLRPCRequestHandler):
    """
    XMLRPC Request handler
    """
    rpc_paths = ('/RPC2',)

class InsultFilter:
    """
    Class that provides the funcionalities needed to submit texts to a work queue to be censored


    Attributes
    ----------
    task_queue : Queue()
        a queue containing all the jobs to be done
    filtered : Manager().list()
        a list of filtered texts

    Methods
    -------
    submit_text(text) -> str:
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

        self.task_queue.put(text) # producer-made job into the queue
        return text

    def get_filtered(self):
        """Gets the filtered text list
        """

        return list(self.filtered)


if __name__ == "__main__":
    task_queue = Queue()
    manager = Manager()
    filtered = manager.list() # avoid loss of result through the worker process

    for _ in range(NUM_WORKERS):
        p = Process(target=worker_loop, args=(task_queue, filtered))
        p.start()
        workers.append(p)

    with SimpleXMLRPCServer(('localhost', 8001),
                            requestHandler=RequestHandler) as server:
        server.register_instance(InsultFilter(task_queue, filtered))

        # Run the server's main loop
        print("InsultFilter XMLRPC running on port 8001")

        signal.signal(signal.SIGINT, shutdown_server) # handle workers' processes when closing the server

        server.serve_forever()