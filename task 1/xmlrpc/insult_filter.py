from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler

from multiprocessing import Process, Queue, Manager

import sys, signal, os

import xmlrpc.client

NUM_WORKERS = int(os.environ.get("INSULT_FILTER_WORKERS", "1"))
INSULTS = ["idiot", "stupid", "nerd"]

workers = []


class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)


class InsultFilter:
    def __init__(self, task_queue, filtered):
        self.task_queue = task_queue
        self.filtered = filtered

    def submit_text(self, text):
        self.task_queue.put(text) # producer-made job into the queue
        return text

    def get_filtered(self):
        return list(self.filtered)
    
def worker_loop(task_queue, filtered):
    svc_port = int(os.environ.get("XMLRPC_SERVICE_PORT", "8000"))
    url = f"http://localhost:{svc_port}"
    while True:
        text = task_queue.get() # blocks until new job

        for insult in xmlrpc.client.ServerProxy(url).get_insults():
            if insult in text:
                text = text.replace(insult, "CENSORED")
        filtered.append(text)

def shutdown_server(signum, frame):
    for worker in workers:
        if worker.is_alive():
            worker.terminate()
            worker.join()
    sys.exit(0)

if __name__ == "__main__":
    task_queue = Queue()
    manager = Manager()
    filtered = manager.list() # avoid loss of result through the worker process

    for _ in range(NUM_WORKERS):
        p = Process(target=worker_loop, args=(task_queue, filtered))
        p.start()
        workers.append(p)

    flt_port = int(os.environ.get("XMLRPC_FILTER_PORT", "8001"))
    with SimpleXMLRPCServer(('localhost', flt_port),
                            requestHandler=RequestHandler) as server:
        server.register_instance(InsultFilter(task_queue, filtered))

        # Run the server's main loop
        print(f"InsultFilter XMLRPC running on port {flt_port}")

        signal.signal(signal.SIGINT, shutdown_server) # handle workers' processes when closing the server

        server.serve_forever()