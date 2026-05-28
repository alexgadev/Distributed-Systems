import Pyro4

from multiprocessing import Process, Queue, Manager

import sys, signal, json

NUM_WORKERS = 1
workers = []

@Pyro4.expose
class InsultFilter:
    def __init__(self, task_queue, filtered):
        self.task_queue = task_queue
        self.filtered = filtered

    def submit_text(self, text):
        self.task_queue.put(text)
        return text
    
    def get_filtered(self):
        return list(self.filtered)

def worker_loop(service_uri, task_queue, filtered):
    while True:
        try:
            text = task_queue.get(block=True) # blocks until new job
        except (KeyboardInterrupt, EOFError):
            break
        for insult in Pyro4.Proxy(service_uri).get_insults():
            if insult in text:
                text = text.replace(insult, "CENSORED")
        filtered.append(text)

def sanitize_closeup():
    with open("task 1/pyro/settings.json", "r+") as file:
        data = json.load(file)
        # empty uri string
        data['filter_uri'] = ""
        # re-store again
        file.seek(0)
        json.dump(data, file, indent=4)
        file.truncate()

def shutdown_server(signum, frame):
    for worker in workers: # terminate all workers
        if worker.is_alive():
            worker.terminate()
            worker.join()
    sanitize_closeup() # clean settings.json
    sys.exit(0)


if __name__ == "__main__":
    task_queue = Queue()
    manager = Manager()
    filtered = manager.list() 

    with open("task 1/pyro/settings.json", "r+") as file:
        data = json.load(file)
        service_uri = data['service_uri']

    for _ in range(NUM_WORKERS):
        p = Process(target=worker_loop, args=(service_uri, task_queue, filtered))
        p.start()
        workers.append(p)

    #Daemon
    daemon = Pyro4.Daemon()
    uri = daemon.register(InsultFilter(task_queue, filtered))
    with open("task 1/pyro/settings.json", "r+") as file:
        data = json.load(file)
        data['filter_uri'] = str(uri)
        file.seek(0)
        json.dump(data, file, indent=4)
        file.truncate()

    signal.signal(signal.SIGINT, shutdown_server)
    
    daemon.requestLoop()