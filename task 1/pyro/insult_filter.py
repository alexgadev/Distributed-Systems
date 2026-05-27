import Pyro4

from multiprocessing import Process, Queue, Manager

import sys, signal, json

NUM_WORKERS = 1
INSULTS = ["idiot", "stupid", "nerd"]

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

def worker_loop(task_queue, filtered):
    while True:
        text = task_queue.get() # blocks until new job
        for insult in INSULTS:
            if insult in text:
                result = text.replace(insult, "CENSORED")
        filtered.append(result)

#def shutdown_server(daemon):
#    daemon.shutdown()
#    for worker in workers:
#        if worker.is_alive():
#            worker.terminate()
#            worker.join()
#    sys.exit(0)


if __name__ == "__main__":
    task_queue = Queue()
    manager = Manager()
    filtered = manager.list() 

    for _ in range(NUM_WORKERS):
        p = Process(target=worker_loop, args=(task_queue, filtered))
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
    #print("InsultFilter PyRO URI: ", uri)

    #signal.signal(signal.SIGINT, shutdown_server(daemon))
    
    daemon.requestLoop()