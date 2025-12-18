import Pyro4
import random
import time
import threading

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
    
    def start_broadcast(self):
        def run():
            while self.running:
                if self.insults:
                    print("[PYRO] Broadcasting: ", random.choice(self.insults))
                time.sleep(5)
        threading.Thread(target=run, daemon=True).start()
        return True
    
#Daemon
daemon = Pyro4.Daemon()
uri = daemon.register(InsultService)
print("InsultService PyRO URI: ", uri)

daemon.requestLoop()