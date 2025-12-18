import Pyro4
import time
#from insult_filter import InsultFilter

INSULT_SERVICE_URI = input("Enter InsultService PyRO URI: ")
insult_srv = Pyro4.Proxy(INSULT_SERVICE_URI)
insult_srv.add_insult("idiot")
insult_srv.add_insult("stupid")

print(insult_srv.get_insults())

#insult_srv.start_broadcast()
#time.sleep(1)

#filter_srv = Pyro4.Proxy(Pyro4.Daemon().register(
#    InsultFilter(INSULT_SERVICE_URI)))
#INSULT_FILTER_URI = input("Enter InsultFilter PyRO URI: ")
#filter_srv = Pyro4.Proxy(INSULT_FILTER_URI)
#filter_srv.submit_service_uri(INSULT_SERVICE_URI)
#filter_srv.update_insults()
#print(filter_srv.submit_text("You're such an idiot!"))
#print(filter_srv.get_filtered())