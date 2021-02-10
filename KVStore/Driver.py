import os
import threading
import time

server = "python Server.py"
alice = "python Alice.py"
bob = "python Bob.py"

thread = threading.Thread(target=os.system,args=(server,)) #using threading to handle clients
thread.start()
time.sleep(.2)
thread = threading.Thread(target=os.system,args=(alice,)) #using threading to handle clients
thread.start()
time.sleep(.2)
thread = threading.Thread(target=os.system,args=(bob,)) #using threading to handle clients
thread.start()