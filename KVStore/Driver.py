import os
import threading
import time

server = "python KVStore/Server.py"
alice = "python KVStore/Alice.py"
bob = "python KVStore/Bob.py"

thread = threading.Thread(target=os.system,args=(server,)) #using threading to handle clients
thread.start()
time.sleep(.2)
thread = threading.Thread(target=os.system,args=(alice,)) #using threading to handle clients
thread.start()
time.sleep(.2)
thread = threading.Thread(target=os.system,args=(bob,)) #using threading to handle clients
thread.start()
