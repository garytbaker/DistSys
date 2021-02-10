import socket
import threading
HOST = '127.0.0.1' # Standard loopback interface address (localhost)
PORT = 2333 # Port to listen on (non-privileged ports are > 1023)
KVStore = {}

def handleClient(conn,addr):
    stillConnected=True
    while stillConnected:
        data = conn.recv(1024).decode("utf-8")
        if(data):
            if(data == "disconnect" ):
                stillConnected = False
            if(data[0:5] == "STORE"):
                parsedDataStore = data.split("STORE")
                parsedDataStore.pop(0)
                for pair in parsedDataStore:
                    pair = pair[1:]
                    KVStore[pair.split("=")[0]]=pair.split("=")[1]
            if(data[0:3] == "GET"):
                parsedDataGet = data.split("GET")
                parsedDataGet.pop(0)
                
                for key in parsedDataGet:
                    key = key[1:]
                    if key in KVStore.keys():
                        msg = key +":"+KVStore[key]
                        conn.send(msg.encode("utf-8"))
                    else:
                        msg = key +": Key Value Pair does not exist"
                        conn.send(msg.encode("utf-8"))

    conn.close



with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    while True:
        conn, addr = s.accept()
 
            
            #Add operation here, if else statement to check input key values
            
        thread = threading.Thread(target=handleClient,args=(conn,addr))
        thread.start()