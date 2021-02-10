import socket
import threading
HOST = '127.0.0.1' # Standard loopback interface address (localhost)
PORT = 2333 # Port to listen on (non-privileged ports are > 1023)
KVStore = {} #this is the Key Value storage

def handleClient(conn,addr): #This is a function to handle the what happens when a client connects to the server 
    stillConnected=True  
    while stillConnected: #while they are connected
        data = conn.recv(1024).decode("utf-8")  #decode the data
        if(data): #if there is data
            if(data == "disconnect" ):  #disconnect from the server if the client requests
                stillConnected = False
            if(data[0:5] == "STORE"): #if they want to store a pair
                parsedDataStore = data.split("STORE") #get all the key value pairs
                parsedDataStore.pop(0) #remove the empty one
                for pair in parsedDataStore: 
                    pair = pair[1:]   #remove the spaces
                    if len(pair.split()) >1: # if there is a space and 
                        if pair.split()[1] == "disconnect": #the user wants to disconnect
                            pair = pair.split()[0] #add this last KV pair
                            KVStore[pair.split("=")[0]]=pair.split("=")[1] #add the pairs to the dictionary
                            stillConnected = False  
                    else:
                        KVStore[pair.split("=")[0]]=pair.split("=")[1] #add the pairs to the dictionary
            print("keys after alice stores: ")
            print(KVStore)
            if(data[0:3] == "GET"): #if the client is getting a pair
                parsedDataGet = data.split("GET")#get the keys they want to get
                parsedDataGet.pop(0)#remove the empty first item
                
                for key in parsedDataGet: #for each key
                    key = key[1:] #remove the space at the start
                    if key in KVStore.keys(): #if the key is in the store
                        msg = key +":"+KVStore[key]
                        conn.send(msg.encode("utf-8")) #send the key value pair back
                    else:
                        msg = key +": Key Value Pair does not exist" #otherwise say it does not exist
                        conn.send(msg.encode("utf-8"))


    conn.close



with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: #starting the server
    s.bind((HOST, PORT))
    s.listen() #listens for clients
    print("Starting server")
    while True:
        conn, addr = s.accept() #waits for clients
 
            
            #Add operation here, if else statement to check input key values
            
        thread = threading.Thread(target=handleClient,args=(conn,addr)) #using threading to handle clients
        thread.start()