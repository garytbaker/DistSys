
import socket
import threading
from multiprocessing import Process
import time
import pickle
import sys

HOST = '127.0.0.1' # Standard loopback interface address (localhost)
PORT = 2333 # Port to listen on (non-privileged ports are > 1023)
NEWPORT = 2334
KVStore = {} #this is the Key Value storage
mappersFinished = 0
reducersFinished = 0
sentToReducers = 0
output = ""
processes = []
def handleMappers(conn,addr): #This is a function to handle the what happens when a client connects to the server 
    global mappersFinished,sentToReducers
    data = conn.recv(1024).decode("utf-8")  #decode the data
    if data == "mapperDone":
        mappersFinished += 1
    if data == "sentToReducers":
        sentToReducers +=1


def handleReducers(conn,addr): #This is a function to handle the what happens when a client connects to the server 
    global output,reducersFinished
    codedData = b"" #decode the data
    while True:
        bits= conn.recv(4096) #decode the data
        if not bits:
            break
        codedData += bits
    newData = pickle.loads(codedData)
    data = newData["data"]
    reducersFinished += 1
    print("reducerFinished:" +str(reducersFinished))
    output += data

def MapReduce(inputFiles,numMappers,numReducers,mapFunc,reduceFunc,OutputFile): #this is the main function
    global NEWPORT #port number to be used with the new processes
    mappers = [] #lists of data
    reducers = []
    filesToMappers = []
    for _ in range(numMappers):   #the next two loops evenly seperate dat for the mappers
        filesToMappers.append(dict())
    print(filesToMappers)
    fileIndex = 0
    for ifile in inputFiles:
        currFile = open(ifile,"r")
        print(fileIndex)
        filesToMappers[fileIndex][ifile] = currFile.read()
        currFile.close()
        fileIndex +=1
        fileIndex =fileIndex %numMappers
    
    for i in range(numMappers):              #starting the mappers servers
        mappers.append(NEWPORT)
        print("multiProcessing")
        newp = Process(target=createMapper, args = (NEWPORT,))
        newp.start()
        processes.append(newp)
        NEWPORT += 1

    for i in range(numReducers):           #starting the reducers servers
        reducers.append(NEWPORT)
        
        newp = Process(target=createReducer, args=(NEWPORT,))
        newp.start()
        processes.append(newp)
        NEWPORT +=1
   
    print("mappers : " + str(mappers) + "\n")    
    print("reducers : " + str(reducers) + "\n")

    time.sleep(1)

    for i in range(numMappers):    #sending the data to the mappers
        messageDict = {"reducers": reducers,"mapFunc":mapFunc,"reduceFunc":reduceFunc,"data":filesToMappers[i],"numMappers":numMappers}
        msg = pickle.dumps(messageDict)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: #starting the server
            print("mappers[i] : "+ str(mappers[i])+ "\n")
            s.connect((HOST,mappers[i]))
            s.send(msg)
        
            
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: #starting the server
        s.bind((HOST, PORT))
        s.listen() #listens for clients
        print("Starting server")

        while mappersFinished<numMappers:           #waiting for the mappers to finish
            conn, addr = s.accept()  
    
                
                 
                
            thread = threading.Thread(target=handleMappers,args=(conn,addr)) #using threading to handle clients
            thread.start()
            time.sleep(.05)

    for i in range(numMappers):      #signaling the mappers to send to the reducers
        msg = "SendToReducers"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: #starting the server
            s.connect((HOST,mappers[i]))
            s.send((msg).encode("utf-8"))

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: #starting the server
        s.bind((HOST, PORT))
        s.listen() #listens for clients
        print("Starting server")

        while sentToReducers<numMappers:                                #waiting for mappers to say they have sent to the server
            conn, addr = s.accept()  
    
                
                 
                
            thread = threading.Thread(target=handleMappers,args=(conn,addr)) #using threading to handle clients
            thread.start()
            time.sleep(.05)

    for i in range(numReducers):                                    #signalling to the reducers to start
        msg = "start"
        startmsg = pickle.dumps(msg)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: #starting the server
            s.connect((HOST,reducers[i]))
            s.send((startmsg))

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: #starting the server
        s.bind((HOST, PORT))
        s.listen() #listens for clients
        print("Starting server")

        while reducersFinished<numReducers:                     #waiting for the reducers to finish
            conn, addr = s.accept()  
    
                
                 
            print("handleReducer")   
            thread = threading.Thread(target=handleReducers,args=(conn,addr)) #using threading to handle clients
            thread.start()
            time.sleep(.05)
    finalFile = open(OutputFile,"w")            #aggregating the data into the specified file
    finalFile.write(output)
    finalFile.close()
    
    
    for p in processes:                                     #terminates the servers
        p.terminate()
    reset()                                                 #resets the data for the next run

def reset():
    global NEWPORT, KVStore,mappersFinished,reducersFinished,sentToReducers,output,processes
    NEWPORT = 2334
    KVStore = {} 
    mappersFinished = 0
    reducersFinished = 0
    sentToReducers = 0
    output = ""
    processes = []    

def createMapper(portNum):   #function used to create a mapper in a process
    print("making mappers")
    sys.stdout.flush()
    newMapper = mapper(portNum)
    while newMapper != None:
        pass

def createReducer(portNum):  #function used to create a reducer in a process
    newReducer = reducer(portNum)
    while newReducer !=None:
        pass

class mapper:                       #this is the mapper class
    def __init__(self,portNum):
        print("mapper initialized\n")
        sys.stdout.flush()
        self.portNum = portNum              #set base values
        self.dataRecieved = False
        self.waitingForSignalToSendToReducers = True
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: #starting the server
            s.bind((HOST, self.portNum))
            s.listen() #listens for clients
            print("Starting mapper")
            sys.stdout.flush()

            while not self.dataRecieved:            #while waiting for the data
                conn, addr = s.accept()  
        
                    
                     
                    
                self.recieveData(conn,addr)         #how to recieve the data
                time.sleep(.05)
            s.close()
            print("outsideOfLoop\n")
            sys.stdout.flush()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: #starting the server
            s.connect((HOST,PORT))
            print("sendingmapperDone\n")                        #signal to the master done mapping
            sys.stdout.flush()
            s.send(("mapperDone").encode("utf-8"))
            s.close()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: #starting the server
            s.bind((HOST, self.portNum))
            s.listen() #listens for clients

            while self.waitingForSignalToSendToReducers:                #wait for master to signal to send to the reducers
                conn, addr = s.accept()  
                sendMsg = (conn.recv(4096).decode("utf-8"))
                if sendMsg == "SendToReducers":
                    print("sendingToReducers\n")
                    sys.stdout.flush()
                    self.sendToReducers()


    def recieveData(self,conn,args):                #function for recieving data
        print("mapperRecievedData\n")
        sys.stdout.flush()
        codedData = b""                             #getting pickle data
        while True:
            bits= conn.recv(4096) #decode the data
            if not bits:
                break
            codedData += bits
        msgFromMaster = pickle.loads(codedData)
        self.reducers = msgFromMaster["reducers"]       #setting values
        self.mapFunc = msgFromMaster["mapFunc"]
        self.reduceFunc = msgFromMaster["reduceFunc"]
        self.data = msgFromMaster["data"]
        self.numMappers = msgFromMaster["numMappers"]
        self.startMapper()
        self.dataRecieved = True
        
    def startMapper(self):                          #actually mapping
        print("startMapper\n")
        print(len(self.reducers))
        sys.stdout.flush()
        
        self.MapperKVStore = self.mapFunc(self.data)
        if self.MapperKVStore ==None:               #if there was no data
            pass
        else:                                       #what is done most of the time
            self.sendingKVStore = dict()
            for i in range(len(self.reducers)):
                self.sendingKVStore[i] = dict()
            for key in self.MapperKVStore:
                reducerNumber = len(key) % len(self.reducers)
                self.sendingKVStore[reducerNumber][key] = self.MapperKVStore[key]   #shuffles at the end of mapping

    def sendToReducers(self):                          #function to send to the reducers
        print("sendingToReducers\n")
        sys.stdout.flush()
        if self.MapperKVStore != None:                  #again checking if there is no data to this mapper
            for i in range(len(self.reducers)):            #sends data to the correct server
                sendDict = {"reduceFunc":self.reduceFunc,"data":self.sendingKVStore[i],"numMappers":self.numMappers}
                msg = pickle.dumps(sendDict)
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: #starting the server
                    s.connect((HOST,self.reducers[i]))
                    s.send(msg)
                    s.close()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: #starting the server
            s.connect((HOST,PORT))
            s.send(("sentToReducers").encode("utf-8"))          #signals to master done sending
            s.close()
        del(self)


class reducer:                                              #this is the reducer class
    def __init__(self,portNum):
        self.portNum = portNum                              #setting base values
        self.dataRecieved = 0
        self.inputs = []
        self.numMappers = 1000
        self.startBool = False
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: #starting the server
            s.bind((HOST, self.portNum))
            s.listen() #listens for clients
            print("Starting reducer")
            sys.stdout.flush()
            while not self.startBool:                       #accepts data from mappers while waiting for signal from reducers
                conn, addr = s.accept()  
    
                    
                self.recieveData(conn,addr)
                
            s.close()
        self.startReducer()             #starting the reducers
        self.sendToMaster()             #sending data to the master
    
    def recieveData(self,conn,args):        #handles data
        print("reducerRecieveData")
        sys.stdout.flush()
        codedData = b"" #decode the data
        while True:
            bits= conn.recv(4096) #decode the data
            if not bits:
                break
            codedData += bits
        msgFromMapper = pickle.loads(codedData)
        if msgFromMapper =="start":                                 #if it was signalled starts reducing
            self.startBool = True
        else:                                                       #otherwise data is from mapper and it stores it appropriately
            self.numMappers = msgFromMapper["numMappers"]
            self.reduceFunc = msgFromMapper["reduceFunc"]
            newData = msgFromMapper["data"]
            self.inputs.append(newData)
            self.dataRecieved +=1           

    def startReducer(self):                                         #reduces here
        time.sleep(2)
        print("reducerstartReducer")
        sys.stdout.flush()
        self.finaldata = self.reduceFunc(self.inputs)

    def sendToMaster(self):                                             #sends data to the master
        print("reducerSendToMaster")
        sys.stdout.flush()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: #starting the server
            s.connect((HOST,PORT))
            dataToMaster = dict()
            dataToMaster["data"] = self.finaldata
            msgToMaster = pickle.dumps(dataToMaster)
            s.send(msgToMaster)
            s.close()
        del(self)