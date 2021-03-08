import threading
import time
import os
import stat
import socket

class MapAndReduce:
    def __init__(self, numberOfMappers, numberOfReducers):
        self.numberOfMappers = numberOfMappers  #the basic variables
        self.numberOfReducers = numberOfReducers
        self.inputFiles = []   #list that will become a list of list of files evenly distributed for the mappers
        self.mapFunction = 0   #these get instatiated in the runMapAndReduce
        self.reduceFunction = 0  
        self.mapComplete = []  #gets filled as mappers finish. when the len = len mappers, thenn moves to shuffle
        self.reduceComplete = [] # same but with reducers
        self.mappers = []  #list of mappers and reducers
        self.reducers = []
        self.reducerInput = [] #same as inputfiles for the reducers
        self.shuffled = False #to see if we are shuffled yet
        
        for i in range(numberOfMappers): #creating mappers and lists for the input files
            self.mappers.append(Mapper(i))
            self.inputFiles.append([])
        for i in range(numberOfReducers):#creating reducers and input files for them
            self.reducers.append(Reducer(i))
            self.reducerInput.append("./MapReduceAssignment/reducerInput" + str(i))



    def runMapandReduce(self,inputFiles, mapFunction, reduceFunction, outputFile):#Main function for running mapand reduce
        self.outputFile = outputFile #setting the destination
        fileIndex = 0 #counterVariable
       # print(self.inputFiles)
        for ifile in inputFiles: #evenly distributing the files to the mappers
          #  print(fileIndex)
           # print(self.numberOfMappers)
            #print("ifile:" +ifile)
            self.inputFiles[fileIndex].append(ifile)
            fileIndex +=1
            fileIndex = fileIndex % self.numberOfMappers #making sure we are not going over num of mappers
        self.mapFunction = mapFunction #storing the functions
        self.reduceFunction = reduceFunction
       # print("inputfiles: " + str(inputFiles))
        currentMapperIndex = 0 #counter variable
        for mapper in self.mappers:#running the mappers
            print("starting "+mapper.id)
            t1 = threading.Thread(target=mapper.runMapper,args=(self.inputFiles[currentMapperIndex],self.mapFunction,self.mapComplete))#parrallelism
            t1.start()
            time.sleep(.05)
            currentMapperIndex +=1
        while len(self.mapComplete) < len(self.mappers):#waiting for mappers to finish
           # print(str(len(self.mapComplete)) + ":" +str(len(self.mappers)))
            pass
        print("mapped")
        mapperFile = open("./MapReduceAssignment/mapper.txt","w")#combinin mapperfiles into one main mapper file
        for mapper in self.mappers:#combining the mappers
            f = open(mapper.id,"r")
            mapperFile.write(f.read())
            mapperFile.write("\n")
            f.close()
        mapperFile.close()#closing the mapper file
        mapperFile = open("./MapReduceAssignment/mapper.txt","r")
        self.shuffled =self.shuffle(mapperFile)#shuffling the input into reducer ready format
        mapperFile.close()
        while(self.shuffled == False):#when shuffled
            pass
        currentReducerIndex = 0
        print("shuffled")
        for reducer in self.reducers:#run the reducers
            print("index")
            print(self.reducerInput[currentReducerIndex])
            t1 = threading.Thread(target=reducer.runReducer,args=(self.reducerInput[currentReducerIndex],self.reduceFunction,self.reduceComplete))
            t1.start()
            time.sleep(.05)
            currentReducerIndex += 1
        
        while len(self.reduceComplete) < len(self.reducers):#when the reducers are finished
            pass
        
        finalFile = open(self.outputFile,"w")
        for reducer in self.reducers:#combines the reducers output into one file
            f = open(reducer.id,"r")
            finalFile.write(f.read())
            finalFile.write("\n")
            f.close()
        finalFile.close()
        print("finished")


#    sampleLine = "key=fish   value=d1, 2   "
    def shuffle(self, mapperFile):#shuffles the output
        shuffledict = {}
        for line in mapperFile:#for every line
            line = line.strip()#following line parses data
            if line =="":
                continue
            pairs = line.split("   ")
        #    print(pairs)
            key = pairs[0].split("=")
            key = key[1]
      #      print("Key: ")
       #     print(key)
            value = pairs[1].split("=")
            value = value[1]
            #print("key: " + key +" value:")
            #print(value)
            if key in shuffledict:#shuffles the data into key -value pairs with lists as pairs
                #print("key: " + key +" valuebefore:")
               # print(shuffledict[key])
                
                shuffledict[key].extend([value])
                #print("key: " + key +" valueafter:")
                #print(shuffledict[key])
            else:
                shuffledict[key] = [value]
        #print(shuffledict)
        for i in range(self.numberOfReducers):#creates input for reducers
            open( "./MapReduceAssignment/reducerInput" + str(i),"w").close()
            os.chmod('./MapReduceAssignment/reducerInput'+str(i), stat.S_IRWXU|stat.S_IRWXG|stat.S_IRWXO)
        currentReducer = 0
        for dictkey in shuffledict:#turns dictionary into the reducer input
           # print(dictkey)
            inputString = "./MapReduceAssignment/reducerInput" + str(currentReducer)
            pairAsString = "key=" + dictkey+ "   value="
            for item in shuffledict[dictkey]:
              #  print(item)
                pairAsString= pairAsString + item
                pairAsString= pairAsString + ", "
            pairAsString = pairAsString[:-2]
            pairAsString = pairAsString +"\n"
            print(pairAsString)
            
            reducerInput = open(inputString,"a")
            reducerInput.write(pairAsString)
            reducerInput.close()
            currentReducer += 1
            currentReducer = currentReducer % self.numberOfReducers
        return True    #returns true to change self.shuffled    


class Mapper:#this is the mappers
    def __init__(self,id):
        self.id = "./MapReduceAssignment/mapper"+str(id)
        self.inputFiles = 0
        self.mapFunction = 0
        

    def runMapper(self,inputFiles,mapFunction, mapComplete):#runs the mapper
      #  print(self.id)
        self.inputFiles = inputFiles
        self.mapFunction = mapFunction
       # print(self.inputFiles)
        open(self.id,"w").close()
        for page in self.inputFiles:
          #  print(page)
            f = open(self.id,"a")
            f.write(mapFunction(page))#using user-defined functions
            f.close()
        mapComplete.append(self.id)
     #   print(self.id)


class Reducer:#these are the reducers
    def __init__(self,id):
        self.id = "./MapReduceAssignment/reducer"+str(id)
        self.inputFiles = 0
        self.reduceFunction = 0

    def runReducer(self,inputFile,reduceFunction,reduceComplete):#running the reducers
        self.inputFile = inputFile
        self.reduceFunction = reduceFunction
        open(self.id,"w").close()
        f = open(self.id,"a")
        f.write(reduceFunction(inputFile))#user defined functions
        f.close()
        reduceComplete.append(self.id)