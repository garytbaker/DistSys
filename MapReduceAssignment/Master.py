from MapReduce import *

def inverseIndexMapper(inputFile):
    
    punctuation = '.!?",'
    f = open(inputFile,"r")
    wordCount = dict()
    output = ""

    for line in f:
      #  print(line)
        line =line.strip()
        line = line.strip(punctuation)
        line = line.lower()
        #print("filtered: ")
        #print(line)
        differentWords = line.split(" ")
        for word in differentWords:
            if word not in wordCount:
                wordCount[word] = 1
            else:
                wordCount[word] += 1
    f.close()
    for word in wordCount:
        output = output+ "key=" + word + "   value=" +inputFile+":"+str(wordCount[word])+ "\n"  
    print("output: " + output)  
    return output

def inverseIndexReducer(inputFile):
#    print("inputfile: ")
 #   print(inputFile)
    f = open(inputFile,"r")
    output = f.read()
    f.close()
#    print("output: " + output)  
    return output

def wordCountMapper(inputFile):
    
    punctuation = '.!?",'
    f = open(inputFile,"r")
    wordCount = dict()
    output = ""

    for line in f:
      #  print(line)
        line =line.strip()
        line = line.strip(punctuation)
        line = line.lower()
        #print("filtered: ")
        #print(line)
        differentWords = line.split(" ")
        for word in differentWords:
            if word not in wordCount:
                wordCount[word] = 1
            else:
                wordCount[word] += 1
    f.close()
    for word in wordCount:
        output = output+ "key=" + word + "   value="+str(wordCount[word])+ "\n"  
#    print("output: " + output)  
    return output

def wordCountReducer(inputFile):
    #print("inputfile: ")
    #print(inputFile)
    f = open(inputFile,"r")
    wordCountdict = {}
    for line in f:
        line = line.strip()
        if line =="":
            continue
        pairs = line.split("   ")
        key = pairs[0].split("=")
        key = key[1]
        value = pairs[1].split("=")
        value = value[1]
        value = value.split(", ")
   #     print("value =")
  #      print(value)
        for x in value:
            if key in wordCountdict:
                wordCountdict[key] = wordCountdict[key] + int(x) 
            else:
                wordCountdict[key] = int(x)
 #           print("wordCOuntdict" + key + ":"+ str(wordCountdict[key]))
#        print(wordCountdict)
    output = ""
    for dictkey in wordCountdict:

        pairAsString = "key=" + dictkey+ "   value=" + str(wordCountdict[dictkey]) + "\n"
        #print("pair : " +pairAsString)
        output = output + pairAsString
    f.close()
    print("output: " + output)  
    return output


if __name__ == "__main__":
    inverseIndexCluster1 = MapAndReduce(2,2)
    files = ["MapReduceAssignment/test1","MapReduceAssignment/test2"]
    inverseIndexCluster1.runMapandReduce(files,inverseIndexMapper,inverseIndexReducer,"./MapReduceAssignment/mapReduceSameNumberOfNodesOutputRI.txt")

    inverseIndexCluster2 = MapAndReduce(2,3)
    files = ["MapReduceAssignment/test1","MapReduceAssignment/test2","MapReduceAssignment/test3"]
    inverseIndexCluster2.runMapandReduce(files,inverseIndexMapper,inverseIndexReducer,"./MapReduceAssignment/mapReduceMoreReducersOutputRI.txt")

    inverseIndexCluster3 = MapAndReduce(3,2)
    files = ["MapReduceAssignment/test1","MapReduceAssignment/test2","MapReduceAssignment/test3"]
    inverseIndexCluster3.runMapandReduce(files,inverseIndexMapper,inverseIndexReducer,"./MapReduceAssignment/mapReduceMoreMappersOutputRI.txt")

    inverseIndexCluster4 = MapAndReduce(4,3)
    files = ["MapReduceAssignment/test1","MapReduceAssignment/test2","MapReduceAssignment/test3"]
    inverseIndexCluster4.runMapandReduce(files,inverseIndexMapper,inverseIndexReducer,"./MapReduceAssignment/mapReduceExtraReducersOutputRI.txt")

    inverseIndexCluster5 = MapAndReduce(3,4)
    files = ["MapReduceAssignment/test1","MapReduceAssignment/test2","MapReduceAssignment/test3"]
    inverseIndexCluster5.runMapandReduce(files,inverseIndexMapper,inverseIndexReducer,"./MapReduceAssignment/mapReduceExtraMappersOutputRI.txt")

    inverseIndexCluster6 = MapAndReduce(4,4)
    files = ["MapReduceAssignment/test1","MapReduceAssignment/test2","MapReduceAssignment/test3"]
    inverseIndexCluster6.runMapandReduce(files,inverseIndexMapper,inverseIndexReducer,"./MapReduceAssignment/mapReduceExtraBothOutputRI.txt")


    inverseIndexCluster7 = MapAndReduce(2,2)
    files = ["MapReduceAssignment/test1","MapReduceAssignment/test2"]
    inverseIndexCluster7.runMapandReduce(files,wordCountMapper,wordCountReducer,"./MapReduceAssignment/mapReduce2M2ROutputWC.txt")

    inverseIndexCluster8 = MapAndReduce(2,3)
    files = ["MapReduceAssignment/test1","MapReduceAssignment/test2","MapReduceAssignment/test3"]
    inverseIndexCluster8.runMapandReduce(files,wordCountMapper,wordCountReducer,"./MapReduceAssignment/mapReduce2M3ROutputWC.txt")

    inverseIndexCluster9 = MapAndReduce(3,2)
    files = ["MapReduceAssignment/test1","MapReduceAssignment/test2","MapReduceAssignment/test3"]
    inverseIndexCluster9.runMapandReduce(files,wordCountMapper,wordCountReducer,"./MapReduceAssignment/mapReduce3M2ROutputWC.txt")

    inverseIndexCluster10 = MapAndReduce(4,3)
    files = ["MapReduceAssignment/test1","MapReduceAssignment/test2","MapReduceAssignment/test3"]
    inverseIndexCluster10.runMapandReduce(files,wordCountMapper,wordCountReducer,"./MapReduceAssignment/mapReduce4M3ROutputWC.txt")

    inverseIndexCluster11 = MapAndReduce(3,4)
    files = ["MapReduceAssignment/test1","MapReduceAssignment/test2","MapReduceAssignment/test3"]
    inverseIndexCluster11.runMapandReduce(files,wordCountMapper,wordCountReducer,"./MapReduceAssignment/mapReduce3M4ROutputWC.txt")

    inverseIndexCluster12 = MapAndReduce(4,4)
    files = ["MapReduceAssignment/test1","MapReduceAssignment/test2","MapReduceAssignment/test3"]
    inverseIndexCluster12.runMapandReduce(files,wordCountMapper,wordCountReducer,"./MapReduceAssignment/mapReduce4M4ROutputWC.txt")