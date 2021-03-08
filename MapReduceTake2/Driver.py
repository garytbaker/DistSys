from Master import *

def inverseIndexMapper(inputDict):      #simple function to do inverse index
    #print("inputDict: ")
    #print(inputDict)
    #print("\n")
    
    wordCount = dict()
    for key in inputDict:
        data = inputDict[key]
        data.replace("\n", " ")
        data.replace(".", " ")
        data.replace(",", " ")
        data.replace("!", " ")
        data.replace("?", " ")
        data.replace("\"", " ")
        data.replace("/", " ")
        data.replace(":", " ")
        data.replace(";", " ")
        differentWords = data.split()
        for word in differentWords:
            if word not in wordCount:
                wordCount[word] = [key,1]
           #     print(word + ":" + str(wordCount[word])+"\n")
            else:
                wordCount[word][1] += 1
           #     print(word + ":" + str(wordCount[word])+"\n")
        return wordCount

def inverseIndexReducer(inputList): #reduces inverse index appropriately
    #print("inputList: ")
    #print(inputList)
    #print("\n")
#    print("inputfile: ")
 #   print(inputFile)
    ReverseIndexdict = {}
    output = ""
    for dictionary in inputList:
        for key in dictionary:
            if key in ReverseIndexdict:
                ReverseIndexdict[key].append(dictionary[key])
            else:
                ReverseIndexdict[key]=[]
                ReverseIndexdict[key].append(dictionary[key])
    print("ReverseIndexdict:")
    print(ReverseIndexdict)
    print("\n")

    for key in ReverseIndexdict:
        newList = ReverseIndexdict[key]
        pairAsString = "word=" + key + " "
        for item in newList:
            pairAsString = pairAsString + "file=" + item[0]+ " count=" + str(item[1]) +", "
        pairAsString = pairAsString[:-2]
        pairAsString = pairAsString + "\n"
        output = output + pairAsString
    return output    
   
    

def wordCountMapper(inputDict):  #simple function for wordcount
       
    wordCount = dict()
    for key in inputDict:
        data = inputDict[key]
        data.replace("\n", " ")
        data.replace(".", " ")
        data.replace(",", " ")
        data.replace("!", " ")
        data.replace("?", " ")
        data.replace("\"", " ")
        data.replace("/", " ")
        data.replace(":", " ")
        data.replace(";", " ")
        differentWords = data.split()
        for word in differentWords:
            if word not in wordCount:
                wordCount[word] = 1
                print(word + ":" + str(wordCount[word])+"\n")
            else:
                wordCount[word] += 1
                print(word + ":" + str(wordCount[word])+"\n")
        return wordCount
    

def wordCountReducer(inputList):  #reduces wordcount appropriately
    #print("inputfile: ")
    #print(inputFile)
 
    wordCountdict = {}
    for dictionary in inputList:
        for key in dictionary:
            if key in wordCountdict:
                wordCountdict[key] += dictionary[key]
                print(key + ":" + str(wordCountdict[key])+"\n")
            else:
                wordCountdict[key] = dictionary[key]
                print(key + ":" + str(wordCountdict[key])+"\n")
    output = ""
    for dictkey in wordCountdict:

        pairAsString = "word=" + dictkey+ "   count=" + str(wordCountdict[dictkey]) + "\n"
        #print("pair : " +pairAsString)
        output = output + pairAsString 
    return output


if __name__ == "__main__":
    inputFiles = ["test4","test5"]
    MapReduce(inputFiles,2,2,inverseIndexMapper,inverseIndexReducer,"SimpleInverseIndex2M2R.txt")
    time.sleep(10)
    MapReduce(inputFiles,2,2,wordCountMapper,wordCountReducer,"SimpleWordCount2M2R.txt")

    time.sleep(10)
    inputFiles2 = ["test1","test2","test3",] 
    MapReduce(inputFiles2,3,2,inverseIndexMapper,inverseIndexReducer,"InverseIndex3M2R.txt")
    time.sleep(15)
    MapReduce(inputFiles2,3,2,wordCountMapper,wordCountReducer,"WordCount3M2R.txt")

    MapReduce(inputFiles2,2,3,inverseIndexMapper,inverseIndexReducer,"InverseIndex2M3R.txt")
    time.sleep(20)
    MapReduce(inputFiles2,2,3,wordCountMapper,wordCountReducer,"WordCount2M3R.txt")
    time.sleep(20)

    MapReduce(inputFiles2,4,4,inverseIndexMapper,inverseIndexReducer,"InverseIndex4M4R.txt")
    time.sleep(20)
    MapReduce(inputFiles2,4,4,wordCountMapper,wordCountReducer,"WordCount4M4R.txt")
    time.sleep(20)