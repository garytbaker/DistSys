 
import socket
HOST = '127.0.0.1' # Standard loopback interface address (localhost)
PORT = 2333 # Port to listen on (non-privileged ports are > 1023)

client = socket.socket(socket.AF_INET,socket.SOCK_STREAM) #connecting to the server
client.connect((HOST,PORT))



def sendKV(key , value):   #this sends a key value pair to the server
    msg = "STORE " + key + "=" + value
    client.send((msg).encode("utf-8"))

print("STORING THE FOLLOWING KEYS FROM ALICE\nkey:value\npassword:Longsword\nkey:ADifferentValue\nkey1:Value1\nkey2:Value2\nkey3:Value3\nkey4:Value4")
sendKV("key","value")#sends a bunch of key value pairs to the server
sendKV("password","Longsword")
sendKV("key","ADifferentValue")  #what happens when you send a value to the same key it should overwrite it
sendKV("key1","value1")
sendKV("key2","value2")
sendKV("key3","value3")
sendKV("key4","value4")
client.send((" disconnect".encode("utf-8"))) #disconnecting from the server