
import socket
HOST = '127.0.0.1' # Standard loopback interface address (localhost)
PORT = 2333 # Port to listen on (non-privileged ports are > 1023)

client = socket.socket(socket.AF_INET,socket.SOCK_STREAM)  #connectting to the server
client.connect((HOST,PORT))

def GetKV(key):   #this is a function that will get the value from the server
    msg = "GET "+ key
    client.send(msg.encode("utf-8")) #sends a message to server to indicate wanting a key value
    print(client.recv(1024).decode("utf-8")) #recieves the value
print("\n\n Getting keys in Bob")
GetKV("key")  #the key will have the value that is the rewrote one
GetKV("password")  #this will be longsword
GetKV("username")  #there is no username
GetKV("key1")       #more values to get and see what they are. added to test a bunch of values
GetKV("key2")   
GetKV("key3")
GetKV("key4")
GetKV("key5")  #getting values key5, key 6, key7, and key 8 bu they do not have a value
GetKV("key6")
GetKV("key7")
GetKV("key8")

client.send((" disconnect".encode("utf-8"))) #disconnecting from the server