
import socket
HOST = '127.0.0.1' # Standard loopback interface address (localhost)
PORT = 2333 # Port to listen on (non-privileged ports are > 1023)

client = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
client.connect((HOST,PORT))

client.send(("GET key").encode("utf-8"))
print(client.recv(1024).decode("utf-8"))
client.send(("GET Password").encode("utf-8"))
print(client.recv(1024).decode("utf-8"))
client.send(("GET Username").encode("utf-8"))
print(client.recv(1024).decode("utf-8"))