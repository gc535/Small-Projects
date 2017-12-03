#!/usr/bin/python

import getopt
import socket
import sys
import time


host = "127.0.0.1"
port = 8765

def checkNonWhiteSpace(string):
    i = -1
    while (i < len(string)):
        if (string[i] != " "):
           break
        i += 1
    return i

def checkWhiteSpace(string):
    i = -1
    while (i < len(string)):
        if (string[i] == " "):
            break
        i += 1
    return i

def stringThing(into, out):
    i = 0
    for x in into:
        if (into[i] != '\\' and into[i+1] != 'r' and into[i+2] != '\\' and into[i+3] != 'n'):
            out = out + into[i]
    print out

def findEndChar(string):
    i = -1
    while (i < len(string)):
        if (string[i:i+6] == "\r\n.\r\n"):
            break
        i += 1
    return i+6

# handle a single client request
class ConnectionHandler:
    def __init__(self, socket):
        self.socket = socket
        self.state = None
        self.completeMessage = ""
        self.partialMessage = ""
        #self.endMessage = False
        #self.error = False

    def handle(self):
        # Lets the client know a connection has been made
        #self.completeMessage = (self.socket.recv(1024))
        if (self.state == None):
            self.socket.send(b"220 rrg67 SMTP CS4410MP3\r\n")
            self.state = "Open"
        while (True):
            i = 0
            self.socket.settimeout(10)
            self.partialMessage = repr(self.socket.recv(500))
            while (i < len(self.partialMessage)):
                if (self.partialMessage.find('\\r\\n') > 0):
                    self.completeMessage = self.completeMessage + self.partialMessage[i]
            print(self.state)
            # Waiting for a HELO command
            if (self.state == "Open"):
                if (self.completeMessage[1:5] == "HELO"):
                    m = checkNonWhiteSpace(self.completeMessage[5:])
                    if (self.completeMessage[5] != " " and self.completeMessage[5:7] != "\r\n"):
                            self.socket.send(b"500 Error: command not recognized\r\n")
                    elif (self.completeMessage[5] != " "):
                        self.socket.send(b"501 Syntax:  proper syntax\r\n")
                    elif (self.completeMessage[m:m+2] == "\r\n"):
                        self.socket.send(b"501 Syntax:  proper syntax\r\n")
                    else :
                        self.state = "HELO"
                        self.socket.send(b"250 rrg67\r\n")
                        self.state = None
                        self.completeMessage = None
                        #self.partialMessage = None
                        #self.endMessage = False
                else:
                    self.socket.send(b"503 Error: need HELO command\r\n")
                        # Waiting for a MAIL FROM command
            elif (self.state == "HELO"):
                if (self.completeMessage[1:11] == "MAIL FROM:"):
                    o = checkNonWhiteSpace(self.completeMessage[12:])
                    if (self.completeMessage[11] != " " and self.completeMessage[11:13] != '\r\n'):
                       self.socket.send(b"500 Error: command not recognized\r\n")
                    elif (self.completeMessage[11] != " "):
                        self.socket.send(b"501 Syntax:  proper syntax\r\n")
                    elif (o > -1):
                        self.socket.send(b"555 <bad email>: Sender address rejected\r\n")                        
                    elif (checkWhiteSpace(self.completeMessage[o:]) > -1):
                        self.socket.send(b"555 <bad email>: Sender address rejected\r\n")
                    else:
                        self.state = "MAIL FROM"
                        self.socket.send(b"250 OK\r\n")
                        self.state = None
                        self.completeMessage = None
                        #self.partialMessage = None
                        #self.endMessage = False
                elif (self.completeMessage[1:5] == "HELO"):
                    self.socket.send(b"503 Error: duplicate HELO\r\n")
                elif (self.completeMessage[1:9] == "RCPT TO:"):
                    self.socket.send(b"503 Error: need MAIL FROM command\r\n")
                elif (self.completeMessage[1:5] == "DATA"):
                    self.socket.send(b"503 Error: need MAIL FROM command\r\n")
                else:
                    self.socket.send(b"500 Error: command not recognized\r\n")
            # Waiting for a RCPT TO command
            elif (self.state == "MAIL FROM"):
                if (self.completeMessage[1:9] == "RCPT TO:"):
                    o = checkWhiteSpace(self.completeMessage[10:])
                    if (self.completeMessage[9] != " " and self.completeMessage[9:11] != '\r\n'):
                        self.socket.send(b"500 Error: command not recognized\r\n")
                    elif (self.completeMessage[9] != " "):
                        self.socket.send(b"501 Syntax:  proper syntax\r\n")
                    elif (o > -1):
                        self.socket.send(b"555 <bad email>: Recipient address invalid\r\n")
                    elif (checkWhiteSpace(self.completeMessage[o:]) > -1):
                        self.socket.send(b"555 <bad email>: Recipient address invalid\r\n")
                    else: 
                        self.state = "RCPT TO"
                        self.socket.send(b"250 OK\r\n")
                        self.state = None
                        self.completeMessage = None
                        #self.partialMessage = None
                        #self.endMessage = False
                elif (self.completeMessage[1:5] == "HELO"):
                    self.socket.send(b"503 Error: duplicate HELO\r\n")
                elif (self.completeMessage[1:11] == "MAIL FROM:"):
                    self.socket.send(b"503 Error: nested MAIL command\r\n")
                elif (self.completeMessage[1:5] == "DATA"):
                    self.socket.send(b"503 Error: need RCPT TO command\r\n")
                else:
                    self.socket.send(b"500 Error: command not recognized\r\n")
            # Waiting for a DATA command or another RCPT TO command
            elif (self.state == "RCPT TO"):
                print ("state is RCPT TO, looking for a DATA")
                if (self.completeMessage[1:5] == "DATA"):
                    if (self.completeMessage[5] != " " and self.completeMessage[5:7] != '\r\n'):
                        self.socket.send(b"500 Error: command not recognized\r\n")
                    elif (self.completeMessage[5] != " "):
                        self.socket.send(b"501 Syntax: proper syntax\r\n")
                    else: 
                        self.state = "DATA"
                        self.socket.send(b"250 OK\r\n")
                        self.state = None
                        self.completeMessage = None
                        #self.partialMessage = None
                        #self.endMessage = False
                elif (self.completeMessage[1:9] == "RCPT TO:\r\n"):
                    o = checkNonWhiteSpace(self.completeMessage[10:])
                    if (self.completeMessage[9] != " " and self.completeMessage[9:13] != '\r\n'):
                        self.socket.send(b"500 Error: command not recognized\r\n")
                    elif (self.completeMessage[9] != " "):
                        self.socket.send(b"501 Syntax:  proper syntax\r\n")
                    elif (o > -1):
                        self.socket.send(b"555 <bad email>: Recipient address invalid\r\n")
                    elif (checkWhiteSpace(self.completeMessage[o:]) > -1):
                        self.socket.send(b"555 <bad email>: Recipient address invalid\r\n")
                    else: 
                        self.state = "RCPT TO"
                        self.socket.send(b"250 OK\r\n")
                elif (self.completeMessage[1:5] == "HELO"):
                    self.socket.send(b"503 Error: duplicate HELO\r\n")
                elif (self.completeMessage[1:11] == "MAIL FROM:"):
                    self.socket.send(b"503 Error: nested MAIL command\r\n")
                else:
                    self.socket.send(b"500 Error: command not recognized\r\n")
                # Waiting for content
            elif (self.state == "DATA"):
                self.state = "354"
                self.socket.send(b"354 End data with <CR><LF>.<CR><LF>")
                #self.completeMessage = None
                #self.partialMessage = None
                self.endMessage = False
            elif (self.state == "354"):
                c = findEndChar(self.completeMessage)
                print(self.completeMessage[:c])
                print(self.completeMessage[c:])
                self.socket.send(b"bean250 OK:  delivered message 1")
                self.state = "end"
            elif (self.state == "end"):
                self.socket.close()
            else: 
                self.socket.send(b"500 Error: command not recognized")

# the main server loop
def serverloop():
    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # mark the socket so we can rebind quickly to this port number
    # after the socket is closed
    serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # bind the socket to the local loopback IP address and special port
    serversocket.bind((host, port))
    # start listening with a backlog of 5 connections
    serversocket.listen(5)

    while True:
        # accept a connection
        (clientsocket, address) = serversocket.accept()
        ct = ConnectionHandler(clientsocket)
        ct.handle()

# You don't have to change below this line.  You can pass command-line arguments
# -h/--host [IP] -p/--port [PORT] to put your server on a different IP/port.
opts, args = getopt.getopt(sys.argv[1:], 'h:p:', ['host=', 'port='])

for k, v in opts:
    if k in ('-h', '--host'):
        host = v
    if k in ('-p', '--port'):
        port = int(v)

print("Server coming up on %s:%i" % (host, port))
serverloop()
