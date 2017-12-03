import socket
import sys
import json
import threading
import os

global data 
global MaxThreads
MaxThreads = 32        # Maximun number of threads allowed to acccess the database

# Loading exsisting database file, or create new one if loading failed
try:
   f = open("db.json", "r")
   s = f.read()
   data = json.loads(s)
   f.close()
except:
   print("No saved database found.\nNew file will be created. Database will be flushed to db.json every 10 operations!\n")
   data = { } 

# Setup the server using options in user input command 
HOST = sys.argv[1]                                                 # user assigned host name 
PORT = int(sys.argv[2])                                            # user assigned socket number
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)              # set connection family and types
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)            # set address reuse option
s.bind((HOST, PORT))


# Handles at most 32 clients(threads) at a time, block new clients untill old one drops out 
class ConnectionHandler(threading.Thread):
    def __init__(self, MaxThreads):
        threading.Thread.__init__(self)
        self.MaxThreads = MaxThreads                     # max capacity of clients 
        self.lock = threading.Lock()                     # conditional variable lock: for client monitor
        self.tcount = 0                                  # client counts (thread counts)
        self.newslot = threading.Condition(self.lock)    # conditional varible: blocks after 32 clients entered
        
        
    def threadPool(self, socket):
        self.newslot.acquire()       
        while self.tcount >= self.MaxThreads:
            self.newslot.wait()                  # wait for new client slots
        self.tcount += 1                         # update client counts
        self.newslot.release()
        while 1:
            socket.settimeout(10)                # set and update timeout at setup or after every input received
            try:
                strings = socket.recv(1024)      # try to recive input if connection is still valid
                while len(strings) > 0:
                    strings = eval_input(strings, socket)   # parsing, checking and operating commands

            except:
                self.newslot.acquire()
                self.tcount += -1               # update client counts when clients drop out
                self.newslot.notify()           # notify a waiting client for the new slot avaliable
                self.newslot.release()
                socket.sendall('connection closed: timed out\n')
                socket.close() 
                print('Disconnect one inactive connection.\n') 
                
                break

# initiate new clients (threads)
class Client(threading.Thread):  
    def __init__(self, conn, ch):
        threading.Thread.__init__(self)
        self.socket = conn                          # socket id
        self.clientHandler = ch                     # ClientHandler class
                                  
    def run(self):
        self.clientHandler.threadPool(self.socket)  # initiate new threads
                        


class DataBaseHandler(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.lock = threading.Lock()      # database lock for single thread access
        self.ops_count = 0                # Number of operations done, reset after reaching 10
        
        
    def access_database(self, argv, conn):
        with self.lock:
            global data
            if argv[0] == 'put':               # perform put
                data[argv[1]] = argv[2]
                conn.sendall('success\n') 
            elif argv[0] == 'get':             # perform get
                try:
                    value = data[argv[1]]
                    conn.sendall("success "+value+"\n")
                except:
                    conn.sendall("failure Cannot find value for key: "+argv[1]+"\n")
            elif argv[0] == 'delete':          # perform delete
                try:
                    del data[argv[1]]
                    conn.sendall("success\n")
                except:
                    conn.sendall("failure Cannot delete key: "+argv[1]+". Key does not exist!\n")    
                    
            # save data to disk after every 10 operations    
            self.ops_count = (self.ops_count+1) % 10
            if self.ops_count == 0:
                s = json.dumps(data)            # load current database
                f = open("db.json", "w")
                f.write(s)                      # flush database to db.json file
                f.close()
                
        
def main():
    
    print "Hello World!"                        # Keep this cute starup message
    ch = ConnectionHandler(MaxThreads)
    s.listen(5)
    while 1:
        conn, addr = s.accept()                 # accept new connection request
        print'connected by', addr               # print connection message on server
        Client(conn, ch).start()                # call clients(threads) initiator
        

def parseline(strings, conn):
    i = 0
    argv = []
    argc = 0
    delim = 0
   
   #ignore leading spaces
   # i = 0
   # while strings[i] == ' ':i+=1
   # strings = strings[i:len(strings)]   

    
    #for i in range(len(strings)):
    while strings[i] != '\n':
        if strings[i] == ':':
            nread = int(strings[delim:i])
            delim= i+1
            try: 
                argv.append(strings[delim:delim+nread])
                delim+=nread
                if strings[delim]!=' ' and strings[delim] != '\n':
                    strings = strings[delim+1:len(strings)]
                    return 0, strings
            except:
                return 0

            argc += 1
        elif argc == 0:
            if strings[i] == ' ':
                argv.append(strings[delim:i])
                delim = i+1
                argc += 1     
        i+=1
    if argc == 0: argv.append(strings[delim:i]) 
    strings = strings[i+1:len(strings)]                    
    return argv, strings


def eval_input(strings, conn):
    argv, strings = parseline(strings, conn)
    #print(repr(argv))                                                      # print acutal command vecotr after pareline (for debug purpose)
    if argv == 0 :
        conn.sendall('failure Invalid data arguments!\n')    
    else: 
        if argv == ['']:                                                    # ignore empty command line
            pass
        elif argv[0] == 'put' or argv[0] == 'get' or argv[0] == 'delete':   # if input is built-in commands
            if(data_check(argv)):                                           # check number of data argument
                database.access_database(argv, conn)
            else:
                conn.sendall('failure Need more input arguments\n')
        elif argv[0] == 'crash':
            os._exit(0)
        else:                                                              # return failure if input is not biult-in commands
            conn.sendall("failure invalid command!\n")
     
    return strings
      
# check arguments count for each biult-in commands      
def data_check(argv):
    argc=2
    if(argv[0] == 'put'): argc=3
    if len(argv) < argc: return 0       
    return 1    
  

if __name__ == '__main__':
    database = DataBaseHandler()
    main()
    
