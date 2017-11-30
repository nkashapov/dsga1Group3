# program to DFS
import os
import Queue
import time
from datetime import datetime
from socket import *
# init function
def initialize(s, hosts, port):
    addr = (hosts, port)
    s.connect(addr)
    return s


# test of path, reduce lenght 
def testPath(paths, currentPaths, sockS):
    pattth = currentPaths
    if paths[0] == '/':
        pattth = paths
    elif paths[0] == '~':
        pattth = "./"+paths
    elif paths.startswith(".."):
        paths = paths.replace("..",'')
        lastBlock = currentPaths.rfind('/')
        if lastBlock !=0:
            pattth = currentPaths[0:lastBlock] + paths
        else:
            pattth = '/' + paths
    elif paths[0] == '.':
            pattth = str(currentPaths+"/"+paths[1:])
    else:
        pattth = str(currentPaths+"/"+paths)
    pattth = pattth.replace("//", "/")
    return pattth

# 
def testPathCD(paths, currentPaths, sockS):
    pattth = currentPaths
    if paths[0] == '/':
        pattth = paths
    elif paths[0] == '~':
        pattth = "./"+paths
    elif paths.startswith(".."):
        paths = paths.replace("..",'')
        lastBlock = currentPaths.rfind('/')
        if lastBlock !=0:
            pattth = currentPaths[0:lastBlock] + paths
        else:
            pattth = '/' + paths
    elif paths[0] == '.':
            pattth = str(currentPaths+"/"+paths[1:])
    else:
        pattth = str(currentPaths+"/"+paths)
    pattth = pattth.replace("//", "/")
    message = str("if " + pattth)
    sockS.send(message)
    answer = sockS.recv(100).decode("utf-8")
    if answer == "Directory not exist":
        print("Directory not exist")
        return currentPaths
    elif len(answer)==0:
        return currentPaths
    return pattth

# read function
def rdServer(s, files):
    global temp
    global filelist
    countInBlock = 1024
    size = 500 #size of read from file
    blockSize = size*countInBlock
    s.send("rd " + files)
    data = s.recv(512).decode()
    if(str(data).startswith("Directory not Exist")):
        print("file not exist")
        return
    try:
        hostFS, blocks_queue = data.split(" ")
        dBlocks = blocks_queue.split(";")
    except:
        print("No answer")
        return
    sockFS = socket(AF_INET, SOCK_STREAM)
    sockFS.connect((hostFS, 10520))
    new_file = open("."+files, 'w')
    sockFS.send("rd")
    number = 0
    sockFS.send(str(len(dBlocks)))
    sockFS.recv(2)
    try:
        for  mes in dBlocks:
            sockFS.send(str(mes).rjust(16, '0').encode("utf-8")) #
            sockFS.recv(2) # 
            for j in range(1024):
                filepart = sockFS.recv(size) #
                new_file.write(filepart)
                sockFS.send(str("ok").encode("utf-8")) 
                if(len(filepart)==0):
                    break 
    except:
        print("Alarm! New attempt...")
        time.sleep(5)
        hostFScrashed = hostFS
        s.send("rd " + files)
        data = s.recv(512).decode()
        if(str(data).startswith("Directory not Exist")):
            print("file not exist")
            return
        hostFS, blocks_queue = data.split(" ")
        if (hostFS == hostFScrashed):
            print("Sorry, servers is not available")
        else:
            rdServer(s, files)
        return
    fileLog = open("./log.txt", 'a')
    fileLog.write(files + " was dowloaded from host " + str(hostFS)+ " at "+  str(datetime.today())+"\n")
    filelist.put(files)
    fileLog.close()
    new_file.close()
    sockFS.close()

def sizeOfFile(s, files):
    global temp
    global filelist
    countInBlock = 1024
    size = 500 #size of read from file
    blockSize = size*countInBlock
    s.send("rd " + files)
    data = s.recv(512).decode()
    if(str(data).startswith("Directory not Exist")):
        print("file not exist")
        return
    try:
        hostFS, blocks_queue = data.split(" ")
        dBlocks = blocks_queue.split(";")
    except:
        print("No answer")
        return
    countOfBlocks = len(dBlocks)
    if(countOfBlocks > 4):
        print(str(files) + " file size is " + str(countOfBlocks/2)  + "MB")
    else:
        sockFS = socket(AF_INET, SOCK_STREAM)
        sockFS.connect((hostFS, 10520))
        sockFS.send("rd")
        number = 0
        new_file = open("./temp",'a')
        sockFS.send(str(1))
        sockFS.recv(2)
        sockFS.send(str(dBlocks[-1]).rjust(16, '0').encode("utf-8")) #
        sockFS.recv(2) #
        for j in range(1024):
            filepart = sockFS.recv(size) #
            new_file.write(filepart)
            sockFS.send(str("ok").encode("utf-8")) 
            if(len(filepart)==0):
                break 
        new_file.close()
        sockFS.close()
        fileS = (countOfBlocks-1)*512 + (os.path.getsize("./temp")/1024)
        print(files +" file size is " + str(fileS)+" kB")
        os.remove("./temp")
    return("Address of Storage Server is: " + str(hostFS))

# write - function 
def wrServer(s, files):
    files = "."+files
    size = 500
    blockSize = size*1024
    try:
        filesize = os.path.getsize(files)
    except:
        return
    s.send("wr " + files[1:] +" "+ str(filesize/blockSize+1))
    data = s.recv(512).decode()
    try:
        hostFS, blocks_queue = data.split(" ")
        dBlocks = blocks_queue.split(";")
    except:
        print("No answer")
        return
    numberOfBlocks = len(dBlocks)
    sockFS = socket(AF_INET, SOCK_STREAM)
    sockFS.connect((hostFS, 10520))   
    my_file = open(files, 'rb')
    sockFS.send("wr")
    sockFS.send(str(numberOfBlocks).rjust(16, '0'))
    sockFS.recv(2)
    i = 0
    try:
        for mes in dBlocks:
            filenum = str(mes)
            sockFS.send(filenum.rjust(16, '0').encode("utf-8")) # sending of the chunknumber
            sockFS.recv(2)# wait ok
            if i != numberOfBlocks-1:
                for j in range(1024):
                    filepart = my_file.read(size) 
                    sockFS.send(filepart) #sending part of the file
                    sockFS.recv(2) # waiting ok
            else:    
                filepart = my_file.read(size)
                while len(filepart) > 0:
                    sockFS.send(filepart)
                    sockFS.recv(2)
                    filepart = my_file.read(size)      
            i = i + 1
    except:
        print("Alarm! New attempt...")
        time.sleep(5)
        hostFScrashed = hostFS
        s.send("wr " + files)
        data = s.recv(512).decode()
        #    print(data+"!!!!!!!!!!!")
        if(str(data).startswith("Directory not Exist")):
            print("file not exist")
            return
        hostFS, blocks_queue = data.split(" ")
        if (hostFS == hostFScrashed):
            print("Sorry, servers is not available")
        else:
            wrServer(s, files)
        return
    my_file.close()
    sockFS.send("0.")
    sockFS.recv(2)
    sockFS.close()
    fileLog = open("./log.txt", 'a')
    fileLog.write(files[1:] +" was created and sended to host " + str(hostFS)+ " at "+  str(datetime.today())+"\n")
    fileLog.close()


def lsServer(s, files):
    s.send("ls " + files)
    data = s.recv(500)
    message = str(data.decode("utf-8"))
    print(message)

def ifServer(s, files):
    s.send("if " + files)
    data = s.recv(500)
    print(data.decode("utf-8"))

def mkdirServer(s, files):
    s.send("md " + files)
    os.system("mkdir ."+files)
    data = s.recv(500)
    print(data.decode("utf-8"))

def rmServer(s, files):
    s.send("rm " + files)
    try:
        os.remove("."+files)
    except:
        print("file not exist")
    data = s.recv(100)
    print(data.decode("utf-8"))

def connectToNS():
    host = '0.0.0.0'
    port = 10505
    addr = (host, port)
    udp_socket = socket(AF_INET, SOCK_DGRAM)
    udp_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    udp_socket.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
    udp_socket.bind(addr)
    conn, addr = udp_socket.recvfrom(1024)
    udp_socket.close()
    fileLog = open("./log.txt", 'a')
    fileLog.write(" Client started at "+  str(datetime.today())+"\n")
    fileLog.close()
    return addr[0]

def userInterface():
    global filelist
    global hostns
    port = 10510
    currentPath = '/'
    sock = socket(AF_INET, SOCK_STREAM)
    sock = initialize(sock, hostns, port)
    while True:
        try:
            usercommand = str(raw_input("dfs:~"+currentPath+"$ "))
            if filelist.qsize() > 10:
                fileToDelete = filelist.get()
                os.remove("."+fileToDelete)
            if usercommand.find(' ') != -1:         
                command, path = usercommand.split(' ')
            else:
                command = usercommand
                path = currentPath
            if len(path) == 0:
                path = currentPath
            if command == 'init':
                try:
                    sock = connectToNS()
                except:
                    print("Incorrect hostname and port!")
            elif command == 'ls':
                path = testPath(path, currentPath, sock)
                lsServer(sock, path)
            elif command == 'rd':
                path = testPath(path, currentPath, sock)
                try:
                    rdServer(sock, path)
                except:
                    print(" ")
            elif command == 'rm':
                path = testPath(path, currentPath, sock)
                try:
                    rmServer(sock, path)
                except:
                    print(" ")
            elif command == 'cache':
                print(filelist.qsize())
            elif command == 'wr':
                path = path.replace('~','/')
                path = path.replace('//','/')
                print(path)
                path = testPath(path, currentPath, sock)
                filelist.put(path)
                try:
                    wrServer(sock, path)
                except:
                    print(" ")
            elif command == 'info':
                path = testPath(path, currentPath, sock)
                try:
                    ifServer(sock, path)
                except:
                    print(" ") 
                print(sizeOfFile(sock, path))
            elif command == 'mkdir':
                path = testPath(path, currentPath, sock)
                mkdirServer(sock, path) 
            elif command == 'cd':
                currentPath = testPathCD(path, currentPath, sock)
            elif command == 'pwd':
                print(currentPath)
            elif command == 'size':
                sizeOfFile(sock, path)
            elif command == 'lsc':
                print(os.system("ls ./"))
            elif command == 'exit' or command == 'ex':
                fileLog = open("./log.txt", 'a')
                fileLog.write(" Client shutdown at "+  str(datetime.today())+"\n")
                fileLog.close()
                while filelist.qsize() > 0:
                    os.remove("."+filelist.get())
                return
            else:
                # try:
                if usercommand.count(' ') > 1 :
                    eraser = usercommand.rfind(' ')
                    print(eraser)
                    path = usercommand[eraser:]
                    command = usercommand[0:eraser]                
                path = testPath(path, currentPath, sock)
                rdServer(sock, path)
                print("something goes wrong")
                print(os.system(command + " ."+path))
                # except:
                #     print("something goes wrong")
        except:
            hostns = connectToNS()
            print("Error")

if __name__ == "__main__":          
    filelist = Queue.Queue()
    hostns = connectToNS()
    print("Address of NS is: " + hostns)
    port = 10510
    size = 512
    userInterface()
