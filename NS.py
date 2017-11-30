import pymysql
import socket
import threading
from pymysql import Error
from time import *
from socket import *

def rd(PathToFile, servid=0):
    global debug
    if debug > 0:
        print("___DEBUG___(rd):", PathToFile, servid)
    global addrServers
    global connection
    global cursor
    fstFlg=0
    parentflag=0
    tmpbal=str(buildPathUp(PathToFile))
    if tmpbal=="Directory not Exist!":
        return "Directory not Exist!"
    qure = """SELECT id_entity, id_par, flagtype, block_1, name FROM ftable where id_entity = '%(delID)s' OR id_par = '%(delID)s'  ORDER BY id_entity;
            """ % {"delID": tmpbal}
    if debug > 0:
        print("___DEBUG___(rd) current query 1:", qure)
    cursor.execute(qure)
    mainId=cursor.fetchall()
    tmpbal = ""
    for ba in mainId:
        if len(tmpbal)==0:
            tmpbal = tmpbal + str(ba[3])
        else:
            tmpbal=tmpbal+";"+str(ba[3])
    tmpbal=addrServers[servid] + ' ' + tmpbal
    return tmpbal

def removeEntity(entityToRemove):
    global connection
    global cursor
    delId = buildPathUp(entityToRemove)
    if delId=="Directory not Exist!":
        return "Directory not Exist!"
    if delId == "ERROR: Incorrect path!":
        return "ERROR: Incorrect path!"
    stFlg = 0
    qure = """SELECT id_entity, id_par, flagtype, name FROM ftable where id_par = '%(delID)s' ORDER BY id_entity;""" % {"delID": delId}
    print(qure)
    cursor.execute(qure)
    currentLen = cursor.fetchall()
    if len(currentLen)==0:
        qure = "delete from ftable where id_entity = '%(parma)s' OR id_par = '%(parma)s';" % {"parma": delId}
        cursor.execute(qure)
        connection.commit()
        return "System: Directories/files are removed"
    delST=0
    zeroSt=0
    for g in currentLen:
        if zeroSt==0:
            delST=[g[0],g[3]]
            zeroSt=1
        else:
            if g[1] != delST[0] and g[3] != delST[1]:
                return "Error you cant deletre this entity. It contains something"
    qure = "delete from ftable where id_entity = '%(parma)s' OR id_par = '%(parma)s';" %{"parma":delId}
    cursor.execute(qure)
    connection.commit()
        # qure = """delete from ftable where id_entity >13"""
    return "System: Directories/files are removed"

def selector(server):
    block_id = ["block_1", "block_2", "block_3"]
    p_block_id = ["pblock_1", "pblock_2", "pblock_3"]
    return block_id[server], p_block_id[server]

def createFile(nameOfFile, numChunk, parentEntityF, serverid):
    global connection
    global cursor
    global path
    global debug
    global chunksForClient
    global addrServers
    msgFClient = ""
    cBlock, pBlock = selector(serverid)
    chunksForClient = []
    qure = """select block_1 From ftable where block_1 IS NOT NULL ORDER BY block_1;"""
    cursor.execute(qure)
    blocks = cursor.fetchall()
    lastel = blocks[-1][0]
    headPar = 0
    j = 1
    i = 0
    tempPar = 0
    while j <= lastel and len(chunksForClient) < numChunk:
        if debug > 0:
            print("___DEBUG___(createFile):", j, len(blocks), len(chunksForClient), "num of chunks:", numChunk)
        if j < blocks[i][0] and len(chunksForClient) < numChunk:  # and j<len(blocks):
            while j < blocks[i][0] and len(chunksForClient) < numChunk:
                chunksForClient.append(j)
                j = j + 1
        j = j + 1
        i = i + 1
    if len(chunksForClient) < numChunk:
        for z in range(lastel + 1, 1 + lastel + (numChunk - len(chunksForClient))):
            chunksForClient.append(z)
    if debug > 0:
        print("___DEBUG___(createFile):", "len:", len(chunksForClient), "chunks:", chunksForClient)
    for z in chunksForClient:
        if len(msgFClient) == 0:
            msgFClient = str(z)
            tempPar = z
            tempTime = int(time())
            qure = """insert into ftable ( id_par, flagtype, name, timest, %(cbBlock)s)  
            VALUES ('%(id_par)s', '1', '%(name)s','%(timest)s', '%(ncBlock)s');
            """ % {"cbBlock": cBlock, "id_par": parentEntityF, "name": nameOfFile, "timest": tempTime, "ncBlock": z}
            if debug > 0:
                print("___DEBUG___(createFile) current query:", qure)
            cursor.execute(qure)
            qure = """select id_entity, id_par, flagtype, name, timest from ftable where
                     id_par = '%(id_par)s' AND flagtype = '1' AND name = '%(name)s' AND timest = '%(timest)s';
                       """ % {"id_par": parentEntityF, "name": nameOfFile, "timest": tempTime}
            cursor.execute(qure)
            headPar = cursor.fetchone()
            headPar = headPar[0]
        else:
            msgFClient = msgFClient + ";" + str(z)
            qure = """insert into ftable ( id_par, flagtype, name, timest, %(cbBlock)s, %(cpBlock)s)  
            VALUES ('%(id_par)s', '1', '%(name)s','%(timest)s', '%(ncBlock)s', '%(npBlock)s');
            """ % {"cbBlock": cBlock, "cpBlock": pBlock, "id_par": headPar, "name": nameOfFile, \
                   "timest": int(time()), "ncBlock": z, "npBlock": tempPar}
            if debug > 0:
                print("___DEBUG___(createFile) current query:", qure)
            tempPar = z
            cursor.execute(qure)
    connection.commit()
    msgFClient= addrServers[serverid]+' '+msgFClient
    return msgFClient

def buildpathdown(idEnt):
    global connection
    global cursor
    global path
    global debug
    path = ""
    while idEnt != 1:
        qur = "SELECT id_entity, id_par, name FROM ftable WHERE id_entity = " + str(idEnt) + ";"
        if debug > 0:
            print("___!!!__DEBUG(path query): ", qur)
        cursor.execute(qur)
        linepath = cursor.fetchone()
        path = str(linepath[2]) + "/" + path
        if debug > 0:
            print("___!!!__DEBUG(current build path path): ", path)  # blocks[j]

        idEnt = linepath[1]
    path = "/" + path
    return (path)

def buildPathUp(nameEnt, createFolderF=0, CreateFileFlag=0, sizeFC=0, serverid=0):
    global connection
    global cursor
    global debug
    global newfolder
    if debug > 0:
        print("___!!!__DEBUG(buildPathUp) meanings:",nameEnt, createFolderF, CreateFileFlag, sizeFC, serverid)
    fPath = ""
    parentEntitty = 0
    parentName = "1"
    try:
        nameEnt = nameEnt.split('/')
    except:
        return "ERROR: Incorrect path!"
    endenflag = 0
    for i in nameEnt:
        endenflag = 1 + endenflag
        goto = 0
        while goto == 0:
            parentEntitty = parentName
            if i == "":
                goto = 1
                continue
            qure = "SELECT id_entity, id_par, name, block_1 FROM ftable WHERE name = '" \
                   + str(i) + "' AND id_par = '" + str(parentName) + "';"
            if debug > 0:
                print("___!!!__DEBUG(buildPathUp) current query: ", qure)
            cursor.execute(qure)
            curpath = cursor.fetchone()
            try:
                parentName = curpath[0]
                goto = 1
            except:
                if createFolderF > 0:
                    if debug > 0:
                        print("len(nameEnt) == endenflag", len(nameEnt), endenflag)
                    if len(nameEnt) == endenflag:
                        if CreateFileFlag > 0:
                            baka = createFile(i, sizeFC, parentEntitty, serverid)
                            print("System: File created", baka)
                            return baka
                    print("folder created")
                    qure = """INSERT INTO ftable(id_par, name, timest, flagtype )
            VALUES ('%(id_par)s', '%(name)s', '%(timest)s', '%(flagtype)s')
            """ % {"id_par": parentEntitty, "name": i, "timest": int(time()), "flagtype": 0}
                    cursor.execute(qure)
                    connection.commit()
                    newfolder = 1
                else:
                    return ("Directory not Exist!")
            if debug > 0:
                print("___!!!__DEBUG(buildPathUp) current position: ", parentName)
    return (parentName)


def showThePath(pathId):
    global connection
    global cursor
    global debug
    list = "\n1 - file; 0 - folder:\n--------------------- \n"
    lsQuery = "SELECT id_par, flagtype, name FROM ftable WHERE id_par = '" + str(pathId) + "' ORDER BY name;"
    cursor.execute(lsQuery)
    pathData = cursor.fetchall()
    if len(pathData) == 0:
        return "Not exist directory or that is a file."
    for pd in pathData:
        list = list + str(pd[1]) + "\t" + str(pd[2]) + "\n"
        if debug > 1:
            print("___!!!__DEBUG(showThePath) list of path :", list)
    return list


def entityInfo(entityPath, failFlag=0):
    global connection
    global cursor
    global debug
    if failFlag == 1:
        return "Directory not exist"
    eIQuery = "SELECT id_entity, name, timest, flagtype FROM ftable WHERE id_entity = " + str(
        buildPathUp(entityPath)) + ";"
    try:
        cursor.execute(eIQuery)
    except:
        fail = "Directory not exist"
        return fail
    tmpdata = cursor.fetchone()
    if tmpdata[3] == 1:
        res = "File: " + tmpdata[1] + " created: " + strftime('%Y-%m-%d %H:%M:%S', localtime(tmpdata[2]))
    else:
        res = "Folder: " + tmpdata[1] + " created: " + strftime('%Y-%m-%d %H:%M:%S', localtime(tmpdata[2]))
    return res

def cliConn(con, adr):
    global debug
    global stopFlag
    global conAddr
    global newfolder
    global tax
    global addrServers
    delFlags=0
    if len(addrServers) > 0:
        serverid = tax % len(addrServers)
    else:
        serverid = 0
    tax = tax + 1
    if debug > 0:
        print("___!!!__DEBUG(cliConn):start with :", adr)
    pool = 512
    temson=[]
    outMsg = ""
    #con.setblocking(0)
    while stopFlag == 0:
        try:

            addrServers.index(adr[0])
            print("addrServers",addrServers, "adr[0]", adr[0])
            sleep(10)
            try:
                outMsg = ""
                temson=[]
                for i in addrServers:
                    temson.append(i)
                temson.remove(adr[0])
                for i in temson:
                    outMsg=outMsg + ";" + i
                if len(outMsg) > 0:
                    outMsg = outMsg+";"
                print(outMsg)
                con.send(outMsg.encode('utf-8'))
            except:
                addrServers.remove(adr[0])
                print("Connection with SS is lost: ", adr[0])
                con.close()
                return
        except ValueError:
            try:
                inMsg = con.recv(pool)
                inMsg = inMsg.decode("utf-8")
                if debug > 0:
                    print("recive from ", adr, " mesage ", inMsg)
                inMsg = inMsg.split()
                if inMsg[0] == "ls":
                    tmpId = buildPathUp(inMsg[1])
                    outMsg = showThePath(tmpId)
                    if outMsg == "\nNone" or outMsg == "None":
                        outMsg = "not exist Dirictory"
                    if debug > 0:
                        print("___!!!__DEBUG(cliConn):outgoing message:", outMsg)
                    con.send(outMsg.encode("utf-8"))
                    outMsg=""
                if inMsg[0] == "if":
                    outMsg = entityInfo(inMsg[1])
                    print("Message sended to client IF:", outMsg)
                    con.send(outMsg.encode("utf-8"))
                if inMsg[0] == "md":
                    newfolder = 0
                    buildPathUp(inMsg[1], 1)
                    if newfolder == 1:
                        outMsg = "Folder are exist"
                    else:
                        outMsg = "Folder(s): " + str(inMsg[1]) + " is created."
                    print("Message sended to client MD:", outMsg)
                    con.send(outMsg.encode("utf-8"))
                    outMsg=""
                if inMsg[0] == "wr":
                    outMsg = buildPathUp(nameEnt=inMsg[1], createFolderF=1, CreateFileFlag=1, sizeFC=int(inMsg[2]), serverid=serverid)
                    print("Message sended to client to WR:", outMsg)
                    con.send(outMsg.encode("utf-8"))
                    outMsg = ""
                if inMsg[0] == "rm":
                    outMsg = removeEntity(inMsg[1])
                    print("Message sended to client to RM:", outMsg)
                    con.send(outMsg.encode("utf-8"))
                    outMsg = ""
                if inMsg[0] == "rd":
                    outMsg = rd(inMsg[1])
                    print("Message sended to client to RD:", outMsg)
                    con.send(outMsg.encode("utf-8"))
                    outMsg = ""
                if inMsg[0] == "SS":
                    addrServers.append(adr[0])
                    print(addrServers)

            except:
                break
    con.close()
    return

def netCon():
    global stopFlag
    global conAddr
    if debug > 0:
        print("___!!!__DEBUG(netCon):start ")
    sock = socket(AF_INET, SOCK_STREAM)
    sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    sock.bind(('0.0.0.0', 10510))
    sock.listen(5)
    print("waiting connections of Clients")
    while stopFlag == 0:
        c, a = sock.accept()
        print("Connection received  from", a)
        conAddr[a] = c
        threading.Thread(target=cliConn, args=(c, a)).start()
    print("___!!!__DEBUG(netCon):stoping ")
    return

def netConSS():
    global stopFlag
    global conAddr
    if debug > 0:
        print("___!!!__DEBUG(netCon):start ")
    sock = socket(AF_INET, SOCK_STREAM)
    sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    sock.bind(('0.0.0.0', 10506))
    sock.listen(5)
    print("waiting connections of Storage Servers")
    while stopFlag == 0:
        c, a = sock.accept()
        print("Received connection from", a)
        conAddr[a] = c
        threading.Thread(target=cliConn, args=(c, a)).start()
    print("___!!!__DEBUG(netCon):stoping ")
    return

def udpSndr():

    global debug
    global stopFlag
    socks = socket(AF_INET, SOCK_DGRAM)
    socks.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
    if debug > 0:
        print("___!!!__DEBUG(udpSndr):start sending")
    while stopFlag == 0:
        sleep(10)
        if stopFlag == 1:
            if debug > 0:
                print("___!!!__DEBUG(udpSndr):sndr stoping")
            brk = "LostNS"
            socks.sendto(brk.encode('utf-8'), ('255.255.255.255', 10505))
            socks.sendto(brk.encode('utf-8'), ('127.0.0.1', 10505))
            break
        brk = "EchoSS"
        socks.sendto(brk.encode('utf-8'), ('255.255.255.255', 10505))
        if debug > 0:
            print("___!!!__DEBUG(udpSndr):Msg sended", brk)


    print("___!!!__DEBUG(udpSndr):sndr stoping")
    return

def udpRcv():
    global stopFlag
    global debug
    global findFlag
    if debug > 0:
        print("___!!!__DEBUG(udpRcv):start listening")
    s = socket(AF_INET, SOCK_DGRAM)
    s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    s.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
    s.bind(('0.0.0.0', 10505))
    global messg
    while stopFlag == 0:
        messgTr, addr = s.recvfrom(2048)
        messgTr = messgTr.decode()
        if debug > 0:
            print("___!!!__DEBUG(udpRcv):recieved msg", messgTr)
    print("___!!!__DEBUG(udpRcv):stoping")
    return


if __name__ == '__main__':
    tax = 0
    ServIP = "192.168.0.79"
    addrServers = []
    num_of_servers = len(addrServers)
    conAddr = {}
    debug = 2
    if debug > 0:
        print("___!!!__DEBUG(debuglevel)=", debug)
    dbhost = '127.0.0.1'
    dbuser = 'root'
    dbpass = ''
    dbpath = 'fs'
    newfolder = 0
    stopFlag = 0
    connection = 0
    chunksForClient = []
    cursor = 0
    asa = ""
    path = ""
    try:
        connection = pymysql.connect(user=dbuser, passwd=dbpass,
                                     host=dbhost,
                                     database=dbpath
                                     )
    except Error as e:
        print("Error while connection to database:", e)
    cursor = connection.cursor()
    curTime = time()
    curTime = int(curTime)
    test = 1
    if test>0:
        thread3 = threading.Thread(target=udpSndr)
        thread1 = threading.Thread(target=udpRcv)
        thread2 = threading.Thread(target=netCon)
        thread4 = threading.Thread(target=netConSS)
        thread3.start()
        thread1.start()
        thread2.start()
        thread4.start()
    '''
    #print(rd("/Nikita/Molodec/Gazman"))
    # print(showThePath(7))
    # print(buildPathUp())
    #print(buildPathUp("/sf2.mp3", 1, 1, 1))
    #   print(entityInfo("/zika/ruke"))
    #  print(entityInfo("/zika/"))
    #print(removeEntity("/Nikita/Molodec/Gazman"))
    #print(removeEntity("/zika"))
    # print(showThePath(13))
    # createFile(1,1,1)
    # print(removeEntity('/lol.mp3'))

    rd("/lol.mp3", 0)
    '''
    while stopFlag == 0:
        if test >0:
            conStr = input("command: ")
        else:
            conStr = "quit"
        if conStr == "quit":
            stopFlag = 1
            if test>0:
                thread3.join()
                thread2.join()
                thread1.join()
        if conStr == "ss":
            print(addrServers)
    connection.close()
