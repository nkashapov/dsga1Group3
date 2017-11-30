#Storage Server Stub1
import socket
import select
import sys
import os, errno
import time

#Add new client
def add_client(tcp_socket):
  sock, addr = tcp_socket.accept()
  CONNECTION_LIST.append(sock)
  print addr, 'connected'
  return sock

#Read chunk
def read_chunk(sock):
  count_chunks = sock.recv(16)
  n = int(count_chunks)
  sock.send('OK')
  while n > 0:
    print 'number of chunks :', n
    name_chunks = sock.recv(16)
    sock.send('OK')
    print 'chunk\'s name is: ', name_chunks
    try:
      chunk_r = open(DIR + '/' + name_chunks, 'rb')
    except:
      print name_chunks, ' no such file'
      break
    for j in range(1024):
      filepart = chunk_r.read(BUFFER)
      sock.send(filepart)
      if len(filepart) == 0:
        break
      data = sock.recv(2)
    print 'chunk ', name_chunks, ' was sent'
    n -= 1
    chunk_r.close()
  print 'Data was sent'
  sock.close()
  CONNECTION_LIST.remove(sock)

#Write chunk
def write_chunk(sock):
  count_chunks = sock.recv(16)
  n = int(count_chunks)
  sock.send('OK')
  while n > 1:
    print 'number of chunks: ', n
    name_chunks = sock.recv(16)
    sock.send('OK')
    print 'chunk\'s name is: ', name_chunks
    chunk_w = open(DIR + '//' + name_chunks, 'wb')
    for j in range(1024):
      data = sock.recv(BUFFER)
      chunk_w.write(data)
      sock.send('OK')
    chunk_w.close()
    print 'chunk ', name_chunks, ' was recieved'
    n -= 1
  name_chunks = sock.recv(BUFFER)
  print 'chunk\'s name is: ', name_chunks
  sock.send('OK')
  data = sock.recv(BUFFER)
  if data[:2] != '0.':
    chunk_w = open(DIR + '//' + name_chunks, 'wb')
    while data[:2] != '0.':
      chunk_w.write(data)
      sock.send('OK')
      data = sock.recv(BUFFER)
    chunk_w.close()
  sock.send('OK')
  print 'Data was recieved'
  sock.close()
  CONNECTION_LIST.remove(sock)  

#UDP-server connection
def udp_connect(addr_port):
  sockfd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  sockfd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
  sockfd.bind(addr_port)
  CONNECTION_LIST.append(sockfd)
  return sockfd

#TCP-server connection
def tcp_connect(addr_port):
  sockfd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  sockfd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
  sockfd.bind(addr_port)
  sockfd.listen(5)
  CONNECTION_LIST.append(sockfd) 
  return sockfd

#Reconnect socket
def reconnect(tcpsock):
  tcpsock.close()
  CONNECTION_LIST.remove(tcpsock)
  sockfd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  return sockfd

#Read replica data
def read_replica(sock):
  try:
    sock.recv(2)
    files = os.listdir(DIR)
    count_files = len(files)
    sock.send(str(count_files))
    sock.recv(2)
    print 'Number of files: ', count_files
    i = 1
    for file_name in files:
      sock.send(file_name)
      sock.recv(2)
      print 'Name: ', file_name, ' sent'
      f = open(DIR + '/' + file_name, 'rb')
      S = 0
      for j in range(1024):
        data = f.read(BUFFER)
        if(len(data) == 0):
          sock.send('BREAK')
          sock.recv(2)
          break
        sock.send(data)
        sock.recv(2)
        S += len(data)
      print i, ': File ', file_name, ' replicated: ', S, ' bytes'
      f.close()
#      sys.stdin.read(1)
      i += 1
  except:
    print 'Reading Replica is down'

#Write Replica
def write_replica(sock):
  try:
    sock.send('OK')
    count_files = int(sock.recv(16))
    sock.send('OK')
    print 'Number of files: ', count_files
    for i in range(count_files):
      file_name = sock.recv(16)
      sock.send('OK')
      n = i + 1
      print n, ': Name ', file_name, ' recieved (Replication)'
      f = open(DIR + '/' + file_name, 'wb')
      S = 0
      for j in range(1024):
        data = sock.recv(BUFFER)
        sock.send('OK')
        if data == 'BREAK':
          break
        f.write(data)
        S += len(data)
      print 'File ', file_name, ' recieved (Replication): ', S, ' bytes'
      f.close()

  except:
    print 'Writing Replica is down'


#Starting program
if __name__ == '__main__':
  #Socket description
  CONNECTION_LIST = []
  BUFFER = 500
  CHUNK = BUFFER * 1024 
  UDP_PORT = 10505
  IP = "0.0.0.0"
  TCP_NS_PORT = 10506
  TCP_CL_PORT = 10520
  TCP_REPLICA = 10849
  DIR = '1'
  sockCL = ''
  repCL = ''
  ip_addr_servs = []

  client = (IP, TCP_CL_PORT)
  nsUDP = (IP, UDP_PORT)
  replica = (IP, TCP_REPLICA)

#Create directory
  if not os.path.exists(DIR):
    os.makedirs(DIR)

#UDP socket for NS
  udp_socket = udp_connect(nsUDP)

#TCP client socket for NS
  tcp_ns_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

#TCP server socket for CLIENT
  tcp_cl_socket = tcp_connect(client)

#TCP server socket for REPLICA
  server_replica_socket = tcp_connect(replica)

#TCP client socket for REPLICA
  client_replica_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

  while True:
    #Get the list sockets which are ready to be read through select
    read_sockets, write_sockets, error_sockets = select.select(CONNECTION_LIST, [], [])
    for sock in read_sockets:
      #UDP Request from Name Server
      if sock == udp_socket:
        data, addr_udp = udp_socket.recvfrom(BUFFER)
        print 'UDP Request: {}'.format(data)
        if "EchoSS" in data and tcp_ns_socket not in CONNECTION_LIST:
          try:
            tcp_ns_socket.connect((addr_udp[0], TCP_NS_PORT))
            print 'Connected to NS using TCP'
            CONNECTION_LIST.append(tcp_ns_socket)
            tcp_ns_socket.send('SS 1')
            CONNECTION_LIST.remove(udp_socket)
          except:
            print 'Failed connection to NS using TCP'
            continue

      #TCP connection with Name Server
      elif sock == tcp_ns_socket:
      #if Name Server was disconnected
        try:
          data = sock.recv(BUFFER)
          ip_addr_servs = data.split(';')
          ip_addr_servs = list(set(ip_addr_servs))
          try:
            ip_addr_servs.remove('')
          except:
            ip_addr_servs
          sock.send('OK')
          print 'List of IP addr: ', ip_addr_servs
          for addr in ip_addr_servs:
            try:
              if client_replica_socket not in CONNECTION_LIST and repCL not in CONNECTION_LIST:
                client_replica_socket.connect((addr, TCP_REPLICA))
                print 'Connected to Replica Server {}'.format(addr)
                CONNECTION_LIST.append(client_replica_socket)
                server_replica_socket.close()
                CONNECTION_LIST.remove(server_replica_socket)

              elif repCL not in CONNECTION_LIST:
                files = os.listdir(DIR)
                count_files_cl = len(files)

                try:
                  client_replica_socket.send(str(count_files_cl))
                  print 'send Replica Server: ', count_files_cl
                  client_replica_socket.settimeout(5.0)
                  res = client_replica_socket.recv(2)
                  if 'rd' in res:
                    print 'Reading from SERVER'
                    write_replica(client_replica_socket)
                  elif 'wr' in res:
                    print 'Writing to SERVER'
                    read_replica(client_replica_socket)

                except socket.error, msg:
                  print 'Replica server: {}'.format(msg)
                  if 'Broken pipe' in msg:
                    CONNECTION_LIST.remove(client_replica_socket)
                    client_replica_socket.close()
                    client_replica_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    server_replica_socket = tcp_connect(replica)
                  
            except socket.error, msg:
              print 'Unable to connect to the Replica server. Error: {}'.format(msg)

        except:
          print 'Name server was disconnected'
          tcp_ns_socket = reconnect(tcp_ns_socket)
          udp_socket = udp_connect(nsUDP)

      #Connect Client
      elif sock == tcp_cl_socket:
        print 'Trying to connect CLIENT'
        sockCL = add_client(sock)

      #Connect Replica
      elif sock == server_replica_socket:
        print 'Trying to add client on Replica Server'
        repCL = add_client(sock)
      
      #Handle Replica request from Client
      elif sock == repCL:
        files = os.listdir(DIR)
        count_files_srv = len(files)
        try:
          count_files_cl = int(sock.recv(5))
        except:
          count_files_cl = 0
        if count_files_cl < count_files_srv:
          print 'Client Replica has less files than Server Replica'
          try:
            sock.send('rd')
          except socket.error, msg:
            print 'Failed to send message to Replica Client. Error: {}'.format(msg)
            repCL.close()
            CONNECTION_LIST.remove(repCL)
            CONNECTION_LIST.remove(server_replica_socket)
            server_replica_socket.close()
            server_replica_socket = tcp_connect(replica)
            client_replica_socket.close()
            client_replica_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            continue         
          read_replica(sock)

        elif count_files_cl > count_files_srv:
          print 'Client Replica has more files than Server Replica' 
          try:
            sock.send('wr')
          except socket.error, msg:
            print 'Failed to send message to Replica Client. Error: {}'.format(msg)
            repCL.close()
            CONNECTION_LIST.remove(repCL)
            CONNECTION_LIST.remove(server_replica_socket)
            server_replica_socket.close()
            server_replica_socket = tcp_connect(replica)
            client_replica_socket.close()
            client_replica_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            continue
          write_replica(sock)
        else:
          print 'All data synchronize'
          try:
            sock.send('OK')
          except socket.error, msg:
            print 'Failed to send message to Replica Client. Error: {}'.format(msg)
            repCL.close()
            CONNECTION_LIST.remove(repCL)
            CONNECTION_LIST.remove(server_replica_socket)
            server_replica_socket.close()
            server_replica_socket = tcp_connect(replica)
            client_replica_socket.close()
            client_replica_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            continue

      #Handle Client request
      elif sock == sockCL:
        data = sock.recv(2)
        print data

        if 'rd' in data:
          read_chunk(sockCL)

        elif 'wr' in data:
          write_chunk(sockCL)

        else:
            continue
      
  udp_socket.close()
  tcp_cl_socket.close()
  tcp_ns_socket.close()