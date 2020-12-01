
import threading
import time
import sys
import socket
import time

pt=sys.argv[1]
wid=sys.argv[2]

q=dict()

def send_request(message, port):
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
		s.connect(("localhost", port))
		message=str(message)
		s.send(message.encode())


def update_master(port, worker_id):
	while(True):
		if(worker_id in q.keys() and q[worker_id]):
			#print('here 2')
			task=q[worker_id][0]
			duration=task[2]
			while(duration != 0):
		    		time.sleep(1)
		    		duration=duration-1
			message=[worker_id,task]
			print(message)
			send_request(message,5001)
			q[worker_id].pop(0)


def read_from_master(port, worker_id):
	s = socket.socket()       
	print ("Socket successfully created") 
	port=int(port)
	s.bind(('localhost', port))  
	s.listen(5)           
	while True:    
		c, addr = s.accept()      
		data=c.recv(1024).decode()
		data = eval(data) 
		if(worker_id in q.keys()):
			q[worker_id].append(data)
		else:
			q[worker_id]=[data]

try:
	t1=threading.Thread(target = read_from_master, args=(pt,wid))
	t1.start()
	t2=threading.Thread(target = update_master, args=(pt,wid))
	t2.start()	
except:
   print("Error: unable to start thread")


