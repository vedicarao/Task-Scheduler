#-----------------------------------------------------------imports-----------------------------------------------------------------
import threading
import time
import sys
import socket
import time
import logging

pt=sys.argv[1]
wid=sys.argv[2]
slots= sys.argv[3]
slots=int(slots)
pt=int(pt)

#--------------------------------------------------------------logging------------------------------------------------------------------

logging.basicConfig(filename="tasks_{}.log".format(str(wid)), format='%(asctime)s %(message)s', filemode='w')
logger=logging.getLogger()
logger.setLevel(logging.INFO)


#------------------------------------------------------------locks--------------------------------------------------------------
lock = threading.Lock()
lock2=threading.Lock()
#-------------------------------------------------------------functions---------------------------------------------------------------
q=dict()

def send_request(message, port):
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
		s.connect(("localhost", port))
		message=str(message)
		s.send(message.encode())


def update_master(port, worker_id):
	while(True):
		if(worker_id in q.keys() and q[worker_id]):
			lock2.acquire()
			task=q[worker_id][0]
			
			q[worker_id].pop(0)
			lock2.release()
			duration=task[2]
			while(duration != 0):
		    		time.sleep(1)
		    		duration=duration-1
			message=[worker_id,task]
			print(message)
			lock.acquire()
			logger.info('taskf'+' '+str(message[1][1]) +' '+ str(worker_id))
			lock.release()
			send_request(message,5001)
			
			


def read_from_master(port, worker_id):
	s = socket.socket()       
	print ("Socket successfully created") 
	
	s.bind(('localhost', port))  
	s.listen(5)           
	while True:    
		c, addr = s.accept()      
		data=c.recv(1024).decode()
		data = eval(data) 
		lock.acquire()
		logger.info('tasks'+' '+str(data[1]) +' '+ str(worker_id))
		lock.release()
		lock2.acquire()
		if(worker_id in q.keys()):
			q[worker_id].append(data)
		else:
			q[worker_id]=[data]
		lock2.release()


#-------------------------------------------------------threading--------------------------------------------------------------
try:
	t1=threading.Thread(target = read_from_master, args=(pt,wid))
	t1.start()
	
	thread=[]
	for i in range(slots):
		t=threading.Thread(target = update_master, args=(pt,wid))
		thread.append(t)
	for t in thread:
		t.start()	
except:
   print("Error: unable to start thread")


