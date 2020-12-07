#--------------------------------------------------imports-----------------------------------------------------------------------
import threading
import time
import sys
import socket    
import json
import logging
import random

input_file=sys.argv[1]
k=sys.argv[2]
#-----------------------------------------------logging------------------------------------------------------------------------

logging.basicConfig(filename="jobs.log", 
                    format='%(asctime)s %(message)s', 
                    filemode='w')

logger=logging.getLogger()
logger.setLevel(logging.DEBUG)
#----------------------------------------------thread locking--------------------------------------------------------------------------
lock = threading.Lock()
def write_to_file(m):
	lock.acquire() 
	logger.info(m)
	lock.release()

	


lock2=threading.Lock()
def check_ports(worker):

	lock2.acquire()
	read_file = open(input_file,"r")
	file_json = json.load(read_file)
	n=file_json['workers'][worker]['slots']
	read_file.close()
	lock2.release()
	return n
	
lock3=threading.Lock()
#---------------------------------------------variables and functions------------------------------------------------------------------
d=dict()

def send_request(message, port):
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
		s.connect(("localhost", port))
		message=str(message)
		s.send(message.encode())


#                                            ------------scheduling algorithms----------------------

wlist_rr=[0,1,2]

def sched_algo(k):
	if(k == 'RANDOM'):
		return rand_algo()
	if(k == 'RR'):
		return rr_algo(wlist_rr)
	if(k == 'LL'):
		return ll_algo()
	else:
		print('Incorrect scheduling algo')

def rand_algo():
	while(1):
		wlist = [0,1,2]
		wlist_shuffle=random.shuffle(wlist)
		for worker in wlist:
			if check_ports(worker) > 0:
		    		return worker
	

def rr_algo(wlist_rr):
	count=0
	while(1):
		count=count+1
		for worker in wlist_rr:
			if check_ports(worker)>0:
				for i in range(count):
					wlist_rr.append(wlist_rr.pop(0))
				return worker
	
		
def ll_algo():
	while(1):
		wlist = [0,1,2]
		max=0
		for worker in wlist:
			if check_ports(worker) >  max:
				max=check_ports(worker)
				wid=worker
		if (max>0):
			return wid
		else:
			time.sleep(1)
#                                         ------------------tasks---------------------------
def read_tasks():
		
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)       
		print ("Socket successfully created") 
		port = 5000           
		s.bind(('localhost', port))          
		s.listen(5)
		   
		while(1):    
			c, addr = s.accept()     
			data=c.recv(1024).decode() 
			x=json.loads(data)

			for task in x['map_tasks']:
					index=sched_algo(k)
					message=[x['job_id'],task['task_id'],task['duration']]
					
					lock2.acquire()
					read_file = open(input_file,"r+")
					file_json = json.load(read_file)
					id_worker=file_json['workers'][index]['worker_id']
					port=file_json['workers'][index]['port']
					file_json['workers'][index]['slots']=file_json['workers'][index]['slots']-1
					read_file.seek(0)
					json.dump(file_json, read_file)
					read_file.truncate()
					read_file.close()
					lock2.release()
					print(id_worker,message)
					send_request(message, port)
					lock3.acquire()
					
					if x['job_id'] in d.keys():
						d[x['job_id']]['mapper'].append(message)
					else:
						d[x['job_id']]=dict()
						d[x['job_id']]['mapper'] = []
						
						res = [] 
						for idx, sub in enumerate(x['reduce_tasks'], start = 0):  
							res.append(list(sub.values()))
						
						reducer=[[x['job_id']]+ele for ele in res]
						
						d[x['job_id']]['reducer']=[0]
						d[x['job_id']]['unassigned_reducer']=reducer
						
						d[x['job_id']]['mapper'].append(message)
						lock.acquire()
						logger.info('jobs'+' '+str(message[0]))
						lock.release()
						
					lock3.release()


def map_red_dep():
	while True:	
			job_id=-1
			lock3.acquire()
			for i in d.keys():
				if len(d[i]['mapper']) == 0 and d[i]['reducer'] == [0]:
						job_id = i 
						break 	
			lock3.release()
					
				
			if job_id != -1: 
					for task in d[job_id]['unassigned_reducer']:
						index=sched_algo(k)
						message=[job_id,task[1],task[2]]
								
						lock2.acquire()
						read_file = open(input_file,"r+")
						file_json = json.load(read_file)
						id_worker=file_json['workers'][index]['worker_id']
						port=file_json['workers'][index]['port']
						file_json['workers'][index]['slots']=file_json['workers'][index]['slots']-1
								
						read_file.seek(0)
						json.dump(file_json, read_file)
						read_file.truncate()
						read_file.close() 
						lock2.release()
								
						print(id_worker,message)
						send_request(message, port)
								
						lock3.acquire()
								
						if job_id in d.keys():
							d[job_id]['unassigned_reducer'].remove(message)
							if d[job_id]['reducer'][0]==0:
								d[job_id]['reducer'].pop(0)
								d[job_id]['reducer'].append(message)
							else:
								d[job_id]['reducer'].append(message)
										
						lock3.release()
		
		#c.close()
	
def update_from_workers():
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)       
	print ("Socket 2 successfully created") 
	port = 5001           
	s.bind(('localhost', port))          
	s.listen(5)           
	while True:    
		c, addr = s.accept()      
		data=c.recv(1024).decode('utf-8')
		data = eval(data)
		data[0]=int(data[0])
		index=data[0]-1
		
		lock2.acquire()
		read_file = open(input_file,"r+")
		file_json = json.load(read_file)
		file_json['workers'][index]['slots']=file_json['workers'][index]['slots']+1
		read_file.seek(0)
		json.dump(file_json, read_file)
		read_file.truncate()
		read_file.close()
		lock2.release()
		
		print('Completed', data[1])
		job_id = data[1][0]
		
		
		if job_id in d.keys():
			if data[1] in d[job_id]['mapper']:
				d[job_id]['mapper'].remove(data[1])
			elif data[1] in d[job_id]['reducer']:
				d[job_id]['reducer'].remove(data[1])

		if job_id in d.keys():
			if len(d[job_id]['mapper']) == 0 and len(d[job_id]['reducer']) == 0 : 
				lock.acquire()
				logger.info('jobf'+' '+str(data[1][0]))
				lock.release()
	socket.close()

#------------------------------------------------threads----------------------------------------------------------------------------
try:
	t1=threading.Thread(target = read_tasks)
	t1.start()
	t2=threading.Thread(target = update_from_workers)
	t2.start()
	t3=threading.Thread(target = map_red_dep)
	t3.start()
except:
	print("Error: unable to start thread")


