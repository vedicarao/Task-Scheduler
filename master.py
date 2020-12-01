import threading
import time
import sys
import socket    
import json

input_file=sys.argv[1]

read_file = open(input_file,"r")
file_json = json.load(read_file)
read_file.close()


d=dict()
#print(file_json['workers'][2]['worker_id'])
#list=[1,2,3]

def send_request(message, port):
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
		s.connect(("localhost", port))
		message=str(message)
		s.send(message.encode())


def read_tasks():
	s = socket.socket()       
	print ("Socket successfully created") 
	port = 5000           
	s.bind(('localhost', port))          
	s.listen(5)           
	while True:    
		c, addr = s.accept()      
		data=c.recv(1024).decode() 
		x=json.loads(data)
		for task in x['map_tasks']:
			index=0
			id_worker=file_json['workers'][index]['worker_id']
			if file_json['workers'][index]['slots'] > 0:
		                #message=dict()
		                #message[id_worker]=[x['job_id'],task['task_id'],task['duration']]
		                message=[x['job_id'],task['task_id'],task['duration']]
		                print(id_worker,message)
		                send_request(message, file_json['workers'][index]['port']) 
		                #print(file_json['workers'][index]['slots'])
		                file_json['workers'][index]['slots']=file_json['workers'][index]['slots']-1
		                
		                read_file = open(input_file,"w")
		                json.dump(file_json, read_file)
		                read_file.close()
		                
		                if x['job_id'] in d.keys():
		                    d[x['job_id']].append(task)
		                else:
		                    d[x['job_id']] = [task]       
		        #else:
		            #index=(index+1)%len(list)
		c.close()  
		
def update_from_workers():
	s = socket.socket()       
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
		file_json['workers'][index]['slots']=file_json['workers'][index]['slots']+1
		
		read_file = open(input_file,"w")
		json.dump(file_json, read_file)
		read_file.close()
		
		print('Completed', data[1])
		job_id = data[1][0]
		if job_id in d.keys():
			if data[1] in d[job_id]:
				d[job_id].remove(data[1])



try:
	t1=threading.Thread(target = read_tasks)
	t1.start()
	t2=threading.Thread(target = update_from_workers)
	t2.start()
except:
	print("Error: unable to start thread")

