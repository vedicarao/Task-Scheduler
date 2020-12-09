from datetime import datetime
from datetime import timedelta
import sys
import matplotlib.pyplot as plt
import numpy as np
#---------------------------------------analysis part 1---------------------------------------------------------------------------------

#-----------------------------------------mean,median functions---------------------------------------------------------------------
def mean(n_list):
	n=len(n_list)
	get_sum=sum(n_list)
	return get_sum /n
  

def median(n_list):
	n = len(n_list) 
	n_list.sort() 
  
	if n%2 == 0: 
    		median1 = n_list[n//2] 
    		median2 = n_list[n//2 - 1] 
    		median = (median1+median2)/2
	else: 
    		median = n_list[n//2] 
	return median
	

#------------------------------------------------------calculating time deltas--------------------------------------------------------
def analysis_part_1(file):
	logs = open(file,"r")
	logs=logs.readlines()

	starts=[]
	ends=[]


	for line in logs:
		x=line.strip().split()
		date=x[0]
		time=x[1]
		date_time=date+' '+time
		word=x[2]
		job_task=x[3]
		time=datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S,%f")
		if word == 'jobs' or word == 'tasks':
			starts.append(time)
		if word == 'jobf' or word == 'taskf':
			ends.append(time)


	diffs=[]
	for i in range(len(starts)):
		time_delta=ends[i]-starts[i]
		diffs.append(time_delta.total_seconds())
	return diffs


#-------------------------------------------------------------analysis part2-----------------------------------------------------------

def analysis_part_2(file):
	logs = open(file,"r")
	logs=logs.readlines()
	
	worker=dict()
	worker = [[],[]]
	count=0
	for line in logs:
		date, time, word, task, wid = line.split()
		date_time=date+' '+time
		time=datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S,%f")
		'''
		if word == 'tasks':
			count=count+1
			worker[time]=count
		if word == 'taskf':
			count=count-1
			worker[time]=count
		'''	
		worker[0].append(time)
		if word == 'tasks':
			count=count+1
			worker[1].append(count)
		if word == 'taskf':
			count=count-1
			worker[1].append(count)
			
	
	return worker
	




		
#------------------------------------------------------main---------------------------------------------------------

if len(sys.argv) >= 2:
	part = sys.argv[1]
else:
	print("No parameter has been included")
		
		
if part == '1':
	JOBS_RANDOM=analysis_part_1('jobs.log')
	TASKS_RANDOM=analysis_part_1('tasks_1.log')+ analysis_part_1('tasks_2.log')+ analysis_part_1('tasks_3.log')
	print(JOBS_RANDOM)
	print(TASKS_RANDOM)

	width=0.2
	figure, ax=plt.subplots()
	
	mean=[mean(TASKS_RANDOM),mean(JOBS_RANDOM)]
	median=[median(JOBS_RANDOM),median(TASKS_RANDOM)]
	labels=['Tasks','Jobs']
	x=np.arange(2)
	
	p = ax.bar(x-width/2, mean, width, label='Mean')
	q = ax.bar(x+width/2, median, width, label='Median')
	
	ax.set_ylabel('Values')
	ax.set_title('Mean and Median')
	ax.set_xticks(x)
	ax.set_xticklabels(labels)
	ax.legend()
	
	
	def autolabel(rects):
		for rect in rects:
			height = rect.get_height()
			ax.annotate('{}'.format(height),xy=(rect.get_x() + rect.get_width()/2, height),xytext=(0,3),textcoords="offset points",ha='center', va='bottom')
		
	autolabel(p)
	autolabel(q)
	figure.tight_layout()
	plt.show()

if part == '2':
	worker1=analysis_part_2("tasks_1.log")
	worker2=analysis_part_2("tasks_2.log")
	worker3=analysis_part_2("tasks_3.log")
	'''
	worker={**worker1, **worker2, **worker3}
	worker_new={key: value for key, value in sorted(worker.items(), key=lambda item: item[0])}

	worker_times={key-worker.items[0][0], value for key, value in worker_new}
	'''
	
	
	min_time=min(worker1[0][0],worker2[0][0],worker3[0][0])
	worker1[0]=[(ele-min_time).total_seconds() for ele in worker1[0]]
	worker2[0]=[(ele-min_time).total_seconds() for ele in worker2[0]]
	worker3[0]=[(ele-min_time).total_seconds() for ele in worker3[0]]
	worker=[worker1,worker2,worker3]
	print(worker)
	
	plt.figure(figsize=(40,10))
	x1 = worker[0][0]
	y1 = worker[0][1]
	plt.plot(x1, y1, label = "worker 1")
	x2 = worker[1][0]
	y2 = worker[1][1]
	plt.plot(x2, y2, label = "worker 2")
	x3 = worker[2][0]
	y3 = worker[2][1]
	plt.plot(x3, y3, label = "worker 3")
	plt.xlabel('time(in seconds)')
	plt.ylabel('Number of tasks scheduled')
	plt.title('Number of tasks scheduled per worker in time ')
	plt.legend()
	plt.show()
		
	

