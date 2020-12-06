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
	
	worker1=[[],[]]
	worker2=[[],[]]
	worker3=[[],[]]
	
	count1=0
	count2=0
	count3=0
	for line in logs:
		date, time, word, task, wid = line.split()
		date_time=date+' '+time
		time=datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S,%f")
		if wid == '1':
			worker1[0].append(time)
			if word == 'tasks':
				count1=count1+1
				worker1[1].append(count1)
			if word == 'taskf':
				count1=count1-1
				worker1[1].append(count1)
		if wid == '2':
			
			worker2[0].append(time)
			if word == 'tasks':
				count2=count2+1
				worker2[1].append(count2)
			if word == 'taskf':
				count2=count2-1
				worker2[1].append(count2)
		if wid == '3':
			
			worker3[0].append(time)
			if word == 'tasks':
				count3=count3+1
				worker3[1].append(count3)
			if word == 'taskf':
				count3=count3-1
				worker3[1].append(count3)
				
	
	
	worker3[0]=[(ele-worker1[0][0]).total_seconds() for ele in worker3[0]]
	worker2[0]=[(ele-worker1[0][0]).total_seconds() for ele in worker2[0]]
	
	worker1[0]=[(ele-worker1[0][0]).total_seconds() for ele in worker1[0]]
	
	
	
	workers=[worker1,worker2,worker3]
	return workers
	




		
#------------------------------------------------------main---------------------------------------------------------

if len(sys.argv) >= 2:
	part = sys.argv[1]
else:
	print("No parameter has been included")
		
		
if part == '1':
	JOBS_RANDOM=analysis_part_1('jobs.log')
	TASKS_RANDOM=analysis_part_1('tasks.log')

	width=0.2
	figure, ax=plt.subplots()
	
	mean=[mean(TASKS_RANDOM),mean(JOBS_RANDOM)]
	median=[median(JOBS_RANDOM),median(TASKS_RANDOM)]
	labels=['Tasks','Jobs']
	x=np.arange(2)
	
	p = ax.bar(x-width/2, mean, width, label='Mean')
	q = ax.bar(x+width/2, median, width, label='Median')
	
	ax.set_ylabel('Values')
	ax.set_title('Analysis Task 1')
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
	workers=analysis_part_2("tasks.log")
	plt.figure(figsize=(40,10))
	x1 = workers[0][0]
	y1 = workers[0][1]
	plt.plot(x1, y1, label = "worker 1")
	x2 = workers[1][0]
	y2 = workers[1][1]
	plt.plot(x2, y2, label = "worker 2")
	x3 = workers[2][0]
	y3 = workers[2][1]
	plt.plot(x3, y3, label = "worker 3")
	plt.xlabel('time(in seconds)')
	plt.ylabel('Number of tasks scheduled')
	plt.title('Number of tasks scheduled per worker in time ')
	plt.legend()
	plt.show()
		
	

