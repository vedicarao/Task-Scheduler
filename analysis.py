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
	
	worker1=[]
	worker2=[]
	worker3=[]
	
	for line in logs:
		date, time, word, task, wid = line.split()
		date_time=date+' '+time
		time=datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S,%f")
		if word == 'tasks':
			if wid == '1':
				worker1.append(time)
			if wid == '2':
				worker2.append(time)
			if wid == '3':
				worker3.append(time)
	worker1_new=[(ele-worker1[0]).total_seconds()*100 for ele in worker1]
	worker2_new=[(ele-worker2[0]).total_seconds()*100 for ele in worker2]
	worker3_new=[(ele-worker3[0]).total_seconds()*100 for ele in worker3]
	
	
	
	workers=[[worker1_new,list(range(1,len(worker1_new)+1))],[worker2_new,list(range(1,len(worker2_new)+1))],[worker3_new,list(range(1,len(worker3_new)+1))]]
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

	
	worker_id = int(input("Enter worker id:"))
	if (worker_id in range(1,4)):
		analysis_2_random=analysis_part_2('tasks.log')
		plt.title("Analysis Task 2- Worker {}".format(worker_id))

		plt.plot(analysis_2_random[worker_id-1][0],analysis_2_random[worker_id-1][1])
		plt.xlabel("Time in milliseconds")
		plt.ylabel("Number of tasks scheduled")
		plt.gcf().autofmt_xdate()
		plt.show()
	

