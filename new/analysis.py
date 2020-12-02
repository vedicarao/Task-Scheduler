from datetime import datetime
from datetime import timedelta


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

	#time_format="%H:%M:%S,%f"

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
	#print('starts',starts)
	#print('ends',ends)
	return diffs

#------------------------------------------------calling mean,median-----------------------------------------------------------------
JOBS_RANDOM=analysis_part_1('jobs.log')
TASKS_RANDOM=analysis_part_1('tasks.log')

#print(JOBS_RANDOM)
#print(TASKS_RANDOM)


#print('mean of jobs',mean(JOBS_RANDOM))
#print('median of jobs',median(JOBS_RANDOM))

#print('mean of tasks',mean(TASKS_RANDOM))
#print('median of tasks',median(TASKS_RANDOM))


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
	

analysis_2_random=analysis_part_2('tasks.log')
print(analysis_2_random)

import matplotlib.pyplot as plt
plt.plot(analysis_2_random[0][0],analysis_2_random[0][1])
plt.gcf().autofmt_xdate()

plt.show()

		
		
		
		
	




