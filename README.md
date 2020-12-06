# BD_Project
Implements simulation of a scheduling framework    

To run project-  
Run in separate terminals  
python3 worker.py 4000 1  
python3 worker.py 4001 2  
python3 worker.py 4002 3  
python3 master.py config.json RANDOM/RR/LL  
python3 requests.py n  

The terminal running master.py displays the tasks scheduled and the tasks completed along with worker_id  
The terminals running worker.py displays the tasks they are scheduled as they are completed.  

On running this setup 2 logfiles are generated- jobs.log and tasks.log, with the jobs and tasks logged with their start and end times respectively.  
'jobs' indicating job start  
'jobf' indicating job finish  
'tasks' indicating task start  
'taskf' indicating task finish  

The analysis.py which performs analysis of the scheduled framework run will compute based on the log files jobs.log and tasks.log, hence provides analysis of the  
latest run of the master.py and worker.py's, the latest task scheduling.  

Analysis:  
analysis.py has 2 tasks, the first one displays a bar graph displaying the mean and median task and job times calculated from the logged file data.  
the second task plots the number of tasks scheduled on each machine, against time, for each scheduling algorithm.  
Hence it displays a plot with 3 lines one for each worker, showing the number of tasks scheduled on that machine, over time in seconds from the beginning of execution.  

To run the first task in analysis.py type in terminal-  
python3 analysis.py 1  

To run the second task in analysis.py type in terminal-  
python3 analysis.py 2  
  

