# crawl_tiki_project
Basicly in normal logic we crawl one by one and it will take aproximately about 28 hours to done, instead of that I use asynio to increase speed crawl
## Total id success: 198940 id 
## Total fail: 1060, error 404 

Production ready incldue 
1. check duplicate
2. logging to know where batch checkpoint to resume
3. Restart when crash, electricity cut suddenly,... by supevisor
- run venv
- then run python3
[program:rerun_when_crash]
command=/bin/bash -c "source /home/nguye/Project_DEC_K20/venv/bin/activate && python3 /home/nguye/Project_DEC_K20/project_2/take_tik>
directory=/home/nguye/Project_DEC_K20/project_2
autostart=true
autorestart=true
stderr_logfile=/home/nguye/Project_DEC_K20/project_2/logs/my_app.err.log
stdout_logfile=/home/nguye/Project_DEC_K20/project_2/logs/my_app.out.log
user=nguye

4. Save error in another csv file
5. split to 4 chunk to increase speed and not casue memory overflow
