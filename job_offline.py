import pandas as pd
import numpy as np
import warnings
import time
import random
random.seed(2018)
import csv
import copy
import math
from datetime import date, timedelta
from machine import *
from util import *
from schedule import *
#######

# job

#######

for k in range(0,8000):
    temp_cpu = machine_big_before[k].temp_cpu[:]
    temp_mem = machine_big_before[k].temp_mem[:]
    temp_cpu_whether =  machine_big_before[k].temp_cpu_whether[:]
    temp_mem_whether = machine_big_before[k].temp_mem_whether[:]
    machine_big_before[k].temp_cpu = []
    machine_big_before[k].temp_mem = []
    machine_big_before[k].temp_cpu_whether = []
    machine_big_before[k].temp_mem_whether = []
    machine_big_before[k].temp_cpu = trans1470(temp_cpu)
    machine_big_before[k].temp_mem = trans1470(temp_mem)
    machine_big_before[k].temp_cpu_whether = trans1470(temp_cpu_whether)
    machine_big_before[k].temp_mem_whether = trans1470(temp_mem_whether)
for k in range(0,8000):
    new_init(machine_big_before[k])

# cpu_job_dict mem_job_dict rate_job_dict count_job_dict time_job_dict 
# job_cate_dict pre_job_dict 
# job_put_dict
job_info = job_info.sort_values(['job_cate','count'],ascending=False)
job_info_values = job_info.values
job_cate = job_info[['job_cate']].drop_duplicates()['job_cate'].tolist()[:]
cpu_job_dict = dict() 
mem_job_dict = dict() 
count_job_dict = dict() 
time_job_dict = dict() 
job_cate_dict = dict() 
pre_job_dict = dict() 
job_put_dict = dict()

for i in range(job_info.shape[0]):
    job_id = job_info_values[i][0]
    cpu_job = job_info_values[i][1]
    mem_job = job_info_values[i][2]
    count_job = job_info_values[i][3]
    time_job = job_info_values[i][4]
    pre_job = job_info_values[i][5]
    cate_job = job_info_values[i][6]
    addtwodimdict_job(cpu_job_dict,job_id,cpu_job)
    addtwodimdict_job(mem_job_dict,job_id,mem_job)
    addtwodimdict_job(count_job_dict,job_id,count_job)
    addtwodimdict_job(time_job_dict,job_id,time_job)
    addtwodimdict_job(pre_job_dict,job_id,pre_job)
    addtwodimdict_job(job_cate_dict,cate_job,job_id)
    addtwodimdict_job(job_put_dict,cate_job,job_id) 
    pre_job_dict[job_id] = pre_job_dict[job_id][0].split(',')
    cpu_job_dict[job_id] = cpu_job
    mem_job_dict[job_id] = mem_job
    count_job_dict[job_id] = count_job
    time_job_dict[job_id] = time_job

job_id_list = job_info['job_id'].tolist()[:]
time_job_start = dict()
for k in range(len(job_id_list)):
    addtwodimdict_job(time_job_start,job_id_list[k],0)
    time_job_start[job_id_list[k]] = 0
time_cate_dict = dict()
for k in range(len(job_cate)):
    addtwodimdict_job(time_cate_dict,job_cate[k],0)
    time_cate_dict[job_cate[k]] = 0
    job_list = job_cate_dict[job_cate[k]][:]
    job_list_pre = []
    while(len(job_list)!=0):
        add_job = job_list[0]
        if pre_job_dict[add_job]==['']:
            job_list_pre.append(add_job)
            job_list.remove(add_job)
        else:
            if list_in_list(pre_job_dict[add_job],job_list_pre):
                job_list_pre.append(add_job)
                job_list.remove(add_job)
            else:
                job_list.remove(add_job)
                job_list.append(add_job)
    job_put_dict[job_cate[k]] = job_list_pre[:]
    
    
cpu_cate_dict = copy.deepcopy(time_cate_dict)
for i in range(len(job_cate)):
    for k in range(len(job_cate_dict[job_cate[i]])):
        cpu_cate_dict[job_cate[i]] = cpu_cate_dict[job_cate[i]] + count_job_dict[job_cate_dict[job_cate[i]][k]]*time_job_dict[job_cate_dict[job_cate[i]][k]]


job_cate.sort(mycmp)

##### one by one task
# job_cate job_put_dict

fail = True
fail_cate = []
cate_i = 0
max_cate = len(job_cate)
time_n = 0
fail_times = 0
# for cate_i in range(900,len(job_cate)):

f = open('task_a_offline.csv', 'w')
p = open('task_a_offline_test.csv', 'w') 
while(cate_i<max_cate):
    now_cate = job_cate[cate_i]
    f.write(now_cate)
    f.write('\n')
    f.write('---------------start----------------------')
    f.write('\n')
    print cate_i,now_cate,time.time()-time_n,len(job_put_dict[now_cate]),count_job_dict[job_put_dict[now_cate][0]]
    time_n = time.time()
    if fail_times==1:
        now_machine = random.sample(machine_4_5,350)
    elif fail_times==2:
        now_machine = random.sample(machine_all,500)
    elif fail_times==0:
        if cate_i<50:
            now_machine = random.sample(machine_45_5,200)
        elif cate_i<100:
            now_machine = random.sample(machine_4_5,200)
        else:
            now_machine = random.sample(machine_all,200)
    while((fail==True)&(len(job_put_dict[now_cate])!=0)):
        put_job = job_put_dict[now_cate][0]
        time_start = time_cate_dict[now_cate]
        time_long = time_job_dict[put_job]
        print time_start,time_long
#         each_machine_start = get_machine_start(time_start,time_long,now_machine)
        each_machine_start = []
        each_machine_start_all = get_machine_start_all(time_start,time_long,now_machine)
        for m in range(len(now_machine)):
            each_machine_start.append(each_machine_start_all[m][0])
        cpu_mem = True
        while(cpu_mem):
            min_index = each_machine_start.index(min(each_machine_start)) 
            if each_machine_start[min_index]!=1470:
                min_index_m = now_machine[min_index]
                cpu_may = 45 - max(machine_big_before[min_index_m].temp_cpu_whether[each_machine_start[min_index]:each_machine_start[min_index]+time_long])    
                mem_may = 287.0 - max(machine_big_before[min_index_m].temp_mem_whether[each_machine_start[min_index]:each_machine_start[min_index]+time_long])
                count_may_cpu = math.ceil(cpu_may/cpu_job_dict[put_job])
                count_may_mem = math.ceil(mem_may/mem_job_dict[put_job])
#                 print count_may_cpu*cpu_job_dict[put_job]+max(machine_big_before[min_index_m].temp_cpu_whether[each_machine_start[min_index]:each_machine_start[min_index]+time_long])
                if count_may_cpu > count_may_mem or count_may_cpu<=0:
                    each_machine_start_all[min_index].remove(each_machine_start[min_index])
                    if len(each_machine_start_all[min_index])==0:
                        each_machine_start[min_index] = 1470
                    else:
                        each_machine_start[min_index] = each_machine_start_all[min_index][0]
                else:
                    cpu_mem = False
                    if count_may_cpu <= count_job_dict[put_job]:
                        print put_job,count_job_dict[put_job],now_machine[min_index],each_machine_start[min_index] 
                        count_cpu = count_may_cpu
                        count_job_dict[put_job] = count_job_dict[put_job] - count_cpu
                        f.write(put_job)
                        f.write(',')
                        f.write(str(now_machine[min_index]))
                        f.write(',')
                        f.write(str(count_cpu))
                        f.write(',')
                        f.write(str(each_machine_start[min_index]))
                        f.write('\n')
                        if count_cpu!=0:
                            p.write(put_job)
                            p.write(',')
                            p.write('machine_')
                            p.write(str(1+transfer_machine[(now_machine[min_index])]))
                            p.write(',')
                            p.write(str(each_machine_start[min_index]))
                            p.write(',')
                            p.write(str(int(count_cpu)))
                            p.write('\n')
                        add_job_machine_whether(machine_big_before[min_index_m],each_machine_start[min_index],put_job,count_cpu)
#                         job_machine_whether_to_now(machine_big_before[min_index_m])
                    else:
                        print put_job,count_job_dict[put_job],now_machine[min_index],each_machine_start[min_index]

                        count_cpu = count_job_dict[put_job]
                        f.write(put_job)
                        f.write(',')
                        f.write(str(now_machine[min_index]))
                        f.write(',')
                        f.write(str(count_cpu))
                        f.write(',')
                        f.write(str(each_machine_start[min_index]))
                        f.write('\n')
                        if count_cpu!=0:
                            p.write(put_job)
                            p.write(',')
                            p.write('machine_')
                            p.write(str(1+transfer_machine[(now_machine[min_index])]))
                            p.write(',')
                            p.write(str(each_machine_start[min_index]))
                            p.write(',')
                            p.write(str(int(count_cpu)))
                            p.write('\n')
                        count_job_dict[put_job] = 0
                        add_job_machine_whether(machine_big_before[min_index_m],each_machine_start[min_index],put_job,count_cpu)
                        job_put_dict[now_cate].remove(put_job)
                        if len(job_put_dict[now_cate])!=0:
                            update_timecate(now_cate,job_put_dict[now_cate][0])
            else:
                print 'fail'
                fail = False
                cpu_mem = False
    if fail==True:
        f.write('---------------success----------------------')
        f.write('\n')
        print '---whether to now---'
        fail_times = 0
        for k in range(len(now_machine)):
            index_m = now_machine[k]
            job_machine_whether_to_now(machine_big_before[index_m])
        cate_i = cate_i + 1
    else:
        f.write('---------------fail----------------------')
        f.write('\n')
        print '===now to whether==='
        fail_times = fail_times+1
        print 'fail times: ',fail_times
        if fail_times==3:
            cate_i = cate_i + 1
            fail_cate.append(now_cate)
            print 'add fail_cate:',now_cate
            fail_times = 0
            
        for k in range(len(now_machine)):
            index_m = now_machine[k]
            job_machine_now_to_whether(machine_big_before[index_m])
        job_put_dict[now_cate] = job_put_dict_b[now_cate][:]
        for k in range(len(job_put_dict[now_cate])):
            count_job_dict[job_put_dict[now_cate][k]] = count_job_dict_b[job_put_dict[now_cate][k]]
        time_cate_dict[now_cate] = 0
        fail = True
p.close()  
f.close()
 
