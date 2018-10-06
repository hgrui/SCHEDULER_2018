import pandas as pd
import numpy as np
import gc
import matplotlib.pyplot as plt
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

#
# load data
#
app_interference = pd.read_csv("semifinal/app_interference.csv",header=None)
app_interference.columns=['app_x','app_y','inst_num']
app_resources = pd.read_csv("semifinal/app_resources.csv",header=None)
app_resources.columns=['app_id','cpu','mem','disk','P','M','PM']
app_resources['cpu_num'] = app_resources.cpu.apply(lambda s:len(s.split('|')))
app_resources['mem_num'] = app_resources.mem.apply(lambda s:len(s.split('|')))
for i in range(98):
    app_resources['cpu_'+str(i)] = app_resources.cpu.apply(lambda s:(s.split('|'))[i])
    app_resources['mem_'+str(i)] = app_resources.mem.apply(lambda s:(s.split('|'))[i])

app_resources = app_resources.drop('cpu',axis=1)
app_resources = app_resources.drop('mem',axis=1)

job_info_csv = []
csv_reader = csv.reader(open("semifinal/job_info.a.csv"))
for row in csv_reader:
    job_info_csv.append([row[0],row[1],row[2],row[3],row[4],','.join(row[5:])])
job_info = pd.DataFrame(columns=['job_id','cpu','mem','count','time','pre_id'],data=job_info_csv)

instance_deploy = pd.read_csv("semifinal/instance_deploy.a.csv",header=None)
instance_deploy.columns=['inst_id','app_id','inst_machine']
instance_deploy = instance_deploy.merge(app_resources,on=['app_id'],how='left')
machine_resources = pd.read_csv("semifinal/machine_resources.a.csv",header=None)
machine_resources.columns=['machine_id','cpu_max','mem_max','disk_max','P_max','M_max','PM_max']

#app interference
#pandas DataFrame
start = time.time()
app_name = app_resources['app_id'].tolist()
app_interference_matrix = pd.DataFrame(-1,columns=app_name,index=app_name)
for i in range(app_interference.shape[0]):
    app_interference_matrix.loc[app_interference[app_interference.index==i]['app_x'][i],app_interference[app_interference.index==i]['app_y'][i]]=app_interference[app_interference.index==i]['inst_num'][i]
    if i%2000==0:
        print(i)
end = time.time()
print(end-start)

cpu_total_plt = []
for i in range(98):
    cpu_total_plt.append(instance_deploy['cpu_'+str(i)].astype(float).sum())

machine_big_before = []
for i in range(8000):
    machine_big_before.append(MACHINE(92,288,2457,7,7,9,2000))


with_machine_inst = []
without_machine_inst = []
instance_machine = instance_deploy[['inst_id','app_id','inst_machine','disk']]
instance_machine['machine_num'] = instance_machine.inst_machine.apply(lambda x:(x.split('_'))[1] if x>'machine_0' else 0)


nan_num = 0
for id_index in range(instance_machine.shape[0]):
    if id_index%1000 == 0:
        temp_time = time.time()
    temp_instance_machine = instance_machine.loc[id_index:id_index].reset_index().copy()
    temp_id_num = int(temp_instance_machine.machine_num[0])
    if ((temp_id_num>0)&(temp_id_num<=8000)):
        with_machine_inst.append(instance_machine['inst_id'][id_index])
        machine_big_before[temp_id_num-1].instance_add_yes_no(temp_instance_machine['inst_id'][0])
    else:
        without_machine_inst.append(instance_machine['inst_id'][id_index])
        nan_num = nan_num+1
print(id_index,nan_num)

for i in range(8000):
    machine_big_before[i].before_to_now()

machine_big_before_before = copy.deepcopy(machine_big_before)

app_cpu_dict = dict()
for i in range(len(instance_deploy_app)):
    instance_deploy_app_s = instance_deploy_app.loc[i]
    key_a = instance_deploy_app_s['app_id']
    key_b = []
    for j in range(98):
        key_b.append(float(instance_deploy_app_s['cpu_'+str(j)]))
    addtwodimdict_cpu(app_cpu_dict,key_a,key_b)

app_dict = dict()
inst_type_app_dict = dict()
mem_app_dict = dict()
cpu_app_dict = dict()
disk_type = [40,60,80,100,120,150,167,180,200,250,300,500,600,650,1000,1024]
rate_app_dict = dict()
disk_app_dict = dict()
instance_values =  instance_deploy.values
instance_deploy_app_values = instance_deploy_app.values
for i in range((instance_deploy.shape[0])):
    inst_id_dict = instance_values[i][0]
    inst_app_dict = instance_values[i][1]
    addtwodimdict_inst(app_dict,inst_app_dict,inst_id_dict)
for i in range(instance_deploy_app.shape[0]):
    inst_app = instance_deploy_app_values[i][0]
    mem_app = instance_deploy_app_values[i][205]
    cpu_app = instance_deploy_app_values[i][204]
    rate_app = instance_deploy_app_values[i][206]
    inst_disk = instance_deploy_app_values[i][2]
    disk_rate = inst_disk/cpu_app
    addtwodimdict_inst(inst_type_app_dict,inst_disk,inst_app)
    addtwodimdict_inst(mem_app_dict,inst_app,mem_app)
    addtwodimdict_inst(cpu_app_dict,inst_app,cpu_app)
    addtwodimdict_inst(rate_app_dict,inst_app,rate_app)
    addtwodimdict_inst(disk_app_dict,inst_app,inst_disk)
app_list = instance_deploy_app['app_id'].tolist()[:]

instance_deploy_app = instance_deploy[['app_id']]
instance_deploy_app['app_num']=1
instance_deploy_app = instance_deploy_app.groupby(['app_id'])['app_num'].sum().reset_index()
instance_deploy_app = instance_deploy_app.merge(app_resources,on=['app_id'],how='left')
instance_deploy_app['cpu_rand'] = (instance_deploy_app.cpu_93.astype(float) + instance_deploy_app.cpu_78.astype(float) + instance_deploy_app.cpu_43.astype(float) + instance_deploy_app.cpu_23.astype(float) + instance_deploy_app.cpu_3.astype(float))/5  
instance_deploy_app['mem_rand'] = (instance_deploy_app.mem_93.astype(float) + instance_deploy_app.mem_78.astype(float) + instance_deploy_app.mem_43.astype(float) + instance_deploy_app.mem_23.astype(float) + instance_deploy_app.mem_3.astype(float))/5  
instance_deploy_app['rate']  = instance_deploy_app.mem_rand / instance_deploy_app.cpu_rand

instance_deploy_app_con_x = instance_deploy_app[['app_id','app_num']]
instance_deploy_app_con_x.columns = ['app_x','app_x_num']
instance_deploy_app_con_y = instance_deploy_app[['app_id','app_num']]
instance_deploy_app_con_y.columns = ['app_y','app_y_num']
app_interference = app_interference.merge(instance_deploy_app_con_x,on=['app_x'],how='left')
app_interference = app_interference.merge(instance_deploy_app_con_y,on=['app_y'],how='left')
app_con_x = app_interference[['app_x','app_y_num']]
app_con_x = app_con_x.groupby(['app_x'])['app_y_num'].sum().reset_index()
app_con_x_list = app_con_x[app_con_x.app_y_num>3000].sort_values('app_y_num',ascending=False)['app_x'].tolist()[:]
only_one = app_interference[(app_interference.app_x == app_interference.app_y)&(app_interference.inst_num==0)]['app_x'].tolist()[:]

cpu_total_plt_46 = []
for i in range(98):
    cpu_total_plt_46.append(cpu_total_plt[i]/3650) 

app_small_try =[]
for i in range(len(app_dict)):
    if len(app_dict[app_list[i]])!=0:
        app_small_try.append(app_list[i])
rate_small = app_small_try[:]
rate_big = []


machine_big_before = []
for i in range(8000): 
    machine_big_before.append(MACHINE(92,288,2457,7,7,9,2000))

# app_dict mem_app_dict cpu_app_dict  # instance_big_app
random.seed(1995)
docker_index = 0
remove_app = []
f = open('big_docker_semifnal.csv', 'w') 

temp_time = 0
while(docker_index<5000):
    print ('                  docker_index',docker_index)
#     for inst_index in range(len(need_inst_disk)):
    mem_now = 1.0
    cpu_now = 1.0
    f.write('docker: ')
    f.write(str(docker_index))
    f.write('\n')
    inst_index = 0
    init_times = 0
    total_times = 0
    next_one = True
    small_try=0
    big_try=0
    small_done = True
    big_done = True
    one_times = 0
    larger_rate = False
    
    
    if next_one==True:
        print(time.time()-temp_time)
        temp_time = time.time()
    while(next_one):
        mem_cpu_rate =  (255.0-max(machine_big_before[docker_index].temp_mem))/(47.0-max(machine_big_before[docker_index].temp_cpu))
        if mem_cpu_rate<=0:
            mem_cpu_rate=6
        if(len(rate_small)!=0):
            if (len(rate_small)==1)|(total_times==0):
                app_index = 0
                print(rate_small[app_index])
            else:
                cpu_total_plt_46_temp = cpu_total_plt_46[:]
                cpu_temp = list(map(lambda x: x[0]-x[1], zip(cpu_total_plt_46_temp, machine_big_before[docker_index].temp_cpu)))
                if len(rate_small)>61:
                    choose_index = random.sample(range(0,len(rate_small)),60)
                else:
                    choose_index = random.sample(range(0,len(rate_small)),len(rate_small)-1)
                score_index = []
                cpu_rest = 47.5 - max(machine_big_before[docker_index].temp_cpu)
                bascore = []
                bascore.append(random.randint(0,len(rate_small)-1))
                for k in range(len(choose_index)):
               
                    if (rate_app_dict[rate_small[choose_index[k]]][0]>=(mem_cpu_rate-4))&(rate_app_dict[rate_small[choose_index[k]]][0]<=(mem_cpu_rate+4)):
                        score_index.append(cosine(app_cpu_dict[rate_small[choose_index[k]]],cpu_temp))
                        bascore.append(choose_index[k])
                    else:
                        score_index.append(cosine(app_cpu_dict[rate_small[choose_index[k]]],cpu_temp)-5)
                      
                        
                if one_times<15:
                    app_index = choose_index[score_index.index(max(score_index))]
                elif (one_times<20)&(one_times>=15):
                    if (len(score_index)>=2):
                        choose_index.remove(choose_index[score_index.index(max(score_index))])
                        score_index.remove(max(score_index))
                    app_index = choose_index[score_index.index(max(score_index))]
                elif (one_times<25)&(one_times>=20):
                    if (len(score_index)>=3):
                        choose_index.remove(choose_index[score_index.index(max(score_index))])
                        score_index.remove(max(score_index))
                        choose_index.remove(choose_index[score_index.index(max(score_index))])
                        score_index.remove(max(score_index))
                    app_index = choose_index[score_index.index(max(score_index))]
                else:
                    app_index = choose_index[score_index.index(max(score_index))]
            app_name = rate_small[app_index]
            inst_here = app_dict[app_name][0]
        f.write(str(init_times))
        f.write(',')
        f.write(app_name)
        f.write(',')
        f.write(inst_here)
        f.write('\n')
        machine_big_before[docker_index].instance_add_yes_no(inst_here)
        cpu_total_plt_46_temp = cpu_total_plt_46[:]
        cpu_temp_whether = list(map(lambda x: x[0]-x[1], zip(cpu_total_plt_46_temp, machine_big_before[docker_index].temp_cpu_whether)))

        if ((inst_confilct_before_more_for_92(machine_big_before[docker_index])))&(machine_big_before[docker_index].temp_mem_whether[0]<280)&(max(machine_big_before[docker_index].temp_cpu_whether)<90):
            app_dict[app_name].remove(inst_here)
            inst_index = inst_index + 1
            machine_big_before[docker_index].instance_add(inst_here)
            one_times = 0
            init_times = init_times + 1
#             mem_now = 0
#             cpu_now = 0
#             for k in range(len(machine_big_before[docker_index].app_already)):
#                 app_name_k = machine_big_before[docker_index].app_already[k]
#                 mem_now = mem_now + mem_app_dict[app_name_k][0]
#                 cpu_now = cpu_now +cpu_app_dict[app_name_k][0]
 
        else:
            machine_big_before[docker_index].now_to_whether()
            one_times = one_times + 1
 
            f.write(str(machine_big_before[docker_index].temp_cpu[0]))
            f.write(',')
            f.write(str(machine_big_before[docker_index].temp_mem[0]))
            f.write(',')
            f.write(str(machine_big_before[docker_index].P_conflic_before))
            f.write(',')
            f.write(str(machine_big_before[docker_index].M_conflic_before))
            f.write(',')
            f.write(str(machine_big_before[docker_index].PM_conflic_before))
            f.write(',')
            f.write(str(machine_big_before[docker_index].disk_conflic_before))
            f.write(',')
            f.write(str(machine_big_before[docker_index].more_cpu_all_conflic_before))
            f.write(',')
            f.write(str(machine_big_before[docker_index].mem_all_conflic_before))
            f.write(',')
            f.write(str(machine_big_before[docker_index].app_conflic_before))
            f.write(',')
            f.write(str(min(cpu_temp_whether)))
            f.write(',')
            f.write(str(cosine(cpu_total_plt_46,machine_big_before[docker_index].temp_cpu)))
            f.write('\n')
        
        if len(app_dict[app_name])==0:
            print('delete app',app_name)
            if app_name in rate_big:
                rate_big.remove(app_name)
            else:
                rate_small.remove(app_name)
            remove_app.append(app_name)
    
        total_times = total_times + 1
        if (one_times > 30)|(init_times==80):
            print(init_times,len(machine_big_before[docker_index].app_already),machine_big_before[docker_index].temp_disk,max(machine_big_before[docker_index].temp_cpu),machine_big_before[docker_index].temp_cpu[0],machine_big_before[docker_index].temp_mem[0])
            next_one = False  
#             docker_index = 1999 - docker_index
            docker_index = docker_index + 1
        if (len(rate_small)==0)&(len(rate_big)==0):
            next_one = False  
            docker_index = 6000
    print(len(rate_small),get_sum(app_dict))

f.close()


f = open('fusai_big_a.csv', 'w') 
kk = 0
for d_index in range(8000):
    
    for j in range(len(machine_big_before[d_index].app_already_inst)):
        kk = kk+1
        f.write(machine_big_before[d_index].app_already_inst[j])
        f.write(',')
        f.write('machine_')
        f.write(str(d_index+1))
        f.write('\n')
#     print(without_machine_inst[d_index],temp_ans)
f.close()
print(kk)

