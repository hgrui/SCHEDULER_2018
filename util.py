import pandas as pd
import numpy as np
import gc
import time
import random
random.seed(2018)
from datetime import date, timedelta


def addtwodimdict_inference(thedict, key_a, key_b, val):
    if key_a in thedict:
        if key_b in thedict[key_a]:
            thedict[key_a][key_b] = thedict[key_a][key_b]+1
        else:
            thedict[key_a].update({key_b: val})
    else:
        thedict.update({key_a:{key_b: val}})   

def addtwodimdict_cpu(thedict, key_a, key_b):
    thedict.update({key_a:key_b})


def cosine_similarity(vector1, vector2):
    dot_product = 0.0
    normA = 0.0
    normB = 0.0
    for a, b in zip(vector1, vector2):
        dot_product += a * b
        normA += a ** 2
        normB += b ** 2
    if normA == 0.0 or normB == 0.0:
        return 0
    else:
        return round(dot_product / ((normA**0.5)*(normB**0.5)) * 100, 2)
     
def cosine(vec1, vec2):
    return cosine_similarity(vec1, vec2)


def get_sum(app_dict):
    k = 0
    for i in range(len(app_dict)):
        k = k+len(app_dict[app_list[i]])
    return k

def addtwodimdict_inst(thedict, key_a, key_b):
    if key_a in thedict:
        thedict[key_a].append(key_b)
    else:
        thedict.update({key_a:[key_b]})

def inst_confilct_before_more_for_92(self):
    score_single = 0
    for j in range(len(self.temp_cpu_whether)):
        score_single = score_single + 1+(len(self.app_already_whether)+1)*(math.exp(max(0,(self.temp_cpu_whether[j]/92.0)-0.5))-1)
    score_single = score_single/(len(self.temp_cpu_whether))
    for i in range(98):
        self.mem_conflic[i] = 1*(not(self.temp_mem_whether[i]<=self.mem_max))
    self.P_conflic_before = 1*(self.temp_P_whether<=self.P_max)
    self.M_conflic_before = 1*(self.temp_M_whether<=self.M_max)
    self.PM_conflic_before = 1*(self.temp_PM_whether<=self.PM_max)
    self.disk_conflic_before = 1*(self.temp_disk_whether<=self.disk_max)
    self.more_cpu_all_conflic_before = 1*(score_single<=1.05)
    self.mem_all_conflic_before = 1*(not(sum(self.mem_conflic)))
    self.app_conflic_before = 1*(self.machine_app_interfernce_before_obj_dict())
    #print(P_conflic,M_conflic,PM_conflic,disk_conflic,cpu_all_conflic,mem_all_conflic,app_conflic)
    if ((self.P_conflic_before)&(self.M_conflic_before)&(self.PM_conflic_before)&(self.disk_conflic_before)&(self.more_cpu_all_conflic_before)&(self.mem_all_conflic_before)&(self.app_conflic_before)):
        return True
    else:
        return False        


# job function
def trans1470(list_0):
    list1 = list_0[:]
    list2 = []
    for i in range(len(list1)):
        for j in range(15):
            list2.append(list1[i])
    list_0 = list2[:]
    return list_0

def new_init(self):
    self.cpu_conflic = []
    self.half_cpu_conflic = []
    self.more_cpu_conflic = []
    self.mem_conflic = []
    for i in range(1470):
        self.cpu_conflic.append(0)
        self.half_cpu_conflic.append(0)
        self.more_cpu_conflic.append(0)
        self.mem_conflic.append(0)

def mycmp(cate1,cate2):
    if cpu_cate_dict[cate1] < cpu_cate_dict[cate2]:
        return 1
    else:
        return -1

# cpu_job_dict mem_job_dict rate_job_dict count_job_dict time_job_dict 
# job_cate_dict pre_job_dict 
# job_put_dict
# job_catecpu

def add_job_machine_whether(machine,start,job_id,count):
    pass_card = True
    if (start+time_job_dict[job_id])>time_job_start[job_id]:
        time_job_start[job_id] = start+time_job_dict[job_id]
    for i in range(time_job_dict[job_id]):
        machine.temp_cpu_whether[start+i] = machine.temp_cpu_whether[start+i] + cpu_job_dict[job_id]*count
        machine.temp_mem_whether[start+i] = machine.temp_mem_whether[start+i] + mem_job_dict[job_id]*count
#         if machine.temp_cpu_whether[start+i]>46 or machine.temp_mem_whether[start+i]>287:
#             pass_card = False
    return pass_card

def job_machine_whether_to_now(machine):
    machine.temp_cpu = machine.temp_cpu_whether[:]
    machine.temp_mem = machine.temp_mem_whether[:]
    
def job_machine_now_to_whether(machine):
    machine.temp_cpu_whether = machine.temp_cpu[:]
    machine.temp_mem_whether = machine.temp_mem[:]
    
def machine_ok_time(machine):
    part = []
    ans_ok = []
    for k in range(len(machine.temp_cpu)):
        if machine.temp_cpu_whether[k]<44.85 and machine.temp_mem_whether[k]<287:
            part.append(k)
        else:
            if (len(part)!=0):
                ans_ok.append([min(part),max(part)])
            part = []
    if (len(part)!=0):
        ans_ok.append([min(part),max(part)])
    return ans_ok

def update_timecate(now_cate,now_job):
    time_cate_dict[now_cate] = 0
    pre_job = pre_job_dict[now_job][:] 
    if pre_job==['']:
        time_cate_dict[now_cate] = 0
    else:
        for k in range(len(pre_job)):
            job_start = time_job_start[pre_job[k]]
            if job_start>time_cate_dict[now_cate] :
                print 'now-pre',now_job,pre_job[k],job_start
                time_cate_dict[now_cate] = job_start


def get_machine_start_all(time_start,time_long,now_machine):
    each_machine_start = []
    for k_i in range(len(now_machine)):
        k = now_machine[k_i]
        each_machine_start.append([])
        ok_time = machine_ok_time(machine_big_before[k])
        j = 0
        while(j<len(ok_time)):
            ok_long = ok_time[j][1]-ok_time[j][0]
            if (ok_long>=time_long)&(ok_time[j][0]>=time_start):
                each_machine_start[k_i].append(ok_time[j][0])
                j = j+1
            elif ((ok_long-(time_start-ok_time[j][0]))>time_long)&(time_start>ok_time[j][0]):
                each_machine_start[k_i].append(time_start)
                j = j+1
            else:
                j = j+1
                if j == len(ok_time):
                    each_machine_start[k_i].append(1470)
    return each_machine_start

#
# qianyi k=3
#
def find_new_machine(inst_here):
    index_machine = -1
    for i in range(3000):
        if inst_here in big_now[i].app_already_inst:
            index_machine = 6000+i
    for i in range(6000):
        if inst_here in small_now[i].app_already_inst:
            index_machine = i
    if index_machine==-1:
        print('why, what happened')
    return index_machine

def find_old_machine(inst_here):
    index_machine = -1
    for i in range(3000):
        if inst_here in big_before[i].app_already_inst:
            index_machine = 6000+i
    for i in range(6000):
        if inst_here in small_before[i].app_already_inst:
            index_machine = i
    if index_machine==-1:
        print('why, what happened')
    return index_machine
#exchange

def machine_change(self,old,new):
    old_inst = self[old].app_already_inst[:]
    new_inst = self[new].app_already_inst[:]
    self[old].init_machine()
    self[new].init_machine()
    for k in range(len(old_inst)):
        self[new].instance_add_yes_no(old_inst[k])
    for j in range(len(new_inst)):
        self[old].instance_add_yes_no(new_inst[j])
    self[old].before_to_now()
    self[new].before_to_now()

def inst_change(old_machine,old_inst_single,new_machine,new_inst_single):
    old_inst = old_machine.app_already_inst[:]
    new_inst = new_machine.app_already_inst[:]
    old_inst.remove(old_inst_single)
    old_inst.append(new_inst_single)
    new_inst.remove(new_inst_single)
    new_inst.append(old_inst_single)
    old_machine.init_machine()
    new_machine.init_machine()
    for k in range(len(old_inst)):
        old_machine.instance_add_yes_no(old_inst[k])
    for j in range(len(new_inst)):
        new_machine.instance_add_yes_no(new_inst[j])
    old_machine.before_to_now()
    new_machine.before_to_now()
    
# compare app
def compare_app(list1,list2):
    ans = 0
    for i in range(len(list1)):
        if list1[i] in list2:
            list2.remove(list1[i])
            ans = ans+1
    return ans

def addtwodimdict_inst_app(thedict, key_a):
    if key_a not in thedict:
         thedict.update({key_a:[]})
 
