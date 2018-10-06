import pandas as pd
import numpy as np
import gc
import time
import random
random.seed(2018)
from datetime import date, timedelta
from util import *


class MACHINE(object):
    def __init__(self,cpu_max,mem_max,disk_max,P_max,M_max,PM_max,numbers):
        self.cpu_max = cpu_max
        self.mem_max = mem_max
        self.disk_max = disk_max
        self.P_max = P_max
        self.M_max = M_max
        self.PM_max = PM_max
        self.numbers = numbers
        #specific
        self.app_already=[]
        self.app_already_whether=[]
        self.app_already_inst=[]
        self.app_already_whether_inst=[]
        self.temp_P = 0
        self.temp_M = 0
        self.temp_PM = 0
        self.temp_disk = 0
        self.temp_P_whether = 0
        self.temp_M_whether = 0
        self.temp_PM_whether = 0
        self.temp_disk_whether = 0
        
        self.temp_cpu = []
        self.temp_mem = []
        self.temp_cpu_whether = []
        self.temp_mem_whether = []
        self.cpu_conflic = []
        self.mem_conflic = []
        self.index_here = []
        
        self.P_conflic = 1 
        self.M_conflic = 1 
        self.PM_conflic = 1 
        self.disk_conflic = 1 
        self.cpu_all_conflic = 1
        self.mem_all_conflic = 1 
        self.app_conflic = 1
        
        self.P_conflic_before = 1 
        self.M_conflic_before = 1 
        self.PM_conflic_before = 1 
        self.disk_conflic_before = 1 
        self.cpu_all_conflic_before = 1 
        self.mem_all_conflic_before = 1 
        self.app_conflic_before = 1 
        
        self.before_conflict = []
        self.before_conflict_app = []
        
        self.half_cpu_conflic = []
        self.half_cpu_all_conflic_before= 1
        self.half_cpu_all_conflic = 1
        
        
        self.more_cpu_conflic = []
        self.more_cpu_all_conflic_before= 1
        self.more_cpu_all_conflic = 1
        
        for i in range(98):
            self.temp_cpu.append(0)
            self.temp_mem.append(0)
            self.temp_cpu_whether.append(0)
            self.temp_mem_whether.append(0)
            self.cpu_conflic.append(0)
            self.half_cpu_conflic.append(0)
            self.more_cpu_conflic.append(0)
            self.mem_conflic.append(0)
    def machine_use(self):
        self.numbers = self.numbers-1
#     def instance_add_yes_no(self,inst_id):
#         temp_instance_deploy = instance_deploy[instance_deploy.inst_id==inst_id].reset_index().copy()
#         app_id_new = temp_instance_deploy['app_id'][0]
#         P_new = temp_instance_deploy['P'][0]
#         M_new = temp_instance_deploy['M'][0]
#         PM_new = temp_instance_deploy['PM'][0]
#         disk_new = temp_instance_deploy['disk'][0]
        
#         self.app_already_whether_inst.append(inst_id)
#         self.app_already_whether.append(app_id_new)
#         self.temp_P_whether = self.temp_P_whether + P_new
#         self.temp_M_whether = self.temp_M_whether + M_new
#         self.temp_PM_whether = self.temp_PM_whether + PM_new
#         self.temp_disk_whether = self.temp_disk_whether + disk_new
        
#         for i in range(98):
#             dif_temp_cpu_new = float(temp_instance_deploy['cpu_'+str(i)][0])
#             dif_temp_mem_new = float(temp_instance_deploy['mem_'+str(i)][0])
#             self.temp_cpu_whether[i] = self.temp_cpu_whether[i] +dif_temp_cpu_new
#             self.temp_mem_whether[i] = self.temp_mem_whether[i] + dif_temp_mem_new
#             self.cpu_conflic[i] = 1*(not(self.temp_cpu_whether[i]<=self.cpu_max))
#             self.mem_conflic[i] = 1*(not(self.temp_mem_whether[i]<=self.mem_max))
    def instance_add_yes_no(self,inst_id):
        temp_instance_deploy = instance_deploy[instance_deploy.inst_id==inst_id].values
        app_id_new = temp_instance_deploy[0][1]
        P_new = temp_instance_deploy[0][4]
        M_new = temp_instance_deploy[0][5]
        PM_new = temp_instance_deploy[0][6]
        disk_new = temp_instance_deploy[0][3]

        self.app_already_whether_inst.append(inst_id)
        self.app_already_whether.append(app_id_new)
        self.temp_P_whether = self.temp_P_whether + P_new
        self.temp_M_whether = self.temp_M_whether + M_new
        self.temp_PM_whether = self.temp_PM_whether + PM_new
        self.temp_disk_whether = self.temp_disk_whether + disk_new

        for i in range(98):
            dif_temp_cpu_new = float(temp_instance_deploy[0][9+2*i])
            dif_temp_mem_new = float(temp_instance_deploy[0][10+2*i])
            self.temp_cpu_whether[i] = self.temp_cpu_whether[i] +dif_temp_cpu_new
            self.temp_mem_whether[i] = self.temp_mem_whether[i] + dif_temp_mem_new
            self.cpu_conflic[i] = 1*(not(self.temp_cpu_whether[i]<=((self.cpu_max))))
            self.half_cpu_conflic[i] = 1*(not(self.temp_cpu_whether[i]<=(((self.cpu_max)/2.0)*1.1)))
            self.more_cpu_conflic[i] = 1*(not(self.temp_cpu_whether[i]<=(((self.cpu_max)/2.0)*1.25)))
            self.mem_conflic[i] = 1*(not(self.temp_mem_whether[i]<=self.mem_max))
        
#     def instance_add(self,inst_id):
#         temp_instance_deploy = instance_deploy[instance_deploy.inst_id==inst_id].reset_index().copy()
#         app_id_true = temp_instance_deploy['app_id'][0]
#         P_true = temp_instance_deploy['P'][0]
#         M_true = temp_instance_deploy['M'][0]
#         PM_true = temp_instance_deploy['PM'][0]
#         disk_true = temp_instance_deploy['disk'][0]
        
#         self.app_already_inst.append(inst_id)
#         self.temp_P = self.temp_P + P_true
#         self.temp_M = self.temp_M + M_true
#         self.temp_PM = self.temp_PM + PM_true
#         self.temp_disk = self.temp_disk + disk_true
#         self.app_already.append(app_id_true)
        
#         for i in range(98):
#             dif_temp_cpu_true = float(temp_instance_deploy['cpu_'+str(i)][0])
#             dif_temp_mem_true = float(temp_instance_deploy['mem_'+str(i)][0])
#             self.temp_cpu[i] = self.temp_cpu[i] +dif_temp_cpu_true
#             self.temp_mem[i] = self.temp_mem[i] + dif_temp_mem_true
    def instance_add(self,inst_id):
        temp_instance_deploy = instance_deploy[instance_deploy.inst_id==inst_id].values
        app_id_true = temp_instance_deploy[0][1]
        P_true = temp_instance_deploy[0][4]
        M_true = temp_instance_deploy[0][5]
        PM_true = temp_instance_deploy[0][6]
        disk_true = temp_instance_deploy[0][3]
        
        self.app_already_inst.append(inst_id)
        self.temp_P = self.temp_P + P_true
        self.temp_M = self.temp_M + M_true
        self.temp_PM = self.temp_PM + PM_true
        self.temp_disk = self.temp_disk + disk_true
        self.app_already.append(app_id_true)
        
        for i in range(98):
            dif_temp_cpu_true = float(temp_instance_deploy[0][9+2*i])
            dif_temp_mem_true = float(temp_instance_deploy[0][10+2*i])
            self.temp_cpu[i] = self.temp_cpu[i] +dif_temp_cpu_true
            self.temp_mem[i] = self.temp_mem[i] + dif_temp_mem_true
            

    def inst_confilct_before_half(self):
        
        for i in range(98):
            self.half_cpu_conflic[i] = 1*(not(self.temp_cpu_whether[i]<=(((self.cpu_max)/2.0)*1.05)))
            self.mem_conflic[i] = 1*(not(self.temp_mem_whether[i]<=self.mem_max))
        self.P_conflic_before = 1*(self.temp_P_whether<=self.P_max)
        self.M_conflic_before = 1*(self.temp_M_whether<=self.M_max)
        self.PM_conflic_before = 1*(self.temp_PM_whether<=self.PM_max)
        self.disk_conflic_before = 1*(self.temp_disk_whether<=self.disk_max)
        self.half_cpu_all_conflic_before = 1*(not((sum(self.half_cpu_conflic)>=35)))
        self.mem_all_conflic_before = 1*(not(sum(self.mem_conflic)))
        self.app_conflic_before = 1*(self.machine_app_interfernce_before_obj_dict())
        #print(P_conflic,M_conflic,PM_conflic,disk_conflic,cpu_all_conflic,mem_all_conflic,app_conflic)
        if ((self.P_conflic_before)&(self.M_conflic_before)&(self.PM_conflic_before)&(self.disk_conflic_before)&(self.half_cpu_all_conflic_before)&(self.mem_all_conflic_before)&(self.app_conflic_before)):
            return True
        else:
            return False   
    def inst_confilct_before_more_for_92(self):
        score_single = 0
        for j in range(len(self.temp_cpu_whether)):
            score_single = score_single + 1+(len(self.app_already)+1)*(math.exp(max(0,(self.temp_cpu_whether[j]/92.0)-0.5))-1)
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
    def inst_confilct_before_more_for_32(self):
        score_single = 0
        for j in range(len(self.temp_cpu)):
            score_single = score_single + 1+(len(self.app_already)+1)*(math.exp(max(0,(self.temp_cpu[j]/32.0)-0.5))-1)
        score_single = score_single/(len(self.temp_cpu))
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

    def inst_confilct_before(self):
        for i in range(98):
            self.cpu_conflic[i] = 1*(not(self.temp_cpu_whether[i]<=((self.cpu_max))))
            self.mem_conflic[i] = 1*(not(self.temp_mem_whether[i]<=self.mem_max))
        self.P_conflic_before = 1*(self.temp_P_whether<=self.P_max)
        self.M_conflic_before = 1*(self.temp_M_whether<=self.M_max)
        self.PM_conflic_before = 1*(self.temp_PM_whether<=self.PM_max)
        self.disk_conflic_before = 1*(self.temp_disk_whether<=self.disk_max)
        self.cpu_all_conflic_before = 1*(not(sum(self.cpu_conflic)))
        self.mem_all_conflic_before = 1*(not(sum(self.mem_conflic)))
        self.app_conflic_before = 1*(self.machine_app_interfernce_before_obj_dict())
        #print(P_conflic,M_conflic,PM_conflic,disk_conflic,cpu_all_conflic,mem_all_conflic,app_conflic)
        if ((self.P_conflic_before)&(self.M_conflic_before)&(self.PM_conflic_before)&(self.disk_conflic_before)&(self.cpu_all_conflic_before)&(self.mem_all_conflic_before)&(self.app_conflic_before)):
            return True
        else:
            return False        

    #range(0,i)+range(i+1,len(app_already)):
    #before
    def machine_app_interfernce_before_obj_dict(self):
        interference_dict= dict(dict())
        if(len(self.app_already_whether)==1):
            return True
        app_x = []
        for i in range(len(self.app_already_whether)):
            if self.app_already_whether[i] not in app_x: 
                app_x.append(self.app_already_whether[i])
                for j in list(range(0,i))+list(range(i+1,len(self.app_already_whether))):
                    addtwodimdict_inference(interference_dict,self.app_already_whether[i],self.app_already_whether[j],1)
        final_dict = 0
        for i in range(len(interference_dict)):
            for j in range(len(interference_dict[list(interference_dict.keys())[i]])):
                app_x_now = list(interference_dict.keys())[i]
                app_y_now = list(interference_dict[list(interference_dict.keys())[i]].keys())[j]
                app_num = interference_dict[list(interference_dict.keys())[i]][list(interference_dict[list(interference_dict.keys())[i]].keys())[j]]
                if (not(app_interference_matrix.loc[app_x_now,app_y_now]==-1)|(app_num<=app_interference_matrix.loc[app_x_now,app_y_now])):
#                     print('dict')
                    self.before_conflict.append(self.app_already_whether_inst[self.app_already_whether.index(app_x_now)])
                    self.before_conflict.append(self.app_already_whether_inst[self.app_already_whether.index(app_y_now)])
                    self.before_conflict_app.append(app_x_now)
                    self.before_conflict_app.append(app_y_now)
#                     print interference_dict.keys()[i],interference_dict[interference_dict.keys()[i]].keys()[j],interference_dict[interference_dict.keys()[i]][interference_dict[interference_dict.keys()[i]].keys()[j]]
                final_dict = final_dict + 1*(not(app_interference_matrix.loc[app_x_now,app_y_now]==-1)|(app_num<=app_interference_matrix.loc[app_x_now,app_y_now]))
        return not(final_dict)

    def before_to_now(self):
        self.app_already = self.app_already_whether[:]
        self.app_already_inst = self.app_already_whether_inst[:]
        self.temp_disk = self.temp_disk_whether
        self.temp_P = self.temp_P_whether
        self.temp_M = self.temp_M_whether
        self.temp_PM = self.temp_PM_whether
        self.temp_cpu = self.temp_cpu_whether[:]
        self.temp_mem = self.temp_mem_whether[:]
    def now_to_whether(self):
        self.app_already_whether = self.app_already[:]
        self.app_already_whether_inst = self.app_already_inst[:]
        self.temp_disk_whether = self.temp_disk
        self.temp_P_whether = self.temp_P
        self.temp_M_whether = self.temp_M
        self.temp_PM_whether = self.temp_PM
        self.temp_cpu_whether = self.temp_cpu[:]
        self.temp_mem_whether = self.temp_mem[:]       
    def no_larger_than_small(self,inst_id):
        temp_instance_deploy = instance_deploy[instance_deploy.inst_id==inst_id].reset_index().copy()
        app_id_true = temp_instance_deploy['app_id'][0]
        P_true = temp_instance_deploy['P'][0]
        M_true = temp_instance_deploy['M'][0]
        PM_true = temp_instance_deploy['PM'][0]
        disk_true = temp_instance_deploy['disk'][0]
        small_cpu_conflic=[]
        small_mem_conflic=[]
        for i in range(98):
            small_cpu_conflic.append(0) 
            small_mem_conflic.append(0)
        for i in range(98):
            dif_temp_cpu_true = float(temp_instance_deploy['cpu_'+str(i)][0])
            dif_temp_mem_true = float(temp_instance_deploy['mem_'+str(i)][0])
            small_cpu_conflic[i] = 1*(not(dif_temp_cpu_true<=32))
            small_mem_conflic[i] = 1*(not(dif_temp_mem_true<=64))
        cpu_all_conflic_small = 1*(not(sum(small_cpu_conflic)))
        mem_all_conflic_small = 1*(not(sum(small_mem_conflic)))
        if (disk_true<=600)&(cpu_all_conflic_small)&(mem_all_conflic_small):
            return True
        else:
            return False
        
        
    def init_machine(self):
        #specific
        self.app_already=[]
        self.app_already_whether=[]
        self.app_already_inst=[]
        self.app_already_whether_inst=[]
        self.temp_P = 0
        self.temp_M = 0
        self.temp_PM = 0
        self.temp_disk = 0
        self.temp_P_whether = 0
        self.temp_M_whether = 0
        self.temp_PM_whether = 0
        self.temp_disk_whether = 0
        
        self.temp_cpu = []
        self.temp_mem = []
        self.temp_cpu_whether = []
        self.temp_mem_whether = []
        self.cpu_conflic = []
        self.mem_conflic = []
        self.index_here = []
        
        self.P_conflic = 1 
        self.M_conflic = 1 
        self.PM_conflic = 1 
        self.disk_conflic = 1 
        self.cpu_all_conflic = 1
        self.mem_all_conflic = 1 
        self.app_conflic = 1
        
        self.P_conflic_before = 1 
        self.M_conflic_before = 1 
        self.PM_conflic_before = 1 
        self.disk_conflic_before = 1 
        self.cpu_all_conflic_before = 1 
        self.mem_all_conflic_before = 1 
        self.app_conflic_before = 1 
        
        self.before_conflict = []
        self.before_conflict_app = []
        
        self.half_cpu_conflic = []
        self.half_cpu_all_conflic_before= 1
        self.half_cpu_all_conflic = 1
        
        self.more_cpu_conflic = []
        self.more_cpu_all_conflic_before= 1
        self.more_cpu_all_conflic = 1
        
        for i in range(98):
            self.temp_cpu.append(0)
            self.temp_mem.append(0)
            self.temp_cpu_whether.append(0)
            self.temp_mem_whether.append(0)
            self.cpu_conflic.append(0)
            self.half_cpu_conflic.append(0)
            self.more_cpu_conflic.append(0)
            self.mem_conflic.append(0)
        
        def len_already_before():
            return len(self.app_already_whether)
        
        
        
        
        
        

        
        
        
        
