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


def find_new_machine(inst_here):
    index_machine = -1
    for i in range(3000):
        if inst_here in machine_big_new[i].app_already_inst:
            index_machine = 3000+i
        elif inst_here in machine_small_new[i].app_already_inst:
            index_machine = i
    if index_machine==-1:
        print('why, what happened')
    return index_machine

def find_before_machine(inst_here):
    index_machine = -1
    for i in range(3000):
        if inst_here in machine_big_before[i].app_already_inst:
            index_machine = 3000+i
        elif inst_here in machine_small_before[i].app_already_inst:
            index_machine = i
    if index_machine==-1:
        print('why, what happened')
    return index_machine



f = open('init_to_final.csv', 'w') 

with_index = 0
while(with_index < len(with_machine_inst)):
    print('to do: ',with_index)
    index_new = find_new_machine(with_machine_inst[with_index])
    index_before = find_before_machine(with_machine_inst[with_index])
    print(index_before,'->',index_new)
    if index_before>=3000:
        before_list = machine_big_before[index_before-3000].app_already_inst[:]
        before_list.remove(with_machine_inst[with_index])
        machine_big_before[index_before-3000].init_machine()
        for ij in range(len(before_list)):
            machine_big_before[index_before-3000].instance_add_yes_no(before_list[ij])
            machine_big_before[index_before-3000].instance_add(before_list[ij]) 
    elif index_before<3000:
        before_list = machine_small_before[index_before].app_already_inst[:]
        before_list.remove(with_machine_inst[with_index])
        machine_small_before[index_before].init_machine()
        for ij in range(len(before_list)):
            machine_small_before[index_before].instance_add_yes_no(before_list[ij])
            machine_small_before[index_before].instance_add(before_list[ij]) 
        
            
            
            
    if index_new>=3000:
        machine_big_before[index_new-3000].instance_add_yes_no(with_machine_inst[with_index])
        if inst_confilct_before(machine_big_before[index_new-3000]):
            machine_big_before[index_new-3000].instance_add(with_machine_inst[with_index])
            f.write(with_machine_inst[with_index])
            f.write(',')
            f.write('machine_')
            f.write(str(index_new+1))
            f.write('\n')
            with_index = with_index + 1
            
                                                             
        
        else:
            if index_before<3000:
                machine_small_before[index_before].instance_add_yes_no(with_machine_inst[with_index])
                machine_small_before[index_before].instance_add(with_machine_inst[with_index]) 
            elif index_before>=3000:
                machine_big_before[index_before-3000].instance_add_yes_no(with_machine_inst[with_index])
                machine_big_before[index_before-3000].instance_add(with_machine_inst[with_index])                 

            no_in_new = []
            in_new = []
            machine_big_before[index_new-3000].now_to_whether()
            for i in range(len(machine_big_before[index_new-3000].app_already_inst)):
                if machine_big_before[index_new-3000].app_already_inst[i] not in machine_big_new[index_new-3000].app_already_inst:
                    no_in_new.append(machine_big_before[index_new-3000].app_already_inst[i])
                else:
                    in_new.append(machine_big_before[index_new-3000].app_already_inst[i])
            machine_big_before[index_new-3000].init_machine()
            print(len(no_in_new),len(in_new),len(machine_big_before[index_new-3000].app_already_inst))
            for j in range(len(in_new)):
                machine_big_before[index_new-3000].instance_add_yes_no(in_new[j])
                machine_big_before[index_new-3000].instance_add(in_new[j])
            k_no = 0
            while(k_no < len(no_in_new)):
                k_rand = random.randint(0,2999)
                machine_big_before[k_rand].instance_add_yes_no(no_in_new[k_no])
                if inst_confilct_before(machine_big_before[k_rand]):
                    machine_big_before[k_rand].instance_add(no_in_new[k_no])
                    f.write(no_in_new[k_no])
                    f.write(',')
                    f.write('machine_')
                    f.write(str(k_rand+3001))
                    f.write('\n')
                    k_no = k_no + 1

                else:
                    machine_big_before[k_rand].now_to_whether()
    
    elif index_new<3000:
        machine_small_before[index_new].instance_add_yes_no(with_machine_inst[with_index])
        if inst_confilct_before(machine_small_before[index_new]):
            machine_small_before[index_new].instance_add(with_machine_inst[with_index])
            f.write(with_machine_inst[with_index])
            f.write(',')
            f.write('machine_')
            f.write(str(index_new+1))
            f.write('\n')
            with_index = with_index + 1
        
        else:
            if index_before<3000:
                machine_small_before[index_before].instance_add_yes_no(with_machine_inst[with_index])
                machine_small_before[index_before].instance_add(with_machine_inst[with_index]) 
            elif index_before>=3000:
                machine_big_before[index_before-3000].instance_add_yes_no(with_machine_inst[with_index])
                machine_big_before[index_before-3000].instance_add(with_machine_inst[with_index])  
                
            
            no_in_new = []
            in_new = []
            machine_small_before[index_new].now_to_whether()
            for i in range(len(machine_small_before[index_new].app_already_inst)):
                if machine_small_before[index_new].app_already_inst[i] not in machine_small_new[index_new].app_already_inst:
                    no_in_new.append(machine_small_before[index_new].app_already_inst[i])
                else:
                    in_new.append(machine_small_before[index_new].app_already_inst[i])
            machine_small_before[index_new].init_machine()
            print(len(no_in_new),len(in_new),len(machine_small_before[index_new].app_already_inst))
            for j in range(len(in_new)):
                machine_small_before[index_new].instance_add_yes_no(in_new[j])
                machine_small_before[index_new].instance_add(in_new[j])
            k_no = 0
            while(k_no < len(no_in_new)):
                k_rand = random.randint(0,2999)
                machine_small_before[k_rand].instance_add_yes_no(no_in_new[k_no])
                if inst_confilct_before(machine_small_before[k_rand]):
                    machine_small_before[k_rand].instance_add(no_in_new[k_no])
                    f.write(no_in_new[k_no])
                    f.write(',')
                    f.write('machine_')
                    f.write(str(k_rand+1))
                    f.write('\n')
                    k_no = k_no + 1

                else:
                    machine_small_before[k_rand].now_to_whether()
