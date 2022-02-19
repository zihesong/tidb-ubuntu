'''
scp -P 22 /Users/zoooesong/Workspaces/galera-thread.py nobi@pc311.emulab.net:/users/nobi/galera-data/galera-thread.py
python3 galera-thread.py -n 1
'''

import linecache
import os
import time
import random
from unittest import result
import pandas as pd
import sys
import getopt
import threading
import resource
import re
import pymysql
import numpy as np
import matplotlib.pyplot as plt

wo_rate=0.2
ro_rate=0.2
wr_rate = 0.5 # write
transaction_num = 20
operation_num = 25
threads_num = 10
node_no=1
folder_num = 11
server_id = [4000,4001,4002]

try:
    opts, args = getopt.getopt(sys.argv[1:],"hw:r:p:t:o:c:n:f:",["help","wo_rate=","ro_rate=","w_percent=","trans_num=","op_num=","client_num=","node_no=","folder_num="])
    for opt, arg in opts:
        if opt in ('-w','--wo_rate'):
            wo_rate = float(arg)
        elif opt in ('-r','--ro_rate'):
            ro_rate = float(arg)
        elif opt in ('-p','--w_percent'):

            
            wr_rate = float(arg)
        elif opt in ('-t','--trans_num'):
            transaction_num = int(arg)
        elif opt in ('-o','--op_num'):
            operation_num = int(arg)
        elif opt in ('-c','--client_num'):
            threads_num = int(arg)
        elif opt in ('-n','--node_no'):
            node_no = int(arg)
        elif opt in ('-f','--folder_num'):
            folder_num = str(arg)
        elif opt in ('-h','--help'):
            print("python3 tidb-thread.py -w <wo_rate> -r <ro_rate> -p <w_percent> -t <trans_num> -o <op_num> -c <client_num> -n <node_no> -f <folder_num>")
            sys.exit()
except getopt.GetoptError:
    print("python3 tidb-thread.py -w <wo_rate> -r <ro_rate> -p <w_percent> -t <trans_num> -o <op_num> -c <client_num> -n <node_no>")
    sys.exit()
# print("Parameters:\nwo_rate = " + str(wo_rate) + "\nro_rate = " + str(ro_rate) + "\nw_percent = " + str(wr_rate) + "\ntrans_num = " + str(transaction_num) + "\nop_num = " + str(operation_num) + "\nclient_num = " + str(threads_num) + "\nnode_no = " + str(node_no) + "\nfolder_num = " + str(folder_num))

key_num = 20
total_op_num = 2*transaction_num*operation_num
folder_name = "./output/"+str(folder_num)+"/"
hist_folder = "./client/"+str(folder_num)+"/"
print('current: ' + folder_num)



def mk_dir(path):
	folder = os.path.exists(path)
	if not folder:                   
		os.makedirs(path)            

class myThread(threading.Thread):
    def __init__(self,id):
        threading.Thread.__init__(self)
        self.id = id
        pass
    def run(self):
        run_thread(i)

class Operation:
    op_type = True  #true is write
    variable = 0
    value = 0
    
    def __init__(self, op_type, variable, value):
        self.op_type = op_type
        self.variable = variable
        self.value = value

    def Read(self,variable):
        self.op_type = False
        self.variable = variable
        self.value = 0
    def Write(self,variable,value):
        self.op_type = True
        self.variable = variable
        self.value = value
    # def Display_info(self):
    #     if(op_type==True):
    #         print("write," + str(variable) + "," + str(value))
    #     elif(op_type==False):
    #         print("read," + str(variable) + "," + str(value))
    #     else:
    #         print("Error in Operation op_type!")


def Zipf(a: np.float64, min: np.uint64, max: np.uint64, size=None):
    """
    Generate Zipf-like random variables,
    but in inclusive [min...max] interval
    """
    if min == 0:
        raise ZeroDivisionError("")

    v = np.arange(min, max+1) # values to sample
    p = 1.0 / np.power(v, a)  # probabilities
    p /= np.sum(p)            # normalized

    return np.random.choice(v, size=size, replace=True, p=p)



def zipf_generator(output_path, client, trans, ops, var):
    '''
    output_path: hist file path
    client: client No.
    trans: trans num for each client
    ops: operation num for each trans
    var: key num
    wr: rate of w/r
    '''
    doc = open(output_path+"hist_"+str(client)+".txt",'w')
    counter = client * total_op_num * 2
    min = np.uint64(1)
    max = np.uint64(var)
    q = Zipf(1, min, max, trans*ops)
    var_list = [int(x)-1 for x in q]
    var_count = 0
    for t in range (0,trans):
        trans_type = random_pick([0,1,2],[wo_rate,ro_rate,1-wo_rate-ro_rate])
        if trans_type == 0:
            for op in range (0,ops):
                variable = var_list[var_count]
                var_count += 1
                counter += 1
                new_op = Operation(True,variable,counter)
                doc.write("write," + str(new_op.variable) + "," + str(new_op.value)+"\n")
        elif trans_type == 1:
            for op in range (0,ops):
                variable = var_list[var_count]
                var_count += 1
                new_op = Operation(False,variable,0)
                doc.write("read," + str(new_op.variable) + "," + str(new_op.value)+"\n")
        elif trans_type == 2:
            w_count = 0
            r_count = 0
            for i in range (0,ops):
                op_type = random_pick([0,1],[wr_rate,1-wr_rate])
                if op_type == 0:
                    w_count += 1
                elif op_type == 1:
                    r_count += 1
                else:
                    print("Error in op_type!")
            for r in range(r_count):
                variable = var_list[var_count]
                var_count += 1
                new_op = Operation(False,variable,0)
                doc.write("read," + str(new_op.variable) + "," + str(new_op.value)+"\n")
            for w in range(w_count):
                variable = var_list[var_count]
                var_count += 1
                counter += 1
                new_op = Operation(True,variable,counter)
                doc.write("write," + str(new_op.variable) + "," + str(new_op.value)+"\n")
        else:
            print("Error in trans_type!")
    doc.close()
    # print(output_path+"hist_"+str(client)+".txt"+" succeeded.")




def uniform_generator(output_path, client, trans, ops, var):
    '''
    output_path: hist file path
    client: client No.
    trans: trans num for each client
    ops: operation num for each trans
    var: key num
    wr: rate of w/r
    '''
    doc = open(output_path+"hist_"+str(client)+".txt",'w')
    counter = client * total_op_num * 2
    for t in range (0,trans):
        trans_type = random_pick([0,1,2],[wo_rate,ro_rate,1-wo_rate-ro_rate])
        if trans_type == 0:
            for op in range (0,ops):
                variable = random.randint(0,var-1)
                counter += 1
                new_op = Operation(True,variable,counter)
                doc.write("write," + str(new_op.variable) + "," + str(new_op.value)+"\n")
        elif trans_type == 1:
            for op in range (0,ops):
                variable = random.randint(0,var-1)
                new_op = Operation(False,variable,0)
                doc.write("read," + str(new_op.variable) + "," + str(new_op.value)+"\n")
        elif trans_type == 2:
            w_count = 0
            r_count = 0
            for i in range (0,ops):
                op_type = random_pick([0,1],[wr_rate,1-wr_rate])
                if op_type == 0:
                    w_count += 1
                elif op_type == 1:
                    r_count += 1
                else:
                    print("Error in op_type!")
            for r in range(r_count):
                variable = random.randint(0,var-1)
                new_op = Operation(False,variable,0)
                doc.write("read," + str(new_op.variable) + "," + str(new_op.value)+"\n")
            for w in range(w_count):
                variable = random.randint(0,var-1)
                counter += 1
                new_op = Operation(True,variable,counter)
                doc.write("write," + str(new_op.variable) + "," + str(new_op.value)+"\n")
        else:
            print("Error in trans_type!")
    doc.close()
    # print(output_path+"hist_"+str(client)+".txt"+" succeeded.")


def random_pick(some_list, probabilities): 
    '''
    randon_pick([true,false],[0.5,0.5])
    '''
    x = random.uniform(0,1) 
    cumulative_probability = 0.0 
    for item, item_probability in zip(some_list, probabilities): 
        cumulative_probability += item_probability 
        if x < cumulative_probability:
               break 
    return item 


def generate_opt(hist_file, trans_num): 
    fo = open(hist_file, "r")
    # print ("Select hist file:", fo.name)
    list_line = []
    for line in fo.readlines():
        line = line.strip()                            
        list_line.append(line)
    fo.close()
    list_trans = []
    op_count=0
    for i in range(0,trans_num):
        temp_ops = []
        for j in range(0,operation_num):
            temp_ops.append(list_line[op_count])
            op_count += 1
        list_trans.append(temp_ops)
    return list_trans


def run_ops(list_of_ops, client_no):
    op_num = 0
    result_ops = []
    server_num = random_pick([0,1,2],[0.34,0.33,0.33])
    server = server_id[server_num]
    # print("client_no: "+ str(client_no) + ", server_no: " + str(server))
    connect = pymysql.connect(host='127.0.0.1', user="root", port=int(server), password="")
    # Disable Auto-Commit
    connect.autocommit = False
    t_count = 0
    e_count = 0
    for i in range(len(list_of_ops)):
        if t_count > transaction_num:
            break
        cursor = connect.cursor()
        cursor.execute("START TRANSACTION WITH CAUSAL CONSISTENCY ONLY;")
        temp_tx_op = []
        e_flag = False
        for j in range(len(list_of_ops[i])):
            op = str.split(list_of_ops[i][j],',',3)
            key = int(op[1])
            if(op[0] == 'write'):
                val = int(op[2])
                try:
                    cursor.execute("UPDATE tidb.variables SET val=%d WHERE var=%d;" % (val,key))
                    single_op = 'w(' + str(key) + ',' + str(val) + ',' + str(client_no) + ',' + str(i) + ',' + str(op_num) + ')'
                except Exception as e:
                    single_op = 'w(' + str(key) + ',' + str(val) + ',' + str(client_no) + ',' + str(i) + ',' + str(op_num) + ')'
                    e_flag = True
            elif(op[0] == 'read'):
                try:
                    cursor.execute("SELECT val FROM tidb.variables WHERE var=%d;" % key)
                    return_val = cursor.fetchall()
                    record_val = return_val[0][0]
                    single_op = 'r(' + str(key) + ',' + str(record_val) + ',' + str(client_no) + ',' + str(i) + ',' + str(op_num) + ')'
                except Exception as e:
                    # print('Error in read: {}'.format(e)) 
                    # print(temp_tx_op)
                    single_op = 'r(' + str(key) + ',' + str(record_val) + ',' + str(client_no) + ',' + str(i) + ',' + str(op_num) + ')'
                    e_flag = True
            else:
                print("Unknown wrong type op: '%s'" % op[0])
            op_num += 1
            temp_tx_op.append(single_op)
            
        if e_flag == True:
            cursor.execute("ROLLBACK;")
        else:
            try:
                cursor.execute("COMMIT;")
            except Exception as e:
                # print('Error in commit: {}'.format(e)) 
                cursor.execute("ROLLBACK;")
                # print(temp_tx_op)
                e_flag = True
        connect.commit()
        if e_flag == False:
            # state_op = 'op(' + str(i+start_pos) + ',0)'
            t_count += 1
            result_ops.append(temp_tx_op)
        else:
            # state_op = 'op(' + str(i+start_pos) + ',1)'
            e_count += 1
        # temp_tx_op.append(state_op)
        # result_ops.append(temp_tx_op)
    cursor.close()
    connect.close()
    if t_count < transaction_num:
        print("################################################################################################################UNFINISH################################################################################################################")
    return result_ops, e_count, server_num

def write_result(result,file_path, error_num, server_num):
    '''
        result_single_history is a three dimensional list
        file is the output path
    '''
    f=open(file_path,"w")
    for n_trans in range(len(result)-1):
        for n_ops in range(len(result[0])):
            f.write(result[n_trans][n_ops]+'\n')
    f.close()
    # print(file_path + ' is completed, contain error: ', error_num)


def run_thread(id):
    client = int((node_no-1)*threads_num+id)
    zipf_generator(hist_folder, client, 4*transaction_num, operation_num, key_num)
    file_path = hist_folder + "hist_" + str(client) + ".txt"
    hist_list = generate_opt(file_path, 4*transaction_num)
    result_list, error_num, server_num = run_ops(hist_list,client)
    # while(error_num > e_threshold):
    #     rerun_count += 1
    #     tmp_error = error_num
    #     print("Client " + str(client) + " requires extra transactions! Count = " + str(rerun_count))
    #     extra_path = './client/' + str(client) + '/extra/'
    #     mkdir(extra_path) 
    #     uniform_generator(extra_path, client, error_num, operation_num, key_num, rerun_count)
    #     extra_file_path = extra_path+"hist_"+str(client)+"_"+str(rerun_count)+".txt"
    #     extra_hist_list = generate_opt(extra_file_path, error_num)
    #     extra_result_list, error_num = run_ops(extra_hist_list,client,start_pos)
    #     start_pos += tmp_error
    #     total_error += error_num
    #     result_list.extend(extra_result_list)
    # summary_line = "total = " + str(start_pos) + ", succeeded = " + str(start_pos-total_error) + ", failed = " + str(total_error)
    # result_list.append(summary_line)
    result_path = folder_name + "result_" + str(client) + "server_" + str(server_num) + ".txt"
    write_result(result_list, result_path, error_num, server_num)


if __name__ == '__main__':
    threads =[]
    tlock=threading.Lock()
    os.makedirs(folder_name, exist_ok=True)
    os.makedirs(hist_folder, exist_ok=True)
    # mk_dir(folder_name)
    # mk_dir(hist_folder) 
    for i in range(threads_num):
        thread = myThread(i)
        threads.append(thread)

    for i in range(threads_num):
        threads[i].start()
