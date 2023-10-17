import yaml
import threading
import logging
import time
import pandas as pd

D = dict()
lock = threading.Lock()
LOG_FILENAME = 'milestone_2A_log.txt'
YAML_FILE_NAME = 'D:\KLA\DataSet\Milestone1\Milestone1A.yaml'

Format = "%(asctime)s.%(msecs)06d;%(message)s"
logging.basicConfig(
    format=Format,
    filename=LOG_FILENAME,
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

def yamlParse(filepath):
    with open(filepath) as stream:
        parsed_yaml = yaml.safe_load(stream)
    return parsed_yaml

def timeFunction(input_dict):
    val = input_dict['ExecutionTime']
    time.sleep(int(val))

def task_exec(function,input_dict,string,condition=None):
    if condition:
        new_condition = condition[2::].split(")")[0]
        sign = condition.split(" ")[1]
        val = int(condition.split(" ")[2])
        while(True):
            if new_condition in D:
                break
        if sign == '>':
            if D[new_condition] > val:
                if function == 'TimeFunction':
                    logging.info(string+" "+"Entry")
                    logging.info(string+" "+"Executing"+" "+function+" "+"("+str(input_dict['FunctionInput'])+', '+str(input_dict['ExecutionTime'])+")")
                    timeFunction(input_dict)
                    logging.info(string+" "+"Exit")
                elif function == 'DataLoad':
                    logging.info(string+" "+"Entry")
                    data = pd.read_csv("D:\\KLA\\DataSet\\Milestone2\\"+input_dict['Filename'])
                    logging.info(string+" "+"Executing"+" "+function+" "+"("+str(input_dict['Filename'])+")")
                    dummy_string = ""
                    dummy_string += string
                    num_defects = data.shape[0] - 1
                    dummy_string += ".NoOfDefects"
                    D[dummy_string] = num_defects
                    logging.info(string+" "+"Exit")
            else:
                logging.info(string+" "+"Skipped")
            print(function,input_dict,string)
        
        elif sign == '<':
            if D[new_condition] < val:

                if function == 'TimeFunction':
                    logging.info(string+" "+"Entry")
                    logging.info(string+" "+"Executing"+" "+function+" "+"("+str(input_dict['FunctionInput'])+', '+str(input_dict['ExecutionTime'])+")")
                    timeFunction(input_dict)
                    logging.info(string+" "+"Exit")
                elif function == 'DataLoad':
                    logging.info(string+" "+"Entry")
                    data = pd.read_csv("D:\\KLA\\DataSet\\Milestone2\\"+input_dict['Filename'])
                    logging.info(string+" "+"Executing"+" "+function+" "+"("+str(input_dict['Filename'])+")")
                    dummy_string = ""
                    dummy_string += string
                    num_defects = data.shape[0] - 1
                    dummy_string += ".NoOfDefects"
                    D[dummy_string] = num_defects
                    logging.info(string+" "+"Exit")

            else:
                logging.info(string+" "+"Skipped")
            print(function,input_dict,string)
    else:
        if function == 'TimeFunction':
            logging.info(string+" "+"Entry")
            logging.info(string+" "+"Executing"+" "+function+" "+"("+str(input_dict['FunctionInput'])+', '+str(input_dict['ExecutionTime'])+")")
            timeFunction(input_dict)
            logging.info(string+" "+"Exit")
        elif function == 'DataLoad':
            logging.info(string+" "+"Entry")
            data = pd.read_csv("D:\\KLA\\DataSet\\Milestone2\\"+input_dict['Filename'])
            logging.info(string+" "+"Executing"+" "+function+" "+"("+str(input_dict['Filename'])+")")
            dummy_string = ""
            dummy_string += string
            num_defects = data.shape[0] - 1
            dummy_string += ".NoOfDefects"
            D[dummy_string] = num_defects
            logging.info(string+" "+"Exit")
        print(function,input_dict,string)


def function(dict_obj,string):
    if dict_obj['Type'] == 'Flow':
        if dict_obj['Execution'] == 'Sequential':
            logging.info(string+" "+"Entry")
            for key in dict_obj['Activities'].keys():
                function(dict_obj['Activities'][key],string+"."+str(key))
            logging.info(string+" "+"Exit")
        else:
            logging.info(string+" "+"Entry")
            L = dict_obj['Activities'].keys()
            res = []
            for i in L:
                pr = threading.Thread(target=function,args=(dict_obj['Activities'][i],string+"."+str(i)))
                res.append(pr)
                pr.start()
            for r in res:
                r.join()
            logging.info(string+" "+"Exit")
    elif dict_obj['Type'] == 'Task':
        if "Condition" in dict_obj:
            print(dict_obj['Condition'])
            task_exec(dict_obj['Function'],dict_obj['Inputs'],string,dict_obj['Condition'])
        else:
            task_exec(dict_obj['Function'],dict_obj['Inputs'],string,None)
     
parsed_yaml = yamlParse(YAML_FILE_NAME)

string = str(list(parsed_yaml.keys())[0])
function(parsed_yaml[string],string)
print(D)