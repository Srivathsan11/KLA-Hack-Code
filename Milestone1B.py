import yaml
import threading
import logging
import time
import concurrent.futures

LOG_FILENAME = 'milestone_1B_log.txt'
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

def task_exec(function,input_dict,string):
    if function == 'TimeFunction':
        logging.info(string+" "+"Entry")
        logging.info(string+" "+"Executing"+" "+function+" "+"("+str(input_dict['FunctionInput'])+', '+str(input_dict['ExecutionTime'])+")")
        timeFunction(input_dict)
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
        task_exec(dict_obj['Function'],dict_obj['Inputs'],string)
     
parsed_yaml = yamlParse(YAML_FILE_NAME)

string = str(list(parsed_yaml.keys())[0])
function(parsed_yaml[string],string)