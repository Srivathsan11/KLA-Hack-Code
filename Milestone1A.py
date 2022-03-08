import yaml
import multiprocessing
import logging
import time

LOG_FILENAME = 'milestone2A_log.txt'
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

"""def returnNestedDict(dict_obj):
    for key,value in dict_obj.items():
        if isinstance(value, dict):
            return returnNestedDict(value) 
        else:
            return dict_obj"""

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
    for k in dict_obj.keys():
        if k=='Type':
            if dict_obj[k] =='Flow':
                logging.info(string+" "+"Entry")
                for key in dict_obj['Activities'].keys():
                    function(dict_obj['Activities'][key],string+"."+str(key))
                logging.info(string+" "+"Exit")
            else:
                task_exec(dict_obj['Function'],dict_obj['Inputs'],string)
     
parsed_yaml = yamlParse('D:\KLA\DataSet\Milestone1\Milestone1A.yaml')
#main_dict = returnNestedDict(parsed_yaml)
string = str(list(parsed_yaml.keys())[0])
function(parsed_yaml[string],string)


        
            
    



