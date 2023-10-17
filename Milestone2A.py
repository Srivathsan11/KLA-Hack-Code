import yaml
import threading
import logging
import time
import pandas as pd

D = dict()
lock = threading.Lock()
LOG_FILENAME = '/home/cheechu/KLA-Hack-Code/milestone_2A_log.txt'
YAML_FILE_NAME = 'D:\KLA\DataSet\Milestone1\Milestone1A.yaml'

Format = "%(asctime)s.%(msecs)06d;%(message)s"
logging.basicConfig(
    format=Format,
    filename=LOG_FILENAME,
    level=logging.WARNING,
    datefmt="%Y-%m-%d %H:%M:%S",
)

def yaml_parse(filepath):
    with open(filepath) as stream:
        parsed_yaml = yaml.safe_load(stream)
    return parsed_yaml

def time_function(input_dict):
    val = input_dict['ExecutionTime']
    time.sleep(int(val))

name_constant = "D:\\KLA\\DataSet\\Milestone2\\"
defect_string = ".NoOfDefects"

def task_exec(function, input_dict, string, condition=None):
    if not condition:
        return
    
    new_condition = condition[2:].split(")")[0]
    sign = condition.split(" ")[1]
    val = int(condition.split(" ")[2])

    if new_condition in D:
        return
    
    if sign == '>' and D.get(new_condition, 0) > val:
        if function in {'TimeFunction', 'DataLoad'}:
            logging.info(f"{string} Entry")
            
            if function == 'TimeFunction':
                logging.info(f"{string} Executing {function} ({input_dict['FunctionInput']}, {input_dict['ExecutionTime']})")
                time_function(input_dict)
                
            elif function == 'DataLoad':
                logging.info(f"{string} Executing {function} ({input_dict['Filename']})")
                data = pd.read_csv(name_constant + input_dict['Filename'])
                dummy_string = string
                num_defects = data.shape[0] - 1
                dummy_string += defect_string
                D[dummy_string] = num_defects
                logging.info(f"{string} Exit")
        else:
            logging.info(f"{string} Skipped")
            print(function, input_dict, string)
    
    elif sign == '<' and D.get(new_condition, 0) < val:
        if function in {'TimeFunction', 'DataLoad'}:
            logging.info(f"{string} Entry")
            
            if function == 'TimeFunction':
                logging.info(f"{string} Executing {function} ({input_dict['FunctionInput']}, {input_dict['ExecutionTime']})")
                timeFunction(input_dict)
                
            elif function == 'DataLoad':
                logging.info(f"{string} Executing {function} ({input_dict['Filename']})")
                data = pd.read_csv(name_constant + input_dict['Filename'])
                dummy_string = string
                num_defects = data.shape[0] - 1
                dummy_string += defect_string
                D[dummy_string] = num_defects
                logging.info(f"{string} Exit")
        else:
            logging.info(f"{string} Skipped")
            print(function, input_dict, string)
    
    else:
        if function in {'TimeFunction', 'DataLoad'}:
            logging.info(f"{string} Entry")
            
            if function == 'TimeFunction':
                logging.info(f"{string} Executing {function} ({input_dict['FunctionInput']}, {input_dict['ExecutionTime']})")
                time_function(input_dict)
                
            elif function == 'DataLoad':
                logging.info(f"{string} Executing {function} ({input_dict['Filename']})")
                dummy_string = string
                dummy_string += string
                data = pd.read_csv(name_constant + input_dict['Filename'])
                num_defects = data.shape[0] - 1
                dummy_string += defect_string
                D[dummy_string] = num_defects
                logging.info(f"{string} Exit")
        else:
            logging.info(f"{string} Skipped")
            print(function, input_dict, string)


def execute_activity(activity, string):
    logging.info(f"{string} Entry")
    if activity['Execution'] == 'Sequential':
        for key in activity['Activities'].keys():
            execute_activity(activity['Activities'][key], f"{string}.{key}")
    else:
        L = activity['Activities'].keys()
        threads = []
        for i in L:
            thread = threading.Thread(target=execute_activity, args=(activity['Activities'][i], f"{string}.{i}"))
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()
    logging.info(f"{string} Exit")

def execute_task(task, string):
    if "Condition" in task:
        print(task['Condition'])
        task_exec(task['Function'], task['Inputs'], string, task['Condition'])
    else:
        task_exec(task['Function'], task['Inputs'], string, None)

def function(dict_obj, string):
    if dict_obj['Type'] == 'Flow':
        execute_activity(dict_obj, string)
    elif dict_obj['Type'] == 'Task':
        execute_task(dict_obj, string)

     
parsed_yaml = yaml_parse(YAML_FILE_NAME)

string = str(list(parsed_yaml.keys())[0])
function(parsed_yaml[string],string)
print(D)