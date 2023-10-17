import yaml
import logging
import time

LOG_FILENAME = '/home/cheechu/KLA-Hack-Code/milestone2A_log.txt'
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

def execute_flow(flow_dict, string):
    logging.info(f"{string} Entry")
    for key, value in flow_dict['Activities'].items():
        execute_activity(value, f"{string}.{key}")
    logging.info(f"{string} Exit")

def execute_activity(activity_dict, string):
    if activity_dict['Type'] == 'Flow':
        execute_flow(activity_dict, string)
    else:
        task_exec(activity_dict['Function'], activity_dict['Inputs'], string)

def task_exec(function, input_dict, string):
    if function == 'TimeFunction':
        function_name = "TimeFunction"
    else:
        function_name = "DataLoad"

    logging.info(f"{string} Entry")
    logging.info(f"{string} Executing {function_name} ({input_dict})")
    if function == 'TimeFunction':
        time_function(input_dict)
    logging.info(f"{string} Exit")
    print(function, input_dict, string)

def main():
    parsed_yaml = yaml_parse(YAML_FILE_NAME)
    root_key = list(parsed_yaml.keys())[0]
    execute_activity(parsed_yaml[root_key], root_key)

if __name__ == '__main__':
    main()


        
            
    



