import yaml
import threading
import logging
import time
import concurrent.futures

LOG_FILENAME = '/home/cheechu/KLA-Hack-Code/milestone_1B_log.txt'
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

def execute_activity(activity, string):
    logging.info(f"{string} Entry")
    if activity['Execution'] == 'Sequential':
        for key, value in activity['Activities'].items():
            execute_activity(value, f"{string}.{key}")
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
        logging.info(task['Condition'])
    task_exec(task['Function'], task['Inputs'], string)

def main():
    parsed_yaml = yaml_parse(YAML_FILE_NAME)
    root_key = list(parsed_yaml.keys())[0]
    execute_activity(parsed_yaml[root_key], root_key)

if __name__ == '__main__':
    main()