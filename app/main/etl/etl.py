import json
import pandas as pd
import logging
from datetime import datetime, timedelta
import time
import re

logger:logging.Logger

logger = logging.getLogger('etl-proccess')

def matchDate(line):
    match = ""
    matched = re.match(r'\[\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\]',line)
    if matched:            
        match = matched.group() 
    return match

def generateDict(log_file):
    currentDict = {}
    for line in log_file:
        if line.startswith(matchDate(line)):
            if currentDict:
                yield currentDict
            currentDict = {
                'date': line.split('\t')[0][1:20], 
                'type': line.split('\t')[1], 
                'sensor': line.split('\t')[2][6:-1].strip('[').strip(']'), 
                'temperature': line.split('\t')[4].split(',')[0], 
                'vibration': line.split('\t')[5].split(')')[0]
            }
    yield currentDict

def executar():
    
    '''id=connection.execute("INSERT INTO  `db`.`tb_sensor` (`sensor_id` ,`equipment_id`) \
                    VALUES (1, 45)")
    print("Row Added  = ",id.rowcount)'''

def main():
    logging.basicConfig('elt-proccess.log', filemode='w+', level=logging.DEBUG, format='%(asctime)s %(levelname) %(message)s')

    try:
        start_time = datetime.now()
        data = datetime.now() - timedelta(days=0)
        executar()
        elapsed_time = datetime.now() - start_time
        logging.info('ETL - %s Finalizado elapsed_time: %s', data, time.strftime('%H:%M:%S', time.gtime(elapsed_time)))

    except Exception as e:
        logging.exception('ETL - Exception: %s', e, exc_info=True)

if __name__ == '__main__':
    main()
