import json
import pandas as pd
import logging
from datetime import datetime
import re
from db_generic import connectionSql
import csv

logger:logging.Logger

logger = logging.getLogger('etl-process')

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
                'sensor': line.split('\t')[2][6:-1].strip('[').strip(']'), 
                'temperature': line.split('\t')[4].split(',')[0], 
                'vibration': line.split('\t')[5].split(')')[0]
            }
    yield currentDict

def generateCsvLogs():
    try:
        logging.debug("ETL - generateCsvLogs - Iniciando o processo")
        with open("raw_data/equipment_failure_sensors.log") as f:
            listNew = list(generateDict(f))
            if listNew:
                logs_csv = pd.DataFrame(listNew)
                logs_csv.to_csv("processed_data/logs.csv", sep=';', encoding='utf-8', index=False)
                logging.debug("ETL - generateCsvLogs - Processo finalizado com sucesso")
 
    except Exception as e:
        logging.exception('ETL - generateCsvLogs - Exception: %s', e, exc_info=True)

def generateCsvEquip():
    try:
        logging.debug("ETL - generateCsvEquip - Iniciando o processo")
        with open("raw_data/equipment.json") as f:
            file = json.load(f)
            equipment_csv = pd.DataFrame(file)
            equipment_csv.to_csv("processed_data/equipments.csv", sep=';', encoding='utf-8', index=False)
            logging.debug("ETL - generateCSV - Processo finalizado com sucesso")
 
    except Exception as e:
        logging.exception('ETL - generateCsv - Exception: %s', e, exc_info=True)


def dataInsert():
    connection = connectionSql()
    #Inserindo os dados dos LOGS na tb_failure
    try:
        with open('processed_data/logs.csv') as csv_file:
            csv_data = csv.reader(csv_file, delimiter=';')
            headers = next(csv_data)
            row_count = 0
            for row in csv_data:
                insert = connection.execute(
                    "INSERT IGNORE INTO tb_failure (dt_failure ,sensor, temperature ,vibration) \
                        VALUES (%s, %s, %s, %s)", row
                )

                row_count += insert.rowcount

            logging.debug('ELT - dataInsert LOGS - Rows added to tb_failure %s', row_count)

    except Exception as e:
        logging.exception('ELT - dataInsert LOGS - Exception: %s', e, exc_info=True)

    #Inserindo os dados dos sensores na tb_sensor
    try:
        with open('raw_data/equipment_sensors.csv') as csv_file:
            csv_data = csv.reader(csv_file, delimiter=';')
            headers = next(csv_data)
            row_count = 0
            for row in csv_data:
                insert = connection.execute(
                    "INSERT IGNORE INTO tb_sensor (equipment_id ,sensor_id) \
                        VALUES (%s, %s)", row
                )
                row_count += insert.rowcount

            logging.debug('ELT - dataInsert SENSOR - Rows added to tb_sensor %s', row_count)

    except Exception as e:
        logging.exception('ELT - dataInsert SENSOR - Exception: %s', e, exc_info=True)


    #Inserindo dados de equipamentos na tb_equipment
    try:
        with open('processed_data/equipments.csv') as csv_file:
            csv_data = csv.reader(csv_file, delimiter=';')
            headers = next(csv_data)
            row_count = 0
            for row in csv_data:
                insert = connection.execute(
                    "INSERT IGNORE INTO tb_equipment (equipment_id ,code, group_name) \
                        VALUES (%s, %s, %s)", row
                )
                row_count += insert.rowcount

            logging.debug('ELT - dataInsert EQUIPMENT - Rows added to tb_equipment %s', row_count)

    except Exception as e:
        logging.exception('ELT - dataInsert EQUIPMENT - Exception: %s', e, exc_info=True)

    #close the connection to the database.
    connection.close()


def main():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        filename='elt-process.log',
        filemode='w+',
        level=logging.DEBUG
    )

    try:
        start_time = datetime.now()
        generateCsvLogs()
        generateCsvEquip()
        dataInsert()
        elapsed_time = datetime.now() - start_time
        logging.info('ETL MAIN - Finalizado elapsed_time: %s', elapsed_time)

    except Exception as e:
        logging.exception('ETL MAIN - Exception: %s', e, exc_info=True)

if __name__ == '__main__':
    main()
