import json
import pandas as pd
import logging
from datetime import datetime
from db_generic import connectionSql
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError

logger:logging.Logger

logger = logging.getLogger('answers-process')

def ansFirstQuestion():
    connection = connectionSql()
    try:
        logging.debug("ANSWER - 1 - Iniciando o de extracao")
        query_1 = ("WITH temp AS \
            (SELECT DISTINCT(CONCAT(dt_failure, '',  sensor)) \
                FROM tb_failure \
                    WHERE dt_failure BETWEEN '2020-01-01 00:00:00' AND '2020-01-30 23:59:59') \
                        SELECT COUNT(*) AS n_failures FROM temp;")
        ans_1 = connection.execute(query_1)
        
        data = []
        for x in ans_1:
            data.append(x)
        
        df_1 = pd.DataFrame(data, ["total_failure"])
        print('\n'*2 + 'ANSWER 1:' + '\n'*2 + df_1.to_string(index=False))
        df_1.to_excel('answers/answer1.xlsx', index=False)
        
    except SQLAlchemyError as e:
        logging.exception('ANSWER - 1 - Exception: %s', e, exc_info=True)

def ansSecondQuestion():
    connection = connectionSql()
    try:
        logging.debug("ANSWER - 2 - Iniciando o de extracao")
        query_2 = (
            "WITH temp AS\
                (SELECT tb_failure.*, tb_equipment.code \
                    FROM tb_failure LEFT JOIN tb_sensor ON tb_failure.sensor = tb_sensor.sensor_id \
                        LEFT JOIN tb_equipment ON tb_sensor.equipment_id = tb_equipment.equipment_id \
                            WHERE dt_failure BETWEEN '2020-01-01 00:00:00' AND '2020-01-30 23:59:59') \
                                SELECT code, COUNT(*) AS counter FROM temp GROUP BY code ORDER BY counter DESC;"
        )
        ans_2 = connection.execute(query_2)
        data = []
        
        for x in ans_2:
            data.append(x)
        
        df_2 = pd.DataFrame(data, columns = ['equipment_code', 'n_failures'])
        print('\n'*2 + 'ANSWER 2:' + '\n'*2 + df_2.to_string(index=False))
        df_2.to_excel('answers/answer2.xlsx', index=False)
        
    except SQLAlchemyError as e:
        logging.exception('ANSWER - 2 - Exception: %s', e, exc_info=True)

def ansThirdQuestion():
    connection = connectionSql()
    try:
        logging.debug("ANSWER - 3 - Iniciando o processo de extracao")
        query_3 = (
            "WITH temp AS \
                (SELECT tb_failure.*, tb_equipment.equipment_id, tb_equipment.group_name \
                    FROM tb_failure \
                        LEFT JOIN tb_sensor ON tb_failure.sensor = tb_sensor.sensor_id \
                            LEFT JOIN tb_equipment ON tb_sensor.equipment_id = tb_equipment.equipment_id \
                                WHERE dt_failure BETWEEN '2020-01-01 00:00:00' AND '2020-01-30 23:59:59'), \
                                    temp2 AS(SELECT equipment_id, group_name, COUNT(*) as n_failures \
                                        FROM temp GROUP BY equipment_id) \
                                            SELECT group_name, AVG(n_failures) AS avg_n_failures \
                                                FROM temp2 GROUP BY group_name ORDER BY avg_n_failures ASC;"
        )
        ans_3 = connection.execute(query_3)
        data = []
        for x in ans_3:
            data.append(x)
        df_3 = pd.DataFrame(data, columns = ['group_name', 'avg_failures'])
        print('\n'*2 + 'ANSWER 3:' + '\n'*2 + df_3.to_string(index=False))
        df_3.to_excel('answers/answer3.xlsx', index=False)
            
        
    except SQLAlchemyError as e:
        logging.exception('ANSWER - 3 - Exception: %s', e, exc_info=True)

def main():
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        filename='answers-process.log',
        filemode='w+',
        level=logging.DEBUG
    )

    try:
        start_time = datetime.now()
        ansFirstQuestion()
        ansSecondQuestion()
        ansThirdQuestion()
        elapsed_time = datetime.now() - start_time
        logging.info('ANSWER MAIN - Finalizado elapsed_time: %s', elapsed_time)

    except Exception as e:
        logging.exception('ANSWER MAIN - Exception: %s', e, exc_info=True)

if __name__ == '__main__':
    main()


