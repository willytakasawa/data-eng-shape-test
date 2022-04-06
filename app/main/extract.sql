--Total equipment failures that happened?
query_1 = WITH temp AS (SELECT DISTINCT(CONCAT(dt_failure, '',  sensor)) FROM tb_failure) SELECT COUNT(*) AS n_failures FROM temp;

--Which equipment code had most failures?
query_2 = WITH temp AS(SELECT tb_failure.*, tb_equipment.code FROM tb_failure LEFT JOIN tb_sensor ON tb_failure.sensor = tb_sensor.sensor_id LEFT JOIN tb_equipment ON tb_sensor.equipment_id = tb_equipment.equipment_id) SELECT code, COUNT(*) AS counter FROM temp GROUP BY code ORDER BY counter DESC;

--Average amount of failures across equipment group, ordered by the number of failures in ascending order?
query_3 = WITH temp AS(SELECT tb_failure.*, tb_equipment.equipment_id, tb_equipment.group_name FROM tb_failure LEFT JOIN tb_sensor ON tb_failure.sensor = tb_sensor.sensor_id LEFT JOIN tb_equipment ON tb_sensor.equipment_id = tb_equipment.equipment_id), temp2 AS(SELECT equipment_id, group_name, COUNT(*) as n_failures FROM temp GROUP BY equipment_id) SELECT group_name, AVG(n_failures) AS avg_n_failures FROM temp2 GROUP BY group_name ORDER BY avg_n_failures ASC;
