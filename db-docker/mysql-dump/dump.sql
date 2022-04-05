CREATE TABLE IF NOT EXISTS tb_equipment (equipment_id INTEGER(10) PRIMARY KEY NOT NULL, code VARCHAR(255), group_name VARCHAR(255));
CREATE TABLE IF NOT EXISTS tb_sensor (sensor_id INTEGER(10) PRIMARY KEY NOT NULL, equipment_id INTEGER(10));
CREATE TABLE IF NOT EXISTS tb_failure (dt_failure DATETIME NOT NULL, sensor INTEGER(10) NOT NULL, temperature FLOAT(20), vibration FLOAT(20));