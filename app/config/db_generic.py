import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
import logging

def connectionSql():
    try:
        # specify database configurations
        config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': 'admin',
            'database': 'db'
        }
        db_user = config.get('user')
        db_pwd = config.get('password')
        db_host = config.get('host')
        db_port = config.get('port')
        db_name = config.get('database')
        # specify connection string
        connection_str = f'mysql+pymysql://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}'
        # connect to database
        engine = sqlalchemy.create_engine(connection_str)
        connection = engine.connect()
        
        print('Conexao ao MySQL realizada com sucesso')
    
    except SQLAlchemyError as e:
        print('Erro ao se conectar ao MySQL - [%s]', e)

    return connection
