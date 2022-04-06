import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
import logging

logger:logging.Logger

logger = logging.getLogger('etl-process')

def connectionSql():
    try:
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

        connection_str = f'mysql+pymysql://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}'
        
        engine = sqlalchemy.create_engine(connection_str, connect_args={'client_flag':0})

        connection = engine.connect()
        
        if connection:
            logging.debug('db_generic - Conexao ao MySQL realizada com sucesso')
    
    except SQLAlchemyError as e:
            logging.exception('db_generic - Erro ao se conectar ao MySQL - [%s]', e)

    return connection
