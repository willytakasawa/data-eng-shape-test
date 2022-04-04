import sqlite3
import os

class databaseGeneric(object):
    db_host: str
    db_port: str
    db_user: str
    db_password: str
    db_name: str

    def __inti__(self):
        db_host = ''
        db_port =  ''
        db_user = ''
        db_passowrd = '' 
        db_name = ''

    def connect(self):
        connection = sqlite3.connect('')
        return connection

    