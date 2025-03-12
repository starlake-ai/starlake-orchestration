from typing import List, Optional, Tuple, Union
from collections import defaultdict
import os
import psycopg2
######################

#parameters


options = {}


######################

connection_parameters = {
   "host": "34.140.108.151",
   "user": "starlake",
   "password": os.environ['DEMO_PG_PASSWORD'],
   "database": "starlake",  # optional
   "port": 5431,
}



class Session:
    def __init__(self, connection_parameters):
        self.conn = psycopg2.connect(database = connection_parameters['database'], 
                        user = connection_parameters['user'], 
                        host= connection_parameters['host'],
                        password = connection_parameters['password'],
                        port = connection_parameters['port'])

    def close(self):
        self.cur.close()
        self.conn.close()

    def sql(self, stmt: str) -> List[Tuple]:
        cur = self.conn.cursor()
        cur.execute(stmt)
        if (stmt.lower().startswith("select")) or (stmt.lower().startswith("with ")):
            result = self.cur.fetchall()
        else:
            result = []
        cur.close()
        return result
    
    def commit(self):
        self.conn.commit()
        return []
    
    def rollback(self):
        self.conn.rollback()
        return []

session = Session(connection_parameters)
rows = session.sql("select * from public.slk_member where id = 1245")
rows2 = session.sql("insert into public.slk_whitelist(email_or_domain) values('gmail.com')")

rows2 = session.commit()
for row in rows:
    print(row)
session.close()