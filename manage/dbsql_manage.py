import os
from mysql import connector
from dotenv import load_dotenv

load_dotenv()

db_con = connector.connect(host=os.getenv('DB_HOST'),
                           database=os.getenv('DB_NAME'),
                           user=os.getenv('DB_USER'),
                           password=os.getenv('DB_PASSWORD'))

watcher_params = {}


def get_watcher_params_db():
    try:
        cursor = db_con.cursor()
        sql = "SELECT pname, pvalue, description FROM fix_params WHERE ptype = 'w'"
        cursor.execute(sql)
        rows = cursor.fetchall()

        for row in rows:
            watcher_params[row[0]] = row[1]
        cursor.close()
        return watcher_params
    except connector.Error as error:
        print("Failed to create table in MySQL: {}".format(error))
        return None


def closeDB():
    db_con.close()
