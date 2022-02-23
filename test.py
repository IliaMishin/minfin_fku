import getpass

from db_engine import PSEngine
from exporters import fp_metrics

ADDRESS = '172.17.130.30'
DB_NAME = 'marts'
SCHEMA_NAME = schema="national_projects_2021"

def connect(login=None, addr=ADDRESS, db_name=DB_NAME, schema_name=SCHEMA_NAME):
	db = PSEngine(addr, db_name )
	
	if not login:
		login = input("Login: ")
	pw = getpass.getpass()

	db.connect(login, pw, schema=schema_name)

	metric1 = fp_metrics.FPMetric1("2072", db)	
	db.disconnect()


if __name__ == "__main__":
	connect()
	
	

	