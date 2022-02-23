from db_engine import PSEngine
from exporters import fp_metrics

LOGIN = ""
PASSWORD = ""

if __name__ == "__main__":
	db = PSEngine('172.17.130.30', 'marts')
	db.connect(LOGIN, PASSWORD, schema="national_projects_2021")

	cursor = db.get_cursor()

	db.disconnect()