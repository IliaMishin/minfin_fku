import psycopg2

class PSEngine:
    def __init__(self, host, db_name, port=None):
        self.host = host
        self.db_name = db_name
        self.port = port or "5432"

    def connect(self, login, password, schema=None):
        opts_string = ""
        if schema:
            opts_string = f"-c search_path={schema}"
        self.connection = psycopg2.connect(host=self.host,
                                           user=login,
                                           password=password,
                                           database=self.db_name,
                                           options=opts_string,
                                           port=self.port)

    def disconnect(self):
        self.connection.close()

    def get_cursor(self):
        return self.connection.get_cursor()
