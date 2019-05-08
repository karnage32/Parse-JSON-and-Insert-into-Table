import cx_Oracle

class Oracle(object):

    def __init__(self, host, port, service, username, password):

        self.connection = cx_Oracle.connect(
            self.username,
            self.password ,
            '{host}:{port}/{service}'.format(
                self.host=host,
                self.port=port,
                self.service=service
            )
        )

        self.cursor = self.connection.cursor()

    def query(self, query, fetch=False):
        self.cursor.execute(query)

        if fetch:
            return self.cursor.fetchall()
        self.connection.commit()

    def query_many(self, query, data):
        self.cursor.executemany(query, data)

    def close(self):
        self.cursor.close()
        self.connection.close()
