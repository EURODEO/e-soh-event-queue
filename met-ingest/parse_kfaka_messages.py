import psycopg2


class stinfosys_decoder():
    def __init__(self, db, host, user, password, port):
        self.conn = psycopg2.connect(database=db,
                                     host=host,
                                     user=user,
                                     password=password,
                                     port=port)

    def getStationMetadata(self, stationID):
        # Start a new cursor
        cursor = self.conn.cursor()

        query = "SELECT * FROM station WHERE stationid = %s;"
        cursor.execute(query, [stationID])

        try:
            return cursor.fetchall()
        finally:
            cursor.close()

    def getParamData(self, paramID):
        # Start a new cursor
        cursor = self.conn.cursor()

        query = "SELECT * FROM param WHERE paramid = %s;"
        cursor.execute(query, [paramID])

        try:
            return cursor.fetchall()
        finally:
            cursor.close()

    def getStationWigos(self, stationID):
        # Start a new cursor
        cursor = self.conn.cursor()

        query = "SELECT * FROM wigos_station WHERE stationid = %s;"
        cursor.execute(query, [stationID])

        try:
            return cursor.fetchall()
        finally:
            cursor.close()
