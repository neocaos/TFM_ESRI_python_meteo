from configparser import ConfigParser
import psycopg2
import logging
import datetime
from Station import Station

class DbManager:
        def __init__(self,db,user,pwd,host):
            self._database = db
            self._user = user
            self._pwd = pwd
            self._host = host

        def __str__(self):
            return  '(Manager) DB: ' + self._database + ' user: ' + self._user + ' host: ' + self._host
        conn = None

        def isconnected(self):
            if self.conn is  None:
                return False
            else:
                return True
        
        def Connect(self):
            try:
                self.conn = psycopg2.connect(database=self._database, user=self._user,
                                password=self._pwd,host=self._host)
                self.conn.set_client_encoding('UTF8')
                return self.conn
            except:
                return False
        
        def ExecuteSelect(self,sqlQuery):
            try:
                if not self.isconnected():
                    self.Connect()
                else:
                    cursor = self.conn.cursor()
                    cursor.execute(sqlQuery)
                    return cursor
            except(Exception, psycopg2.DatabaseError) as error:
                logging.error(error.message)
        
        def ExecuteInsert(self,sqlQuery,values):
            try:
                if not self.isconnected():
                    self.Connect()
                else:
                    cursor = self.conn.cursor()
                    if type(values[1]) != datetime.datetime:
                        cursor.execute(sqlQuery,(values[0],datetime.datetime.now(),values[2]))
                    else:                    
                        cursor.execute(sqlQuery,(values[0],values[1],values[2]))

                    return cursor
            except(Exception, psycopg2.DatabaseError) as error:
                logging.error(error.message)
        
        def Commit(self):
            try:
                self.conn.commit()
            except(Exception, psycopg2.DatabaseError) as error:
                logging.error(error.message)
        
        def ExecuteUpdate(self,sqlQuery):
            try:
                if not self.isconnected():
                    self.Connect()
                else:
                    cursor = self.conn.cursor()
                    cursor.execute(sqlQuery)
                    return cursor
            except(Exception, psycopg2.DatabaseError) as error:
                logging.error(error.message)
            
        
class DbManagerSingleton(DbManager):
    instance = None
    def __init__(self,db,user,pwd,host):
        if not DbManagerSingleton.instance:
            DbManagerSingleton.instance = DbManager(db,user,pwd,host)
        else:
            return DbManagerSingleton.instance
    
    def __str__(self):
            return  '(Singleton) DB: ' + self.instance._database + ' user: ' + self.instance._user + ' host: ' + self.instance._host

#Example
# try:
#     x = DbManagerSingleton('canyons','owner','owner','localhost')

    
#     print(x.instance.isconnected())
#     conection = x.instance.Connect()
#     print(x.instance.isconnected())

#     myQuery = "SELECT INDICATIVO,B1 FROM owner.estaciones_aemet LIMIT 10"

#     for x in x.instance.ExecuteSelect(myQuery):
#         print(x)



# except Exception as e:
#     print(e.message)
def getStationsInfo(aemet_stations = True,openwheater_stations = True,limit = None):

    logging.debug('Obteniendo identificadores y demas informacion de las estaciones')
    try:
        x = DbManagerSingleton('canyons','owner','owner','localhost')

        
        print(x.instance.isconnected())
        conection = x.instance.Connect()
        print(x.instance.isconnected())

        #myQuery = "SELECT INDICATIVO,NOMBRE,PROVINCIA,COORD_X,COORD_Y,TIPO FROM owner.full_stations LIMIT 100"
        # myQuery = "SELECT INDICATIVO,LAT,LONG,TIPO FROM owner.full_stations where tipo = 'AUTOMATICAS'"
        if aemet_stations == True and openwheater_stations == False:
            myQuery = "SELECT OBJECTID,LAT,LONG,TIPO,INDICATIVO FROM OWNER.METEOPOINTS WHERE TIPO <> 'OPENWHEATERMAP'"
        elif aemet_stations == False  and openwheater_stations == True:
            myQuery = "SELECT OBJECTID,LAT,LONG,TIPO,INDICATIVO FROM OWNER.METEOPOINTS WHERE TIPO = 'OPENWHEATERMAP'"
        else:
            myQuery = "SELECT OBJECTID,LAT,LONG,TIPO,INDICATIVO FROM OWNER.METEOPOINTS"

        if limit is not None:
            myQuery += ' LIMIT ' + str(limit)

        #errores '2536D'

        #INDEX
        #0 INDICATIVO
        #1 NOMBRE
        #2 PROVINCIA
        #3 X
        #4 Y
        #5 TIPO
        fullestaciones = []

        

        for x in x.instance.ExecuteSelect(myQuery):
            try:
                #print(x)
                id = int(x[0])
                #nombre = x[1].decode('utf-8').encode('latin-1')
                #provincia = x[2]
                lat = x[1]
                longit = x[2]
                tipo = x[3]
                indicativo = x[4]

                a = Station(_identificador=id)
                #a.nombre = nombre
                a.x = longit
                a.y = lat
                a.indicativo = indicativo
                a.tipeStation = tipo.upper()

                a.__readMyWheater__()
                fullestaciones.append(a)
                logging.debug('Leida la estacion '   + str(a.identificador))


            except Exception as e:
                logging.error('No se ha podido leer correctamente la estacion ' + x[0])
                logging.error(e.message)

        return fullestaciones

    except Exception as e:
        print(e.message)


def makeInserts(fullstations):
    try:
        logging.debug('Hacemos las inserciones en BBDD')

        x = DbManagerSingleton

        print(x.instance.isconnected())
        conection = x.instance.Connect()
        print(x.instance.isconnected())

        filasinsertadas = 0
        for station in fullstations:
            if station.acumulado == -1:
                continue
            
            values = [station.identificador,station.fecha_lectura,station.acumulado]
            myQuery = "INSERT INTO owner.DATAHISTORIC (ID_ESTACION,FECHA_CAPTURA,PREC_ACUMULADA) VALUES (%s,%s,%s)"
            
            filasinsertadas += x.instance.ExecuteInsert(myQuery,values).rowcount
            

        logging.debug('Se han insertado ' + str(filasinsertadas))
        x.instance.Commit()
        return filasinsertadas

    except Exception as e:
        logging.error('Se ha producido un error insertando los datos de las estaciones ')
        logging.error(e.message)
    


def insertTest():

    try:
        logging.debug('Hacemos las inserciones en BBDD')
        x = DbManagerSingleton('canyons','owner','owner','localhost')

        print(x.instance.isconnected())
        conection = x.instance.Connect()
        print(x.instance.isconnected())

        myQuery = "INSERT INTO owner.WHEATERHISTORIC (INDICATIVO,FECHA,ACUMULADO_DIARIO) VALUES ('TEST04',CURRENT_DATE,0.25)"

        x.instance.ExecuteInsert(myQuery)

    except Exception as e:
        logging.error('Se ha producido un error insertando los datos de las estaciones ')
        logging.error(e.message)


def updateMeteoPoints():
    try:
        logging.debug('Haciendo update de la tabla MeteoPoints con los valores del historico')

        logging.debug('Hacemos las inserciones en BBDD')
        x = DbManagerSingleton('canyons','owner','owner','localhost')

        print(x.instance.isconnected())
        conection = x.instance.Connect()
        print(x.instance.isconnected())

        myQuery = """UPDATE owner.METEOPOINTS dest
	SET
	b1 = 	COALESCE((SELECT hist.PREC_ACUMULADA FROM OWNER.DATAHISTORIC hist 
					WHERE hist.ID_ESTACION = dest.OBJECTID AND hist.FECHA_CAPTURA > CURRENT_DATE -1),0),
	B2 =  COALESCE((SELECT sum(hist.PREC_ACUMULADA) FROM OWNER.DATAHISTORIC hist 
					WHERE hist.ID_ESTACION = dest.OBJECTID AND hist.FECHA_CAPTURA > CURRENT_DATE -2),0),
	b3 = 	COALESCE((SELECT SUM(hist.PREC_ACUMULADA) FROM OWNER.DATAHISTORIC hist 
					WHERE hist.ID_ESTACION = dest.OBJECTID AND hist.FECHA_CAPTURA > CURRENT_DATE -3),0),
	w1 = 	COALESCE((SELECT SUM(hist.PREC_ACUMULADA) FROM OWNER.DATAHISTORIC hist 
					WHERE hist.ID_ESTACION = dest.OBJECTID AND hist.FECHA_CAPTURA > CURRENT_DATE -7),0),
	w2 = 	COALESCE((SELECT SUM(hist.PREC_ACUMULADA) FROM OWNER.DATAHISTORIC hist 
					WHERE hist.ID_ESTACION = dest.OBJECTID AND hist.FECHA_CAPTURA > CURRENT_DATE -15),0),
	m1 = 	COALESCE((SELECT SUM(hist.PREC_ACUMULADA) FROM OWNER.DATAHISTORIC hist 
					WHERE hist.ID_ESTACION = dest.OBJECTID AND hist.FECHA_CAPTURA > CURRENT_DATE -31),0);"""

        return x.instance.ExecuteUpdate(myQuery.replace("\n","").replace("\t",""))

    except Exception as e:
        logging.error('Se ha producido un error haciendo Update de la tabla Meteopoints')
        logging.error(e.message)
