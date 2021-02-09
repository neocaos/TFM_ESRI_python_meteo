#!/usr/bin/env python
# -*- coding: utf-8 -*-
import http.client,json
import getAemetAPI
import logging
import Station
from decimal import Decimal
import datetime
import time
from dateutil import tz
import copy
#my api:


def readdatafromAEMET(station):
    JRMAPI = getAemetAPI.getAPI('mypassword')
    conn = http.client.HTTPSConnection("opendata.aemet.es")
    headers = {
    'cache-control': "no-cache"
    }
    ###
    # Base
    # conn.request("GET", "/opendata/api/valores/climatologicos/inventarioestaciones/todasestaciones/?api_key=" + JRMAPI, headers=headers)
    # 
    ##
    #https://opendata.aemet.es/opendata/api/observacion/convencional/datos/estacion/9115X/?api_key=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJqYXZpZXJyaXZhc2VzQGdtYWlsLmNvbSIsImp0aSI6ImNmNjQ2ZWIwLThmNDQtNDllZC04YzQxLWVjZThjY2I2ODJkYSIsImlzcyI6IkFFTUVUIiwiaWF0IjoxNTgyNzMyNzc5LCJ1c2VySWQiOiJjZjY0NmViMC04ZjQ0LTQ5ZWQtOGM0MS1lY2U4Y2NiNjgyZGEiLCJyb2xlIjoiIn0.USNStiBfYxOdMYm2VE5WKtnNTR-TJKV0RVnqXn6JFyA
    # 
    
    

    getstationdatauri = "/opendata/api/observacion/convencional/datos/estacion/"+ station.identificador + "/?api_key=" + JRMAPI

    conn.request("GET",getstationdatauri,headers=headers)
    res = conn.getresponse()
    data = res.read()
    js_data = json.loads(data)

    if js_data['estado'] == 200:
        newuri = js_data['datos']
        conn.request("GET",newuri)
        newresponse = conn.getresponse()
        realdata = newresponse.read()
        try:
            try:
                datafromstation = json.loads(realdata,encoding='ascii')
            except:
                ascidata = unicode(realdata,errors='replace')
                datafromstation = json.loads(ascidata)

            totalacumulado = 0
            for medicion in datafromstation:
                totalacumulado += medicion['prec']

            logging.debug('Total acumulado para la estacion ' + station.identificador + ' : ' + str(totalacumulado))
            station.acumulado = totalacumulado
        except Exception as e:
            logging.debug('Se ha producido un error recogiendo los datos de AEMET de la estacion ' + station.identificador)
            logging.debug(e.message)
    else:
        #Cuando la estacion no sea automatica. Hacer WebScrapping con Meteoblue.
        print(data.decode("utf-8"))


def readdatafromMeteoblue(station):
    logging.debug('Leemos los datos desde MeteoBlue.WebScrapping!')


def readallStations(all_stations):



    try:
        JRMAPI = getAemetAPI.getAPI('mypasword')
        conn = http.client.HTTPSConnection("opendata.aemet.es")
        headers = {
        'cache-control': "no-cache",
        'api_key': JRMAPI
        }
        logging.debug('Leemos los datos de las estaciones obtenidas por BBDD')
        ##https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/2020-05-07T00%3A00%3A00UTC/fechafin/2020-05-09T00%3A00%3A00UTC/todasestaciones
        #�ltima Request
        ###conn.request("GET", "/opendata/api/valores/climatologicos/inventarioestaciones/todasestaciones/?api_key=" + JRMAPI, headers=headers)

        #fechafin = today
        #fechaini = today -5 dias. Si encontramos dos veces una estación , la incluimos , puesto que luego haremos la media de los datos.

        now = datetime.datetime.now() #- datetime.timedelta(days=1)
        ini = now - datetime.timedelta(days=5)
        strnow = now.strftime("%Y-%m-%d")
        strini = ini.strftime("%Y-%m-%d")

        finaluri = '/opendata/api/valores/climatologicos/diarios/datos/fechaini/'+strini + 'T00%3A00%3A00UTC/fechafin/'+strnow+'T00%3A00%3A00UTC/todasestaciones'

        print(finaluri)
        logging.debug(finaluri)

        conn.request("GET",finaluri,
        headers=headers)
        
        newres = conn.getresponse()
        newdata = newres.read()
        jsfull  = json.loads(newdata)

        if jsfull['estado'] == 200:
            newuri = jsfull['datos']
            conn.request("GET",newuri)
            newresponse = conn.getresponse()
            realdata = newresponse.read()
            try:
                all_stations_info = json.loads(realdata)
            except:
                ascidata = unicode(realdata,errors='replace')
                all_stations_info = json.loads(ascidata)
            
            for stationinfo in all_stations_info:
                try:
                    stationinfo['prec']#si no tiene valor de precipitacion, pasamos
                except:
                    continue
                
                indi_procesada = stationinfo['indicativo']
                station_in_my_list = next((x for x in all_stations if x.indicativo == indi_procesada), None)
                if station_in_my_list is not None:
                    if station_in_my_list.acumulado == -1:

                    #Si no se ha actualizado ya su valor de precipitación, es el primer día que leemos

                        station_in_my_list.acumulado = 0
                        try:
                            acumulado = Decimal(stationinfo['prec'].replace(',','.'))
                        except:
                            acumulado = Decimal(0,0)
                        station_in_my_list.acumulado += acumulado
                        station_in_my_list.fecha_lectura = datetime.datetime.strptime(stationinfo['fecha'], '%Y-%m-%d') 
                        print(str(station_in_my_list.identificador) + ' actualizada precipitacion')
                    else:

                        #Si ya se ha actualizado, hay que crear una Station (ficticia), para añadir a la lista y añadir su precipitación de otro día
                        newstation = copy.copy(station_in_my_list)
                        newstation.acumulado = 0
                        try:
                            acumulado = Decimal(stationinfo['prec'].replace(',','.'))
                        except:
                            acumulado = Decimal(0,0)
                        newstation.acumulado += acumulado
                        newstation.fecha_lectura = datetime.datetime.strptime(stationinfo['fecha'], '%Y-%m-%d') 
                        print(str(newstation.identificador) + ' actualizada precipitacion')
                        #por último, añadimos la newstation a la lista.
                        all_stations.append(newstation)

                logging.debug('Obteniendo el valor para la estacion')

    except Exception as e:
        logging.error('Ha habido un error leyendo las estaciones ')
        logging.error(e.message)

readopenWheater = True
def ReadDataFromOpenWheater(station):
    if readopenWheater == False:
        return
    try:
        logging.debug('Leyendo estacion ' + str(station.identificador))
        lat = station.x
        lon = station.y
        apikey = 'e71a00055f0a12ea1e4951d0d50a70b7'
        #http://api.openweathermap.org/data/2.5/onecall/timemachine?lat=42&lon=1&dt=1589410800&appid=e71a00055f0a12ea1e4951d0d50a70b7&units=metric
        baseuri = r'api.openweathermap.org'
        baseurl = r'/data/2.5/onecall/timemachine?lat=%s&lon=%s&dt=%s&appid=%s&units=metric'

        #Gestión del tiempo
        today = datetime.datetime.now()
        dia = 1
        finded = False
        while dia <= 5: #aprovechamos para buscar un valor de lluvia para cada punto en los 5 días que 
                        #ofrece la API de OPENWHEATERMAP
            yesterday = today - datetime.timedelta(days = dia)
            y_in_utc = int((yesterday - datetime.datetime(1970, 1, 1)).total_seconds())

            finalurl = baseurl % (lat,lon,y_in_utc,apikey)

            conn = http.client.HTTPSConnection(baseuri)
            headers = {
                'cache-control': "no-cache"
                }
            conn.request("GET",finalurl,headers=headers)
            newres = conn.getresponse()
            newdata = newres.read()
            jsfull  = json.loads(newdata)

            daywheater = jsfull['hourly']
            for x in daywheater:
                if not 'rain' in x:
                    continue
                else:
                    dic = x['rain']
                    valor = dic.values()[0]

                    station.acumulado = valor
                    station.fecha_lectura = yesterday
                    print('Hemos encontrado un día en un punto en el que ha llovido')

                    finded = True
                    break
                    

            if finded == True:
                break
            else:
                dia +=1
    except Exception as e:
        logging.error('Error capturando datos de OpenWheaterMap')
        logging.error(e.message)