#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging,xml,json,datetime
import os
from os import path
import DatabaseManagerSingleton,ReadWheaterValues
from Station import Station
import esri

now = datetime.datetime.now()
strnow = now.strftime("%m_%d_%Y")#-%H-%M-%S
dirpath = os.getcwd()
foldername = os.path.basename(dirpath)
logfolder = os.path.join(dirpath,'Logs')
if os.path.isdir(logfolder) == False:
    os.mkdir(logfolder)
filename = os.path.join(logfolder , 'DbUpdater'+strnow+'.log')
logging.basicConfig(format='%(asctime)s %(message)s',filename=filename,level=logging.DEBUG,filemode='w')
logging.debug('Inicio de aplicacion de lectura de estaciones metereologogicas')

readfromWheater = True
readfromAemet = True

reading = True  
updating = True
publishing = True

if reading:
    try:
        #Obtenemos el resultado de la API de AEMET con todas las estaciones
        ReadWheaterValues.peticionesaAPI = 0


        #LEER DE BBDD LAS ESTACIONES
        logging.debug('Obtenemos de BBDD los identificadores de las estaciones para la obtencion de datos metereologicos')
        
        stations = DatabaseManagerSingleton.getStationsInfo()
        logging.debug('Se han leido un total de ' + str(len(stations)) + ' de BBDD')


        print('Se han leido un total de ' + str(len(stations)) + ' de BBDD')
        before = sum(1 for i in stations if i.acumulado != -1)
        ReadWheaterValues.readallStations(stations)
        #Comprobamos cuantas se han editado
        after = sum(1 for i in stations if i.acumulado != -1)
        print('Se han obtenido valores de precipitacion de :' + str(after))
        logging.debug('Leidas todas las estaciones. Hacemos la insercion')
        inserciones = DatabaseManagerSingleton.makeInserts(stations)
        print('Se han insertado en BBDD valores de precipitacion de :' + str(inserciones))
        logging.debug('Se han insertado un total de ' + str(inserciones) + ' de BBDD')
    except Exception as e:
        logging.error(e)

    logging.debug('Finalizando aplicacion de lectura de estaciones meteorologicas');
else:
    logging.debug('La aplicación está configurada para no leer el valor de las estaciones')

if updating:
    try:
        logging.debug('Pasamos a hacer update de la BBDD para los valores actuales')

        registrosactualizados = DatabaseManagerSingleton.updateMeteoPoints()
        if registrosactualizados > 0:
            logging.debug('Actualizacion realizada correctamente')
    except Exception as e:
        logging.error(e.message)

if publishing:
    try:
        logging.debug('Pasamos a publicar los Raster del día')
        esri.publishdayRasters()
    except Exception as e:
        logging.error(e.message)







