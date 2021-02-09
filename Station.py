#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import logging
import ReadWheaterValues


readfromOpenWheater = True
class Station():
    tipeStation = ''
    nombre = ''
    provincia = ''
    identificador = None
    indicativo = None
    acumulado = -1.0
    fecha_lectura = None
    x = None
    y = None
    

    def __init__(self,_identificador):
        if _identificador is None:
            raise Exception('No se puede instanciar una estacion sin identificador')
        else:
            self.identificador = _identificador


    def __readMyWheater__(self):
        #print('Leyendo los datos de la estaci�n ' + self.identificador)

        if self.identificador is not None and self.tipeStation is not None:
            if self.tipeStation == 'AUTOMATICAS':
                logging.debug(str(self.identificador) + ' ' + self.nombre + ' via api AEMET')

                #Eliminado por petición única para todas las estaciones

                #ReadWheaterValues.readdatafromAEMET(self)
                #Se le asginaran después
                self.fecha_lectura = datetime.datetime.now
            elif self.tipeStation == 'OPENWHEATERMAP':
                print('Leyendo estacion '+ str(self.identificador) + ' desde OpenWheaterMap')
                if readfromOpenWheater == True:
                    ReadWheaterValues.ReadDataFromOpenWheater(self)
                else:
                    message = 'La lectuda desde API OpenWheater está desactivada'
                    print(message)
                    logging.debug(message)
            else:
                message = str(self.identificador) + ' ' + self.nombre + ' via AEMET todas estaciones'
                logging.debug(message)
                print(message)
        else:
            raise Exception('No se puede leer una estacion sin su identificador ni su tipo de estacion')
        