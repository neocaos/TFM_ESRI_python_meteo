# Captura Datos TFM Master ESRI
Aplicación python que recoge valores metereológicos de distintas fuentes y genera capas raster en función de los datos. Posteriormente publica el .MXD

En el archivo principal (Program.py) hay 5 booleans que son parámetros importantes del proceso. 

readfromWheater = True -- Establece si leer de la API de OpenWheaterMap 
readfromAemet = True -- Establece si leer de la API de AEMET

reading = True   -- Define si leer datos
updating = True  -- Define si sobreescribir los datos en BBDD (instancia local, BBDD PostgreSQL)
publishing = True -- Define si publicar las capas Raster basadas en los nuevos datos. 
