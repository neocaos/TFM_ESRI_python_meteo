import arcpy
from arcpy import env
from arcpy.sa import *
import logging
from datetime import datetime
import os

def publishdayRasters():
    myname = 'publishdayRasters'

    defaultworkspace = r'C:\TrabajoFinalMasterESRI\Data\DF_Worskpace'
    rasterDefaultfolder = r'C:\TrabajoFinalMasterESRI\Data\Rasters'
    #creamos la carpeta de los rasters del dia
    now = datetime.now()
    dayfolder = now.strftime("%d%m%Y")
    rasterDefaultfolder = os.path.join(rasterDefaultfolder,dayfolder)

    if not os.path.isdir(rasterDefaultfolder) :
        os.mkdir(rasterDefaultfolder)

    # Set environment settings
    env.workspace = defaultworkspace
    env.overwriteOutput = True
    # Check out the ArcGIS Spatial Analyst extension license
    arcpy.CheckOutExtension("Spatial")
    arcpy.CheckOutExtension("3D")

    listfields = ["B1","B2","B3","W1","W2","M1"] 

    for fieldname in listfields:
        
        try:
            logging.debug('----->' + myname)
            # Set local variables
            inFeatures = "Database Connections\Owner@Canyons.sde\canyons.owner.MeteoPoints"
            cellSize = 15000
            power = 2
            searchRadius = RadiusVariable(12)

            #sitios de salida de los rasters.
            outVarRaster = os.path.join(rasterDefaultfolder,fieldname + '_' + dayfolder)
            dbraster = "Database Connections\Owner@Canyons.sde\canyons.owner." + fieldname

            
            kModel = "CIRCULAR"
            cellSize = 15000
            kRadius = 20000
            # Execute Kriging
            arcpy.Kriging_3d(inFeatures, fieldname, outVarRaster, kModel, 
                            cellSize, kRadius)
            
            #Pendiente de validar
            clip_features = "Database Connections\Owner@Canyons.sde\canyons.owner.Admin\canyons.owner.pais"
            #aqui harIamos el clip
            rectangle = extents(clip_features)
            arcpy.Clip_management(in_raster=outVarRaster,
                                    rectangle=rectangle,
                                    out_raster=dbraster,
                                    in_template_dataset=clip_features,
                                    clipping_geometry="ClippingGeometry",
                                    maintain_clipping_extent="NO_MAINTAIN_EXTENT")


            #Construimos las piramides de los rastsers 
            arcpy.BatchBuildPyramids_management(dbraster)



            logging.debug('<-----' + myname)
        except Exception as e:
            logging.error('Error ejecutando ' + myname)
            logging.error('El raster que ha dado problemas es el ' + fieldname)
            arcpy.CreateRasterDataset_management(out_path="Database Connections/Owner@Canyons.sde", 
                                                out_name="B1", 
                                                cellsize="", 
                                                pixel_type="8_BIT_UNSIGNED", 
                                                raster_spatial_reference="PROJCS['ETRS_1989_UTM_Zone_30N',GEOGCS['GCS_ETRS_1989',DATUM['D_ETRS_1989',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Transverse_Mercator'],PARAMETER['False_Easting',500000.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',-3.0],PARAMETER['Scale_Factor',0.9996],PARAMETER['Latitude_Of_Origin',0.0],UNIT['Meter',1.0]]", 
                                                number_of_bands="1", config_keyword="",pyramids="PYRAMIDS -1 NEAREST DEFAULT 75 NO_SKIP", 
                                                tile_size="128 128", compression="LZ77", pyramid_origin="-5120763,26772284 9997963,94293634")
                                                 
            logging.error(e.message)
    
    logging.debug('Todos los rasters de los campos se han creado correctamente')

def extents(fc):
    extent = arcpy.Describe(fc).extent
    west = extent.XMin
    south = extent.YMin
    east = extent.XMax
    north = extent.YMax
    width = extent.width
    height = extent.height

    finalstring = str(west) + ' ' +  str(south) + ' '+  str(east) + ' ' +  str(north)

    return finalstring


# arcpy.Clip_management(in_raster="C:/TrabajoFinalMasterESRI/Data/Rasters/23052020/b1_23052020", 
#                         rectangle="-13428,7395000001 3893743,4386 1125823,1249 4859380,2549", 
#                         out_raster="Database Connections/Owner@Canyons.sde/canyons.owner.B1", 
#                         in_template_dataset="Database Connections/Owner@Canyons.sde/canyons.owner.Admin/canyons.owner.Pais", n
#                         odata_value="-3,402823e+38", clipping_geometry="ClippingGeometry", maintain_clipping_extent="NO_MAINTAIN_EXTENT")