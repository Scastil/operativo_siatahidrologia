
import pandas as pd
import numpy as np 
import datetime as dt
import os

rutaCodigo = '/media/nicolas/Home/Jupyter/Soraya/git/Alarmas/06_Crones/radar_otroformato/Radar2RainConvStra.py'
rutaRadar = '/media/radar/'
rutaNC = '/media/nicolas/Home/nicolas/101_RadarClass/'
fecha1 = (dt.datetime.now() - pd.to_datetime('7 days')).strftime('%Y-%m-%d')
fecha2 = dt.datetime.now().strftime('%Y-%m-%d')
hora_1 = '00:00'
hora_2 = '00:00'

print (dt.datetime.now())
comando = rutaCodigo+' '+fecha1+' '+fecha2+' '+rutaRadar+' '+rutaNC+' -1 '+hora_1+' -2 '+hora_2+' -v'
os.system(comando)
print (comando)
print (dt.datetime.now())