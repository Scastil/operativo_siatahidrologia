#!/usr/bin/env python
# -*- coding: utf-8 -*-

# FUENTE
import matplotlib
matplotlib.use('Agg')
import matplotlib.font_manager as fm
import matplotlib.dates as mdates
import matplotlib.font_manager as font_manager
import locale
locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')

font_dirs = ['/media/nicolas/Home/Jupyter/Sebastian/AvenirLTStd-Book']
font_files = font_manager.findSystemFonts(fontpaths=font_dirs)
font_list = font_manager.createFontList(font_files)
font_manager.fontManager.ttflist.extend(font_list)

matplotlib.rcParams['font.family'] = 'Avenir LT Std'
matplotlib.rcParams['font.size'] = 9

import pylab as pl 
#COLOR EDGES
pl.rc('axes',labelcolor='#4f4f4f')
pl.rc('axes',linewidth=0.75)
pl.rc('axes',edgecolor='#4f4f4f')
pl.rc('text',color= '#4f4f4f')
pl.rc('text',color= '#4f4f4f')
pl.rc('xtick',color='#4f4f4f')
pl.rc('ytick',color='#4f4f4f')


import MySQLdb
import pandas as pd
import numpy as np 
import glob 
import os
import pylab as pl
import datetime as dt
import humedad as hm

import warnings
warnings.filterwarnings('ignore')


##########################################################################ARGUMENTOS

host = '192.168.1.74'
user = 'siata_Oper'
passw = 'si@t@64512_operacional'
dbname = 'siata'

# host   = '192.168.1.100'
# user   = 'siata_Consulta'
# passwd = 'si@t@64512_C0nsult4'
# dbname = 'siata'

# query = 'describe estaciones'
query = 'select codigo, nombreestacion,estado,hd,red from estaciones where red in ("humedad","humedad_stevens","humedad_laderas_5te_rasp","humedad_stevens_laderas_rasp") and estado in ("A","P")'
df_metadata = hm.query_mysql(host,user,passw,dbname,query)

df_metadata = df_metadata.set_index('codigo')
df_metadata.index = list(map(int,df_metadata.index))
df_metadata.columns = ['nombreestacion','estado', 'tipo_sensor', 'red']
df_metadata.tipo_sensor = list(map(int,df_metadata.tipo_sensor.values))

##########################################################################ASIGNACIONES
codigos_pluvio = np.array([20,288,189,25,43,57,295,43,295,389,373,418])

#si en la consulta hay mas filas que pluvio, se descartan las filas que excden el size. puede que haya una estacion nueva y no nos hayan contado.
#No se grafica hasta que se tengan todos los metadatos, 
if df_metadata.shape[0]>codigos_pluvio.size:
    print('Warnign: Possibly there are more stations than pluviometers assigned. %s vs. %s'%(df_metadata.shape[0],codigos_pluvio.size))
    df_metadata = df_metadata.loc[df_metadata.index[:codigos_pluvio.size]]
    
else:
    pass


df_metadata[['p_asociado','depths2drop','depths_laderas']] = pd.DataFrame([codigos_pluvio,
                                                          np.array([None,[2],[1],[1,3],None,None,None,None,None,[3],[2,3],[3]]),
                                                          np.array([None,None,None,None,None,None,None,None,None,['1','1.2'],['1.36'],['0.5','1']])],
                                                          columns=df_metadata.index, 
                                                          index = ['p_asociado','depths2drop','depths_laderas']).T



###############################################################################EJECUCION
#ARGUMENTOS
codesH = df_metadata.index
freq='1T'
calidad = False # False to create .csv, True for realtime
path_dfh = '/media/nicolas/maso/Soraya/data_op/humedad/'
ruta_figs = '/media/nicolas/Home/Jupyter/Soraya/Op_Alarmas/Result_to_web/figs_operacionales/humedad/'

for codeH in codesH[:]:
    hm.grafica_humedad2geoportal(codeH,freq,path_dfh,host,user,passw,dbname,df_metadata,
                                 ruta_figs=ruta_figs,calidad=calidad)