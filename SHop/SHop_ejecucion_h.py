# PAQUETES PARA CORRER OP.
import pandas as pd
import numpy as np
import datetime as dt
import json
import wmf.wmf as wmf

import hydroeval
import glob
import os
import MySQLdb

#modulo pa correr modelo
import SH_operacional_py3_v2020 as SHop

import warnings
warnings.filterwarnings('ignore')

#STYLE
# fuente
import matplotlib 
import matplotlib.font_manager as fm
import matplotlib.dates as mdates
import matplotlib.font_manager as font_manager

font_dirs = ['/media/nicolas/Home/Jupyter/Sebastian/AvenirLTStd-Book']
font_files = font_manager.findSystemFonts(fontpaths=font_dirs)
font_list = font_manager.createFontList(font_files)
font_manager.fontManager.ttflist.extend(font_list)

matplotlib.rcParams['font.family'] = 'Avenir LT Std'
matplotlib.rcParams['font.size']=12

import pylab as pl 
#COLOR EDGES
pl.rc('axes',labelcolor='#4f4f4f')
pl.rc('axes',linewidth=1.5)
pl.rc('axes',edgecolor='#bdb9b6')
pl.rc('text',color= '#4f4f4f')


###############################################################################################################
#ARGUMENTOS.

configfile='/media/nicolas/Home/Jupyter/Soraya/SH_op/SHop_SM_E260_90m_1d/configfile_SHop_SM_E260_1h.md'
save_hist = True
date = dt.datetime.now().strftime('%Y-%m-%d %H:%M') #'2020-11-01 22:13'#

########################################################################3

#EJECUCION.


ConfigList= SHop.get_rutesList(configfile)
# abrir simubasin
path_ncbasin = SHop.get_ruta(ConfigList,'ruta_nc')
cu = wmf.SimuBasin(rute=path_ncbasin)
#sets para correr modelo.
SHop.set_modelsettings(ConfigList)
warming_steps =  5*24#pasos de simulacion no seg, dependen del dt.
warming_window ='%ss'%int(wmf.models.dt * warming_steps) #siempre en seg
dateformat_starts = '%Y-%m-%d %H'
dateformat_binrain = '%Y%m%d%H%M'

#definicion de ventanas
starts  = ['%ss'%(0),'%ss'%(0)]
starts_names = ['1d','1d'] #starts y windows deben ser del mismo len.
window_end = '%ss'%(0*24*60*60) #hora actual

#definicion de executionprops
df_executionprops_h = pd.DataFrame([starts,
                                  starts_names,
                                  ['/media/nicolas/maso/Soraya/SHOp_files/SHop_SM_E260_90m_1d/results_op/Sto_op-p01-ci1-90d.StOhdr',
                                  'reglas_pant'],
                                  ['ci2','ci3'],
                                  #[1.0 , 5.9 , 5.7 , 0.0 , 1.0 , 1.0 , 10.8 , 1.0 , 1.0 , 1.0 ],
                                  [[0.8 , 10 , 17.7 , 0.0 , 9.0 , 2.0 , 15 , 0.9 , 1.0 , 1.0, 1.0 ],
                                   [0.8 , 10 , 17.7 , 0.0 , 9.0 , 2.0 , 15 , 0.9 , 1.0 , 1.0, 1.0 ]],
                                  ['-p01','-p01'],
                                  [0,0]], #pasos de sim, depende de dt
                                 columns = [1,2],
                                 index = ['starts','start_names','CIs','CI_names','pars','pars_names','wup_steps']).T

print ('#########################')
print ('Start HOURLY execution: %s'%dt.datetime.now())    
#ventanas de tiempo en que se correra


#dates
start_o = pd.to_datetime(pd.to_datetime(date).strftime('%Y-%m-%d')) - pd.Timedelta('2d')#arranca desde las 00 del dia anterior para tener 1d de calentamiento.

starts_w_h = [start_o - pd.Timedelta(start) for start in starts]
starts_m_h = [start_w_h - pd.Timedelta(warming_window) for start_w_h in starts_w_h]
end_h = pd.to_datetime(pd.to_datetime(date).strftime(dateformat_starts)) + pd.Timedelta(window_end)


#rainfall  : takes 3min
pseries,ruta_out_rain_h = SHop.get_rainfall2sim(ConfigList,cu,path_ncbasin,[starts_m_h[0]],end_h, #se corre el bin mas largo.
                                             Dt= float(wmf.models.dt),include_escenarios=None,
                                             evs_hist= False,
                                             check_file=True,stepback_start = '%ss'%int(wmf.models.dt*1),
                                             complete_naninaccum=True,verbose=False)

print (ruta_out_rain_h)

SHop.set_modelsettings(ConfigList)
# set of executions
ListEjecs_h =  SHop.get_executionlists_fromdf(ConfigList,ruta_out_rain_h,cu,starts_m_h,end_h,df_executionprops_h,
                                         warming_steps=warming_steps, dateformat_starts = dateformat_starts,
                                         path_pant4rules = ruta_out_rain_h)

# #execution
print ('Start simulations: %s'%dt.datetime.now())
print ('start: %s - end: %s'%(starts_m_h[0], end_h))
#     SHop.set_modelsettings(ConfigList)
res = SHop.get_qsim(ListEjecs_h,set_CI=True,save_hist=save_hist,verbose = True)
print ('End simulations: %s'%dt.datetime.now())


#########################################################################

#GRAFICAS

#PARA CAUDAL.
#tramos, para sacar los datos del modelo.
df_tramos = pd.read_csv('/media/nicolas/maso/Soraya/SHOp_files/nc_basin2sim/E260_90m_py2_tramos.csv',index_col=0)
ests = df_tramos.index
#curvas que escogí.
df_est_features = pd.DataFrame.from_csv('/media/nicolas/maso/Soraya/SHOp_files/SHop_SM_E260_90m_1d/temp/df_curvascal_frankenstein_20200915.csv')
ests = df_est_features.index
tramos_ids = np.concatenate(df_tramos.loc[ests].values)
df_est_features['tramo'] = list(map(str,tramos_ids))

#df con metadatos de estaciones
#METADATOS BD

server   = '192.168.1.100'
user   = 'siata_Consulta'
passwd = 'si@t@64512_C0nsult4'
dbname     = 'siata'

conn_db = MySQLdb.connect(server,user,passwd,dbname)
db_cursor = conn_db.cursor ()
query = 'select codigo,nombreestacion,longitude,latitude,estado from estaciones where codigo in ("%s","%s","%s","%s","%s","%s","%s","%s","%s");'%(ests[0],ests[1],ests[2],ests[3],ests[4],ests[5],ests[6],ests[7],ests[8])#,"%s","%s","%s","%s"

db_cursor.execute (query)
data = db_cursor.fetchall()
conn_db.close()
fields = [field.lower() for field in list(np.array(db_cursor.description)[:,0])]
df_bd = pd.DataFrame(np.array(data), columns = fields)
df_bd['codigo'] = list(map(int,df_bd['codigo']))
df_bd = df_bd.set_index('codigo')

# df_bd = df_bd.loc[df_bd.index[:-2]]

df_bd.longitude = list(map(float,df_bd.longitude.values))
df_bd.latitude = list(map(float,df_bd.latitude.values))


#OTROS ARGS
start = starts_w_h[0]
end= end_h
Dt = '1h'
colors_d = ['lightgreen','g']#['c','darkblue']#
ylims = [5,20,50,100,200,100,300]

path_r = ruta_out_rain_h.split('.')[0]+'.hdr'
path_masks_csv = path_masks_csv= '/media/nicolas/maso/Soraya/SHOp_files/nc_basin2sim/df_E260_90m_py2_subbasins_posmasks.csv'
rutafig = '/media/nicolas/Home/Jupyter/Soraya/SH_op/SHop_SM_E260_90m_1d/validacion/'


#GRAFICA
print ('----------')
print ('Start plotting: %s'%dt.datetime.now())
SHop.plot_Q(start,end,Dt,server,user,passwd,dbname,
            ListEjecs_h,cu,ests,colors_d,path_r,path_masks_csv,
            df_bd,df_est_features,ylims,rutafig=rutafig)
print ('End plotting: %s'%dt.datetime.now())