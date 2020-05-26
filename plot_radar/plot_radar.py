#!/usr/bin/env python
# -*- coding: utf-8 -*-

# FUENTE
import matplotlib
matplotlib.use('Agg')
import matplotlib.font_manager as fm
import matplotlib.dates as mdates
import matplotlib.font_manager as font_manager

font_dirs = ['/media/nicolas/Home/Jupyter/Sebastian/AvenirLTStd-Book']
font_files = font_manager.findSystemFonts(fontpaths=font_dirs)
font_list = font_manager.createFontList(font_files)
font_manager.fontManager.ttflist.extend(font_list)

matplotlib.rcParams['font.family'] = 'Avenir LT Std'
matplotlib.rcParams['font.size'] = 12

import MySQLdb
from wmf import wmf
import pandas as pd
import numpy as np 
import glob 
import os
import pylab as pl
import datetime as dt
import cartopy.crs as crs
import geopandas as gpd
import warnings
warnings.filterwarnings('ignore')

import SH_operacional_py3 as SHop


############################################333

def logger(orig_func):
    '''logging decorator, alters function passed as argument and creates
    log file. (contains function time execution)
    Parameters
    ----------
    orig_func : function to pass into decorator
    Returns
    -------
    log file
    '''
    import logging
    from functools import wraps
    import time
    logging.basicConfig(filename = 'plot_radar.log',level=logging.INFO)
    @wraps(orig_func)
    def wrapper(*args,**kwargs):
        start = time.time()
        f = orig_func(*args,**kwargs)
        date = dt.datetime.now().strftime('%Y-%m-%d %H:%M')
        took = time.time()-start
        log = '%s:%s:%.1f sec'%(date,orig_func.__name__,took)
        print (log)
        logging.info(log)
        return f
    return wrapper

#######################################################################33
#funciones
###################


def round_time(date = dt.datetime.now(),round_mins=5):
    '''
    Rounds datetime object to nearest 'round_time' minutes.
    If 'dif' is < 'round_time'/2 takes minute behind, else takesminute ahead.
    Parameters
    ----------
    date         : date to round
    round_mins   : round to this nearest minutes interval
    Returns
    ----------
    datetime object rounded, datetime object
    '''    
    dif = date.minute % round_mins

    if dif <= round_mins/2:
        return dt.datetime(date.year, date.month, date.day, date.hour, date.minute - (date.minute % round_mins))
    else:
        return dt.datetime(date.year, date.month, date.day, date.hour, date.minute - (date.minute % round_mins)) + dt.timedelta(minutes=round_mins)

def radar_cmap(window_t,idlcolors=False):
    '''
    Parameters
    ----------
   
    Returns
    ----------
   
    '''
    import matplotlib.colors as colors
        
    if idlcolors == False:
        bar_colors=[(255, 255, 255),(0, 255, 255), (0, 0, 255),(70, 220, 45),(44, 141, 29),(255,255,75),(255,142,0),(255,0,0),(128,0,128),(102,0,102),(255, 153, 255)]
        if pd.Timedelta(window_t) < pd.Timedelta('7 days'):
            #WEEKLY,3h,EVENT.
            lev = np.array([0.,1.,5.,10.,20.,30.,45.,60., 80., 100., 150.])
        if pd.Timedelta(window_t) > pd.Timedelta('7 days'):
            #MONTHLY
            lev = np.array([1.,5.0,10.0,15.,20.0,25.0,30.0,40.0,50.,60.,70.0])*10.
    
    #IDL
    elif pd.Timedelta(window_t) <= pd.Timedelta('3h'):
        #coor para python
        bar_colors = [[  0, 255, 255],[  0, 255, 255],[  0, 255, 255],[  0,   0, 255],[ 70, 220,  45],[ 44, 141,  29],[255, 255,  75],[255, 200,  50],[255, 142,   0],[255,   0,   0],[128,   0, 128],[255, 153, 255]]        
        #original de juli en idl
        #bar_colors = [[200, 200, 200],[  0,   0,   0],[  0, 255, 255],[  0,   0, 255],[ 70, 220,  45],[ 44, 141,  29],[255, 255,  75],[255, 200,  50],[255, 142,   0],[255,   0,   0],[128,   0, 128],[255, 153, 255]]
        #3h
        lev  = np.array([0.2,1,2,4,6,8,10,13,16,20,24,30])*5.
    elif pd.Timedelta(window_t) > pd.Timedelta('3h'):
        #coor para python
        bar_colors = [[  0, 255, 255],[  0, 255, 255],[  0, 255, 255],[  0,   0, 255],[ 70, 220,  45],[ 44, 141,  29],[255, 255,  75],[255, 200,  50],[255, 142,   0],[255,   0,   0],[128,   0, 128],[255, 153, 255]]        
        #original de juli en idl
        #bar_colors = [[200, 200, 200],[  0,   0,   0],[  0, 255, 255],[  0,   0, 255],[ 70, 220,  45],[ 44, 141,  29],[255, 255,  75],[255, 200,  50],[255, 142,   0],[255,   0,   0],[128,   0, 128],[255, 153, 255]]
        #WEEKLY
        lev = np.array([0.2,1,4,7,10,13,16,20,25,30,35,50])*10.
    
    scale_factor =  ((255-0.)/(lev.max() - lev.min()))
    new_Limits = list(np.array(np.round((lev-lev.min())*scale_factor/255.,3),dtype = float))
    Custom_Color = list(map(lambda x: tuple(ti/255. for ti in x) , bar_colors))
    nueva_tupla = [((new_Limits[i]),Custom_Color[i],) for i in range(len(Custom_Color))]
    cmap_radar =colors.LinearSegmentedColormap.from_list('RADAR',nueva_tupla)
    levels_nuevos = np.linspace(np.min(lev),np.max(lev),255)
    norm_new_radar = colors.BoundaryNorm(boundaries=levels_nuevos, ncolors=256)
    return cmap_radar,list(levels_nuevos),norm_new_radar

def plot_basin_rain(codigo, mapprop, path_vector, window_t='3h',rutafig_mapas=None,
                        fontsize=16,cbar_label = u'Precipitación acumulada [mm]',title=None):

    cu = wmf.SimuBasin(rute=path_basins+'%s.nc'%(codigo))
    vec_rain = cu.Transform_Map2Basin(mapprop[0],mapprop[1])

    mapa, mprop = cu.Transform_Basin2Map(vec_rain)
    celdas_x, celdas_y,coordenada_x_abajo_izquierda, coordenada_y_abajo_izquierda, delta_x, delta_y, nodata = mprop

    longs = coordenada_x_abajo_izquierda + delta_x * np.arange(celdas_x)
    lats = coordenada_y_abajo_izquierda + delta_y * np.arange(celdas_y)
    longitudes, latitudes = np.meshgrid(longs, lats)

    if window_t == '30m_ahead':
        cmap,levels,norm = radar_cmap('30m')
    else:
        cmap,levels,norm = radar_cmap(window_t)

    fig = pl.figure(figsize=(7,7),dpi=95)

    ax = fig.add_subplot(111, projection = crs.PlateCarree())
    cs = ax.contourf(longitudes,latitudes,mapa.T[::-1], transform = crs.PlateCarree(),
                     cmap = cmap, levels = levels, norm = norm, extend = 'both')

    data = gpd.read_file(path_vector+"cuencas/%s/"%(codigo))
    bounds= data.total_bounds
    ax.set_extent([bounds[0], bounds[2], bounds[1], bounds[3]])
    ax.add_geometries(data.geometry, crs = crs.PlateCarree(), facecolor='',edgecolor='k',lw=1.2)

    drenaje = gpd.read_file(path_vector+"drenajes/%s/"%(codigo))
    ax.add_geometries(drenaje.geometry, crs = crs.PlateCarree(), facecolor='',edgecolor='k',lw=0.4)

    ax.set_title(title,fontsize= fontsize)
    ax.outline_patch.set_linewidth(0)

    cax, kw = matplotlib.colorbar.make_axes(ax, pad = 0.05, shrink = .7) # location = ubicacion_colorbar
    cbar = fig.colorbar(cs, cax = cax, **kw, )
    cbar.outline.set_visible(False)
    cbar.set_label(cbar_label, size = fontsize)
    cbar.ax.tick_params(labelsize = fontsize * .66)

    if rutafig_mapas is not None:
        pl.savefig(rutafig_mapas+window_t+'/%s.png'%codigo,bbox_inches='tight',dpi=95) 

def plot_radar_by_basin(windows_t, date, path_basins,path_radtif,path_vector,
                        path_hietograms,name_hietograms,
                        cbar=False,title=False,df_metadata=None,
                        rutafig_mapas=None,rutafig_hietogramas=None):
    
    for window_t in windows_t[:]:
        if window_t == '30m_ahead':
            start = date
            end = start + pd.Timedelta('30m')
        else:
            end = date
            start= end -  pd.Timedelta(window_t)

        if window_t == '30m_ahead':
            include_escenarios = 'extrapol'
        else:
            include_escenarios =  None
        
        #tif
        path_tif = path_radtif+'p_acum_%s_%s.tif'%(260,window_t)
        #generacion de lluvia
        print ('Window to run: %s ; include_escenarios = %s'%(window_t,include_escenarios))
        print ('Start rainfall generation: %s'%dt.datetime.now())
        df,rvec_accum,dfaccum = SHop.get_radar_rain_OP(start,end,300.,path_basins+'%s.nc'%(260),codigos,
                                                       accum=True,meanrain_ALL=False,
                                                       path_tif = path_tif,
                                                       include_escenarios = include_escenarios,
                                                       verbose=False)
        print ('End rainfall generation: %s'%dt.datetime.now())

        #mapa
        mapprop = wmf.read_map_raster(path_tif)
        #hietogramas
        df_p3h = pd.DataFrame.from_csv(path_hietograms+name_hietograms[0])
        df_p30m = pd.DataFrame.from_csv(path_hietograms+name_hietograms[1])

        #PLOTS

        print ('Start plotting: %s'%dt.datetime.now())

        for codigo in codigos[:]:
            nombreestacion = df_metadata['nombreestacion'].loc[codigo]   
            title = 'Est. %s | %s \n %s - %s'%(codigo,nombreestacion,
                                            pd.to_datetime(start).strftime('%Y-%m-%d %H:%M'),
                                            pd.to_datetime(end).strftime('%Y-%m-%d %H:%M'))


            plot_basin_rain(codigo, mapprop, path_vector, window_t=window_t,rutafig_mapas=rutafig_mapas,
                        fontsize=16,cbar_label = u'Precipitación acumulada [mm]',title=title)

            #HIETOGRAMASS
            #solo necesita el for de las estaciones, es indepenediente de la ventana por eso corre solo con la primera
            if window_t == '30m_ahead':

                fig = pl.figure(figsize=(6,4),dpi=100)
                ax = fig.add_subplot(111)

                #observed
                (df_p3h['%s'%codigo]*12).plot.area(ax=ax,alpha=0.5,color='darkcyan')
                #extrapolated
                (df_p30m['%s'%codigo]*12).plot.area(ax=ax,alpha=0.25,color='darkcyan',style=':')
                ax.set_xlabel('Tiempo',fontsize=13)
                ax.set_ylabel('Precipitación promedio \n en la cuenca (mm/h)',fontsize=14.5)
                #title
                ax.set_title(title)
                #ylim minimo 5mm
                if ax.get_ylim()[-1]< 5:
                    ax.set_ylim(0,5)
                else:
                    pass
                ax.axvline(x=df_p30m.index[0],ymax=10.,c='k')#,lw=1)
                ytext = (ax.get_ylim()[-1] - ax.get_ylim()[0]) *0.7
                ax.text(x=df_p30m.index[0]-pd.Timedelta('27m'),y=ytext, s=u'Extrapolación')

                if rutafig_hietogramas is not None:
                    pl.savefig(rutafig_hietogramas+'%s.png'%codigo,bbox_inches='tight',dpi=100)

    print ('End plotting: %s'%dt.datetime.now())
    print ('')
    
########################################################
# argumentos
########################################################


date = pd.to_datetime(dt.datetime.strftime(SHop.round_time(dt.datetime.now()), '%Y-%m-%d %H:%M')) #pd.to_datetime("2020-02-26 18:00:00")
print ('#####################date2run: %s'%(date))
windows_t= ['30m_ahead','3h']


#codigos con mascara
path_masks_shp = '/media/nicolas/Home/nicolas/01_SIATA/info_operacional_cuencas_nivel/red_nivel/vector/cuencas/'
codigos = np.sort(list(map(int,os.listdir(path_masks_shp))))

print (' ')
print (codigos)
print(' ')

#rutas mapas
path_basins= '/media/nicolas/Home/nicolas/01_SIATA/info_operacional_cuencas_nivel/red_nivel/nc_cuencas_py3/'
path_radtif = '/media/nicolas/Home/nicolas/01_SIATA/info_operacional_cuencas_nivel/red_nivel/radar_by_cuenca/'
path_vector = '/media/nicolas/Home/nicolas/01_SIATA/info_operacional_cuencas_nivel/red_nivel/vector/'
rutafig_mapas= '/media/nicolas/Home/Jupyter/Soraya/Op_Alarmas/Result_to_web/radar/acum_radar/'

# rutas hietogramas
path_hietograms = '/media/nicolas/Home/nicolas/01_SIATA/info_operacional_cuencas_nivel/red_nivel/radar_by_cuenca/'
name_hietograms = ['df_rad_acum3h.csv','df_rad_next30m.csv']
rutafig_hietogramas = '/media/nicolas/Home/Jupyter/Soraya/Op_Alarmas/Result_to_web/radar/hietogramas/'

#fig properties
# cbar=True
# title=True
fontsize  = 16

#consulta nombres de estaciones.
host = ''
user = ''
passw =''
dbname = ''
query = ("select codigo,nombreestacion from ****** where red = **** and codigo in %s;"%(list(map(str,codigos)))).replace('[','(')
query = query.replace(']',')')

conn_db = MySQLdb.connect(host,user,passw,dbname)
db_cursor = conn_db.cursor ()
db_cursor.execute (query)
data = db_cursor.fetchall()
conn_db.close()
fields = [field.lower() for field in list(np.array(db_cursor.description)[:,0])]
df_metadata = pd.DataFrame(np.array(data), columns = fields)
df_metadata.index = list(map(int,df_metadata['codigo']))

####################correr para todas las estaciones
plot_radar_by_basin(windows_t, date, path_basins,path_radtif,path_vector,
                    path_hietograms,name_hietograms,
                    df_metadata=df_metadata,
                    rutafig_mapas=rutafig_mapas,rutafig_hietogramas=rutafig_hietogramas)

print (dt.datetime.now())
