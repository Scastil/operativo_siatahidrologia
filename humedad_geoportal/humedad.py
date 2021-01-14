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
matplotlib.rcParams['font.size'] = 11

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
import warnings
warnings.filterwarnings('ignore')

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
    date
    time object rounded, datetime object
    '''    
    dif = date.minute % round_mins

    if dif <= round_mins/2:
        return dt.datetime(date.year, date.month, date.day, date.hour, date.minute - (date.minute % round_mins))
    else:
        return dt.datetime(date.year, date.month, date.day, date.hour, date.minute - (date.minute % round_mins)) + dt.timedelta(minutes=round_mins)

    
def query_mysql(host,user,passw,dbname,query,verbose=False):
    conn_db = MySQLdb.connect(host,user,passw,dbname)
    db_cursor = conn_db.cursor ()
    db_cursor.execute (query)
    data = db_cursor.fetchall()
    conn_db.close()
    fields = [field.lower() for field in list(np.array(db_cursor.description)[:,0])]
    if len(data) == 0:
        df_query = pd.DataFrame(columns = fields)
        if verbose:
            print ('empty query')
    else:
        df_query = pd.DataFrame(np.array(data), columns = fields)

    return df_query

def query_2_dfseries(start,end,freq,host,user,passw,dbname,query):
    df_query = query_mysql(host,user,passw,dbname,query,verbose=True)

    #se encarga de index y de cuando no hay datos.
    if df_query.shape[0] == 0:
        rng = pd.date_range(start,end,freq=freq)
        df = pd.DataFrame(index = rng, columns=df_query.columns)
        df = df.drop(['fecha_hora'],axis=1)
        df.index.name=''
    else:
        df = df_query.set_index('fecha_hora')
        df=df.apply(pd.to_numeric)
        df.index.name=''
        rng=pd.date_range(start,end,freq=freq)
        df=df.reindex(rng)
    return df

# QUERY 
def query_humedad(codeH,freq,path_dfh,host,user,passw,dbname,df_metadata,update_or_save_csv=0,start=None,end=None,calidad=False):    
    if update_or_save_csv == 0:
        #abre csv para saber hasta donde actualizar
        df_old = pd.DataFrame.from_csv(path_dfh+'%s_30d.csv'%codeH)
        #se redefine start y end con los df que se guardan y actualizan
        start = df_old.index[-1]
        end = round_time(dt.datetime.now())
        #si el csv tiene mas de 30d sin actualizarse toma ult. 30d
        actual_timedelta = round_time(dt.datetime.now()) - df_old.index[-1]
        if actual_timedelta>pd.Timedelta('30d'): 
            start = round_time(dt.datetime.now()-pd.Timedelta('30d'))
            end = round_time(dt.datetime.now())
    else:
        #si no hay 30d.csv tons se consulta nuevo.
        start = round_time(dt.datetime.now()-pd.Timedelta('30d'))
        end = round_time(dt.datetime.now())

    #CONSULTA
    #query humedad de acuerdo al tipo de sensor y tabla donde guarda datos.
    if df_metadata.tipo_sensor.loc[codeH] == 1:
        query = 'select fecha_hora, h1, h2, h3,calidad from humedad_rasp where cliente = "%s" and fecha_hora between "%s" and "%s";'%(codeH,start,end)
        df_query = query_2_dfseries(start,end,freq,host,user,passw,dbname,query)

    elif df_metadata.tipo_sensor.loc[codeH] == 2:
        if df_metadata.red.loc[codeH] == 'humedad_laderas_5te_rasp':
            query = 'select fecha_hora, vw1,vw2,vw3,t1,t2,t3,c1,c2,c3,calidad from humedad_laderas_5te_rasp where cliente = "%s" and fecha_hora between "%s" and "%s";'%(codeH,start,end)
        else:
            query = 'select fecha_hora, vw1,vw2,vw3,t1,t2,t3,c1,c2,c3,calidad from humedad_rasp where cliente = "%s" and fecha_hora between "%s" and "%s";'%(codeH,start,end)
        df_query = query_2_dfseries(start,end,freq,host,user,passw,dbname,query)

    else:
        if df_metadata.red.loc[codeH] == 'humedad_stevens_laderas_rasp':
            query = 'select fecha_hora, sh1,sh2,sh3,stc1,stc2,stc3,sc1,sc2,sc3,calidad from humedad_stevens_laderas_rasp where cliente = "%s" and fecha_hora between "%s" and "%s";'%(codeH,start,end)
        else:
            query = 'select fecha_hora, sh1,sh2,sh3,stc1,stc2,stc3,sc1,sc2,sc3,calidad from humedad_stevens_rasp where cliente = "%s" and fecha_hora between "%s" and "%s";'%(codeH,start,end)

        df_query = query_2_dfseries(start,end,freq,host,user,passw,dbname,query)
        df_query[df_query.columns[:3]] = df_query[df_query.columns[:3]]*100.
    #filtro calidad inicial
    df_query[df_query<=0] = np.nan
    df_query[df_query>120.] = np.nan

    # print  (dt.datetime.now()) 

    #query pluvio
    query = "select fecha,hora,p1/1000,p2/1000 from datos where cliente='%s' and (((fecha>'%s') or (fecha='%s' and hora>='%s')) and ((fecha<'%s') or (fecha='%s' and hora<='%s')))"%(df_metadata.p_asociado.loc[codeH],start.strftime('%Y-%m-%d'),start.strftime('%Y-%m-%d'),start.strftime('%H:%M:%S'),end.strftime('%Y-%m-%d'),end.strftime('%Y-%m-%d'),end.strftime('%H:%M:%S'))
    P = query_mysql(host,user,passw,dbname,query)

    P.columns=['fecha','hora','p1','p2']
    nulls = np.where(P[['fecha']]['fecha'].isnull() == True)[0]
    P= P.drop(nulls)
    dates = [P['fecha'][i].strftime('%Y-%m-%d') +' '+str(P['hora'][i]).split(' ')[-1][:-3] for i in P.index]
    P.index = pd.to_datetime(dates)
    P['fecha_hora']= dates
    P = P.drop_duplicates(['fecha_hora'],keep='first').asfreq('1T')
    P = P.drop(['fecha','hora','fecha_hora'],axis=1)
    rng=pd.date_range(start,end,freq=freq)
    P=P.reindex(rng)

    #agregar en dfsoil.
    df_query[['p1','p2']] = P[['p1','p2']]

    #CALIDAD: aplica solo para las primeras 3 (humedad) y ultimas 2 filas (pluvios)
    #si todo es nan porque no hay datos, pass
    if np.isnan(df_query.calidad.mean()):
        pass
    elif calidad == True:
        if update_or_save_csv==1:
            print  (dt.datetime.now())
            print (u'calidá')
        #valores de calidad distintos a nan
        cal_values = np.array(list(map(int,np.unique(df_query.calidad)[np.where(np.isnan(np.unique(df_query.calidad))==False)[0]])))
        pos_badcal =  np.where((cal_values != 1)&(cal_values != 2))[0]
        #si hay calidad diferente a 1 y 2, se ejecuta el pedazo de calidad, si no, no.
        if pos_badcal.size != 0:
            #para cada valor de calidad dudosa.
            for val in cal_values[pos_badcal]:
                #fecha a las que aplica cada codigo de calidad dudosa
                dates = df_query[df_query.calidad == val].index
                #dependiendo de cuales sean se hacen nan los valoresde humedad, cuando los malos son solo p1,p2 no se hace nada, por ahora.
                #se demora mil annos porque toca hacer nan valor a valor de cada date y cada columns.
                if val == 152:
                    for date in dates:
                        df_query[df_query.columns[0]].loc[date]=np.nan
                        df_query[df_query.columns[1]].loc[date]=np.nan
                        df_query[df_query.columns[2]].loc[date]=np.nan
                        df_query[df_query.columns[-2]].loc[date]=np.nan
                        df_query[df_query.columns[-1]].loc[date]=np.nan
                if val == 1520:
                    for date in dates:
                        df_query[df_query.columns[0]].loc[date]=np.nan
                        df_query[df_query.columns[1]].loc[date]=np.nan
                        df_query[df_query.columns[2]].loc[date]=np.nan
                if val == 1521:
                    for date in dates:
                        df_query[df_query.columns[0]].loc[date]=np.nan
                if val == 1522:
                    for date in dates:
                        df_query[df_query.columns[1]].loc[date]=np.nan
                if val == 1523:
                    for date in dates:
                        df_query[df_query.columns[2]].loc[date]=np.nan
                if val == 1526:
                    for date in dates:
                        df_query[df_query.columns[0]].loc[date]=np.nan
                        df_query[df_query.columns[1]].loc[date]=np.nan
                if val == 1527:
                    for date in dates:
                        df_query[df_query.columns[0]].loc[date]=np.nan
                        df_query[df_query.columns[2]].loc[date]=np.nan
                if val == 1528:
                    for date in dates:
                        df_query[df_query.columns[1]].loc[date]=np.nan
                        df_query[df_query.columns[2]].loc[date]=np.nan
                        
        if update_or_save_csv==1:
            print  (dt.datetime.now())

    if update_or_save_csv == 0:
        #ACTUALIZAR ARCHIVOS OP.
        df_old=df_old.append(df_query)
        #borra index repetidos, si los hay - la idea es que no haya pero si los hay no funciona el df.reindex
        df_old[df_old.index.duplicated(keep='last')]=np.NaN
        df_old = df_old.dropna(how='all')
        #reindex
        #si no hay datos, deja los nan.
        if df_old[df_old.columns[:-2]].mean().isnull().all() == True:
            rng=pd.date_range(start,end,freq=freq)
        else:
            rng=pd.date_range(df_old.index[0],end,freq=freq)
        
        df_old=df_old.reindex(rng)
        #Guarda el archivo actualizado,pero se descabeza 5min para no aumentar el tamano del .csv op.
        df_old[end-pd.Timedelta('30d'):end].to_csv(path_dfh+'%s_30d.csv'%codeH)
        return df_old[end-pd.Timedelta('30d'):end]
    else:
        df_query.to_csv(path_dfh+'%s_30d.csv'%codeH)
        return df_query
    
def grafica_humedad(codeH,dfax,dfax2,ylabelax,ylabelax2,xlabel,fontsizeylabel,fontsizexlabel,
                         colors,window,tiposensor,ylocfactor_texts,yloc_legends,df_metadata,rutafig=None,
                         loc2legend=None,title=None):
    '''
    Make a df.plot with parallel axisa and the SIATA format, It's useful for plotting rainfall.
    Can be setted for plotting .png's ready for SIATA webpage.

    loc2legend: If None returns a plot for webpage, if not None have to be the location (x,y) for ax.legend

    --------
    Returns:
    - Plot

    '''
    
    #bottom text
    #escoger la columna con menos nan (antes de calidad) para sacar el porentaje de datos recuperados en tiemporeal
    #estimar el porcentaje de datos recuperados a partir de eso.
    df_isnan =  dfax.applymap(lambda x: 0 if np.isnan(x) == True else 1)
    perc_datos= round((df_isnan.sum().max()/float(df_isnan.shape[0]))*100.,2)
    #se escoge las profundidaes 2show
    pos2show = np.array(list(map(int,[key.split(' ')[2] for key in dfax.columns])))-1
    depths_names = np.array(['0.1','0.5','0.9'])
    #y los colores que corresponden
    colors = colors[pos2show]
    if df_metadata.tipo_sensor.loc[codeH] == 1:
        bottomtext= u'Esta estación tiene sensores a {} m de profundidad. \nCada uno mide el Contenido Volumétrico de Agua (CVA) en el \nsuelo. También cuenta con una estación pluviométrica \nasociada.\n \nTipo de Sensores: {}\nResolución Temporal: 1 min.\nPorcentaje de datos transmitidos*: {}% \n *Calidad de datos aún sin verificar exhaustivamente.'.format(', '.join(depths_names[pos2show]),tiposensor,perc_datos) 
    else:
        #si hay valores en la lista diferentes de nan
        if type(df_metadata.depths_laderas.loc[codeH]) == list:
            depths_names = np.array(df_metadata.depths_laderas.loc[codeH])
            bottomtext= u'Esta estación tiene sensores a {} m de profundidad. \nCada uno mide Contenido Volumétrico de Agua (CVA), Tem-\nperatura (T) y Conductividad Eléctrica (CE) en el suelo. También \ncuenta con una estación pluviométrica asociada.\n \nTipo de Sensores: {}\nResolución Temporal: 1 min.\nPorcentaje de datos transmitidos*: {}% \n *Calidad de datos aún sin verificar exhaustivamente.'.format(', '.join(depths_names[pos2show]),tiposensor,perc_datos)
        else:
            bottomtext= u'Esta estación tiene sensores a {} m de profundidad. \nCada uno mide Contenido Volumétrico de Agua (CVA), Tem-\nperatura (T) y Conductividad Eléctrica (CE) en el suelo. También \ncuenta con una estación pluviométrica asociada.\n \nTipo de Sensores: {}\nResolución Temporal: 1 min.\nPorcentaje de datos transmitidos*: {}% \n *Calidad de datos aún sin verificar exhaustivamente.'.format(', '.join(depths_names[pos2show]),tiposensor,perc_datos)
    
    
    #figure
    fig  =pl.figure(dpi=97.5,facecolor='w')
    ax = fig.add_subplot(111)
    dfax.plot(ax=ax,lw=1.85,color=colors)
    ax.set_ylabel(ylabelax,fontsize=fontsizeylabel)
    ax.set_xlabel(xlabel,fontsize=fontsizexlabel)
    #number of yticks
    pl.locator_params(axis='y', nbins=5)
    if title is not None:
        ax.set_title(title,fontsize=fontsizexlabel)
    #second axis
    axAX=pl.gca()
    ax2=ax.twinx()
    ax2AX=pl.gca()
    dfax2.plot(ax=ax2,alpha=0.5,color=['#4392d6','#70AFBA'])
    
    ax2.set_ylim(0,ax2AX.get_ylim()[1]*4.5)
    ax2AX.set_ylim(ax2AX.get_ylim() [::-1]) 
    ax2.set_ylabel(ylabelax2,fontsize=fontsizeylabel)

    if loc2legend is None: 
        #setting loc's
        if window == '3h':
            yloc_legend = yloc_legends[0]
            ylocfactor_text=ylocfactor_texts[0]
        elif window == '24h' or window == '72h':
            yloc_legend = yloc_legends[1]
            ylocfactor_text=ylocfactor_texts[1]
        else:
            yloc_legend = yloc_legends[2]
            ylocfactor_text = ylocfactor_texts[2]
        #legend
        #xloc by default
        xloc_legend1 = 0.219; xloc_legend2 = 0.37
        #xloc if there is only one sensor.
        if dfax.shape[1] == 1:
            xloc_legend1 = 0.3875
        ax.legend(loc=(xloc_legend1,yloc_legend),ncol=2,fontsize=12)
        ax2.legend(loc=(xloc_legend2,yloc_legend*1.23),ncol=2,fontsize=12)
        #se ubica el text, x e y que se ajustan de acuerdo a los dominios de x e y
        ax.text(dfax.index[int(dfax.shape[0]*0.049)], ax.get_ylim()[0]-(1*(ax.get_ylim()[1]-ax.get_ylim()[0])*ylocfactor_text),
                bottomtext, fontsize=11.37,
                bbox=dict(edgecolor='#c1c1c1',facecolor='w'))
    else:
        ax.legend(loc=loc2legend,ncol=2)
    #day label for 3h window plot
    if window == '3h':
        pl.draw()
        labels = [item.get_text() for item in ax.get_xticklabels()]
        labels_len = [len(i) for i in labels]
        if 12 in labels_len: #si ya hay uno con day-month label
            print('it has label already')
            pass
        elif np.mean(labels_len)==5.: #si todos los ticks tienen text y si ninguno es >5
            labels[0]= '%s\n%s'%(labels[0],dfax2.index[-1].strftime('%d-%b'))
        elif labels_len[0] == 0: #si el primero es vacio, ponerle day-month label al sgte.
            labels[1]= '%s\n%s'%(labels[1],dfax2.index[-1].strftime('%d-%b'))
        ax.set_xticklabels(labels)
    #number of yticks
    pl.locator_params(axis='y', nbins=5)
    if rutafig is not None:
        pl.savefig(rutafig,bbox_inches='tight',dpi=97.5,facecolor='w')

def grafica_humedad2geoportal(codeH,freq,path_dfh,host,user,passw,dbname,df_metadata,
                         ruta_figs=None,calidad=False):#,rutacredentials_remote,rutacredentials_local)
    '''
    Execute the operational plots of the SIATA soil moisture network within the official webpage format,
    colors, legends, time windows, etc. Use plot_HydrologicalVar() functions for plotting.

    Returns
    ---------
    No returns, save the plots is if settled.
    '''

    # Consulta SAL - Humedad
    soilm_df= query_humedad(codeH,freq,path_dfh,host,user,passw,dbname,df_metadata,update_or_save_csv=0,calidad=calidad)
    
    if df_metadata.tipo_sensor.loc[codeH] == 1:

        #plot arguments
        dfax2=soilm_df[soilm_df.columns[-2:]]
        tiposensor='Análogos MAS-1'
        yloc_legends=[-0.4,-0.421,-0.48]
        ylocfactor_texts=[0.97,1,1.07] 
        title='Est. %s | %s'%(df_metadata.loc[codeH].name,df_metadata.nombreestacion.loc[codeH])
        ylabelax='Cont. Volumétrico de  Agua $(\%)$'
        ylabelax2='Precipitación  ($mm$)'
        xlabel='Tiempo'
        fontsizeylabel=13
        fontsizexlabel=16
        loc2legend=None
        colors= np.array(['#3CB371','#008d8d','#09202E'])

        #cant. de datos qe llegan sobre los esperados

        #profundidades a mostrar
        soilm_df=soilm_df[soilm_df.columns[:3]]
        soilm_df.columns=['Sensor CVA 1','Sensor CVA 2','Sensor CVA 3']
        depths = np.array(list(map(int,[key.split(' ')[2] for key in soilm_df.columns])))
        if np.isnan(df_metadata.depths2drop.loc[codeH]) == False:
            pos_depthsout=[]
            for depth in df_metadata.depths2drop.loc[codeH]:
                pos_depthsout.append(np.where(depths == depth)[0] )
            pos_depthsout = np.sort(np.concatenate(pos_depthsout))

            # redef del df sin detphs out.
            soilm_df = soilm_df.drop(soilm_df.columns[pos_depthsout],axis=1)
            dfax=soilm_df.copy()      
        else:
            dfax=soilm_df.copy() 

        #grafica
        for window in ['3h','24h','72h','30d']:
            if ruta_figs is not None:
                rutafig='%s%s/%s_H.png'%(ruta_figs,window,codeH)
                grafica_humedad(codeH,dfax.loc[dfax.index[-1]-pd.Timedelta(window):dfax.index[-1]],
                                 dfax2.loc[dfax.index[-1]-pd.Timedelta(window):dfax.index[-1]],
                                 ylabelax,ylabelax2,xlabel,fontsizeylabel,fontsizexlabel,
                                 colors,window,tiposensor,ylocfactor_texts,yloc_legends,
                                 df_metadata,
                                 title=title,rutafig=rutafig)
            else:
                grafica_humedad(codeH,dfax.loc[dfax.index[-1]-pd.Timedelta(window):dfax.index[-1]],
                                     dfax2.loc[dfax.index[-1]-pd.Timedelta(window):dfax.index[-1]],
                                     ylabelax,ylabelax2,xlabel,fontsizeylabel,fontsizexlabel,
                                     colors,window,tiposensor,ylocfactor_texts,yloc_legends,
                                     df_metadata,
                                     title=title)

    # Se escoge info y graficas de acuerdo al tipo de sensor.
    elif df_metadata.tipo_sensor.loc[codeH] == 2:
        tiposensor='Digitales - 5TE'
        #plot arguments
        dfax2=soilm_df[soilm_df.columns[-2:]]
        yloc_legends=[-0.4,-0.421,-0.48]
        ylocfactor_texts=[0.97,1,1.07] 
    #         yloc_legends=[-0.35,-0.39,-0.46]
    #         ylocfactor_texts=[0.97,1.02,1.10] 
        title='Est. %s | %s'%(df_metadata.loc[codeH].name,df_metadata.nombreestacion.loc[codeH])
        ylabelaxs=['Conductividad Eléctrica   $(dS.m^{-1})$', u'Temperatura ($^\circ$C)','Cont. Volumétrico de  Agua $(\%)$']
        ylabelax2='Precipitación  ($mm$)'
        xlabel='Tiempo'
        fontsizeylabel=13
        fontsizexlabel=16
        loc2legend=None
        colors= np.array(['#3CB371','#008d8d','#09202E'])

        #profundidades a mostrar
        soilm_df=soilm_df[soilm_df.columns[:9]]
        soilm_df.columns=['Sensor CVA 1','Sensor CVA 2','Sensor CVA 3','Sensor T 1','Sensor T 2','Sensor T 3','Sensor CE 1','Sensor CE 2','Sensor CE 3']
        depths = np.array(list(map(int,[key.split(' ')[2] for key in soilm_df.columns])))
        #si no hay valores nan dentro de la lista depths2drop: hay que saltarse algun sensor
        if np.isnan(np.mean(df_metadata.depths2drop.loc[codeH])) == False:
            pos_depthsout=[]
            for depth in df_metadata.depths2drop.loc[codeH]:
                pos_depthsout.append(np.where(depths == depth)[0] )
            pos_depthsout = np.sort(np.concatenate(pos_depthsout))

            # redef del df sin detphs out.
            soilm_df = soilm_df.drop(soilm_df.columns[pos_depthsout],axis=1)
            dfax=soilm_df.copy()      
        else:
            dfax=soilm_df.copy() 

        # definicion de combos de columnas por variable medida por el sensor.
        var_s = np.array([key.split(' ')[1] for key in soilm_df.columns])
        dfaxs = []
        dfaxs.append(soilm_df[soilm_df.columns[np.where(var_s=='CE')]])
        dfaxs.append(soilm_df[soilm_df.columns[np.where(var_s=='T')]])
        dfaxs.append(soilm_df[soilm_df.columns[np.where(var_s=='CVA')]])

        #grafica
        for window in ['3h','24h','72h','30d'][:]:
            namesfig=['EC','T','VW']
            if ruta_figs is not None:
                rutafig='%s%s/%s'%(ruta_figs,window,codeH)
                rutafigs= [rutafig+'_'+namefig+'.png' for namefig in namesfig]
                for index,dfax in enumerate(dfaxs):
                    grafica_humedad(codeH,dfax.loc[dfax.index[-1]-pd.Timedelta(window):dfax.index[-1]],
                                     dfax2.loc[dfax.index[-1]-pd.Timedelta(window):dfax.index[-1]],
                                     ylabelaxs[index],ylabelax2,xlabel,fontsizeylabel,fontsizexlabel,
                                     colors,window,tiposensor,ylocfactor_texts,yloc_legends,
                                     df_metadata,
                                     title=title,rutafig=rutafigs[index])
            else:
                for index,dfax in enumerate(dfaxs):
                    grafica_humedad(codeH,dfax.loc[dfax.index[-1]-pd.Timedelta(window):dfax.index[-1]],
                                     dfax2.loc[dfax.index[-1]-pd.Timedelta(window):dfax.index[-1]],
                                     ylabelaxs[index],ylabelax2,xlabel,fontsizeylabel,fontsizexlabel,
                                     colors,window,tiposensor,ylocfactor_texts,yloc_legends,
                                     df_metadata,
                                     title=title,rutafig=rutafigs[index])
    #STEVENS
    elif df_metadata.tipo_sensor.loc[codeH] == 3:
        tiposensor='Digitales - Stevens'
        #plot arguments
        dfax2=soilm_df[soilm_df.columns[-2:]]
        yloc_legends=[-0.4,-0.421,-0.48]
        ylocfactor_texts=[0.97,1,1.07] 
    #         yloc_legends=[-0.35,-0.39,-0.46]
    #         ylocfactor_texts=[0.97,1.02,1.10] 
        title='Est. %s | %s'%(df_metadata.loc[codeH].name,df_metadata.nombreestacion.loc[codeH])
        ylabelaxs=['Conductividad Eléctrica   $(dS.m^{-1})$', u'Temperatura ($^\circ$C)','Cont. Volumétrico de  Agua $(\%)$']
        ylabelax2='Precipitación  ($mm$)'
        xlabel='Tiempo'
        fontsizeylabel=13
        fontsizexlabel=16
        loc2legend=None
        colors= np.array(['#3CB371','#008d8d','#09202E'])

        #profundidades a mostrar
        soilm_df=soilm_df[soilm_df.columns[:9]]
        soilm_df.columns=['Sensor CVA 1','Sensor CVA 2','Sensor CVA 3','Sensor T 1','Sensor T 2','Sensor T 3','Sensor CE 1','Sensor CE 2','Sensor CE 3']
        depths = np.array(list(map(int,[key.split(' ')[2] for key in soilm_df.columns])))
        #si no hay valores nan dentro de la lista depths2drop: hay que saltarse algun sensor
        if np.isnan(np.mean(df_metadata.depths2drop.loc[codeH])) == False:
            pos_depthsout=[]
            for depth in df_metadata.depths2drop.loc[codeH]:
                pos_depthsout.append(np.where(depths == depth)[0] )
            pos_depthsout = np.sort(np.concatenate(pos_depthsout))

            # redef del df sin detphs out.
            soilm_df = soilm_df.drop(soilm_df.columns[pos_depthsout],axis=1)
            dfax=soilm_df.copy()      
        else:
            dfax=soilm_df.copy() 

        # definicion de combos de columnas por variable medida por el sensor.
        var_s = np.array([key.split(' ')[1] for key in soilm_df.columns])
        dfaxs = []
        dfaxs.append(soilm_df[soilm_df.columns[np.where(var_s=='CE')]])
        dfaxs.append(soilm_df[soilm_df.columns[np.where(var_s=='T')]])
        dfaxs.append(soilm_df[soilm_df.columns[np.where(var_s=='CVA')]])

        #grafica
        for window in ['3h','24h','72h','30d'][:]:
            namesfig=['EC','T','VW']
            if ruta_figs is not None:
                rutafig='%s%s/%s'%(ruta_figs,window,codeH)
                rutafigs= [rutafig+'_'+namefig+'.png' for namefig in namesfig]
                for index,dfax in enumerate(dfaxs):
                    grafica_humedad(codeH,dfax.loc[dfax.index[-1]-pd.Timedelta(window):dfax.index[-1]],
                                     dfax2.loc[dfax.index[-1]-pd.Timedelta(window):dfax.index[-1]],
                                     ylabelaxs[index],ylabelax2,xlabel,fontsizeylabel,fontsizexlabel,
                                     colors,window,tiposensor,ylocfactor_texts,yloc_legends,
                                     df_metadata,
                                     title=title,rutafig=rutafigs[index])
            else:
                for index,dfax in enumerate(dfaxs):
                    grafica_humedad(codeH,dfax.loc[dfax.index[-1]-pd.Timedelta(window):dfax.index[-1]],
                                     dfax2.loc[dfax.index[-1]-pd.Timedelta(window):dfax.index[-1]],
                                     ylabelaxs[index],ylabelax2,xlabel,fontsizeylabel,fontsizexlabel,
                                     colors,window,tiposensor,ylocfactor_texts,yloc_legends,
                                     df_metadata,
                                     title=title,rutafig=rutafigs[index])