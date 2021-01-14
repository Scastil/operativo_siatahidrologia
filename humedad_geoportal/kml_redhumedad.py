#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import pandas as pd
import numpy as np
import os
import datetime as dt
import humedad as hm

######################################################################################################FUNCIONES

def write_kml_humedad(codes,df_metadata,path_figs,path_kml_format,path_kml,g0,g1,g2):
    blocks=[]
    for code in codes:
        date = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data_bla=df_metadata.loc[code][['nombreestacion','ciudad','latitude','longitude']]
        url_png='http://siata.gov.co/nicolasl/files/est_humedad.png'
        url_icon = 'http://www.siata.gov.co/iconos/humedad/humedad_naranja.png'

        if df_metadata.loc[code]['tipo_sensor'] == 1:
            urls_png=[path_figs+'3h/'+str(code)+'_T.png',
                      path_figs+'24h/'+str(code)+'_T.png',
                      path_figs+'72h/'+str(code)+'_T.png',
                      path_figs+'30d/'+str(code)+'_T.png',
                        path_figs+'3h/'+str(code)+'_EC.png',
                      path_figs+'24h/'+str(code)+'_EC.png',
                      path_figs+'72h/'+str(code)+'_EC.png',
                      path_figs+'30d/'+str(code)+'_EC.png',
                      path_figs+'3h/'+str(code)+'_VW.png',
                      path_figs+'24h/'+str(code)+'_VW.png',
                      path_figs+'72h/'+str(code)+'_VW.png',
                      path_figs+'30d/'+str(code)+'_VW.png'
                     ]
            listlol= [data_bla['nombreestacion'],code,data_bla['ciudad'],url_png,data_bla['latitude'],data_bla['longitude'],
                  date]+urls_png

            blocks.append( g1.format(listlol[0],listlol[1],listlol[2],listlol[3],listlol[4],listlol[5],listlol[6],listlol[7],listlol[8],
                     listlol[9],listlol[10],listlol[11],listlol[12],listlol[13],listlol[14],listlol[15],listlol[16],
                     listlol[17],listlol[18]))
        elif df_metadata.loc[code]['tipo_sensor'] == 2:
            urls_png=[path_figs+'3h/'+str(code)+'_T.png',
                      path_figs+'24h/'+str(code)+'_T.png',
                      path_figs+'72h/'+str(code)+'_T.png',
                      path_figs+'30d/'+str(code)+'_T.png',
                        path_figs+'3h/'+str(code)+'_EC.png',
                      path_figs+'24h/'+str(code)+'_EC.png',
                      path_figs+'72h/'+str(code)+'_EC.png',
                      path_figs+'30d/'+str(code)+'_EC.png',
                      path_figs+'3h/'+str(code)+'_VW.png',
                      path_figs+'24h/'+str(code)+'_VW.png',
                      path_figs+'72h/'+str(code)+'_VW.png',
                      path_figs+'30d/'+str(code)+'_VW.png'
                     ]
            listlol= [data_bla['nombreestacion'],code,data_bla['ciudad'],url_png,data_bla['latitude'],data_bla['longitude'],
                  date]+urls_png

            blocks.append( g2.format(listlol[0],listlol[1],listlol[2],listlol[3],listlol[4],listlol[5],listlol[6],listlol[7],listlol[8],
                     listlol[9],listlol[10],listlol[11],listlol[12],listlol[13],listlol[14],listlol[15],listlol[16],
                     listlol[17],listlol[18]))
        else:
            urls_png=[path_figs+'3h/'+str(code)+'_H.png',
                      path_figs+'24h/'+str(code)+'_H.png',
                      path_figs+'72h/'+str(code)+'_H.png',
                      path_figs+'30d/'+str(code)+'_H.png']
            listlol= [data_bla['nombreestacion'],code,data_bla['ciudad'],url_png,data_bla['latitude'],data_bla['longitude'],
                  date]+urls_png
            blocks.append( g0.format(listlol[0],listlol[1],listlol[2],listlol[3],listlol[4],listlol[5],listlol[6],listlol[7],listlol[8],
                     listlol[9],listlol[10]))

    #armar el kml
    codes_block = ['%s\n\n'%'%s' for i in codes]

    kml = open(path_kml_format,'r').read().format(''.join(codes_block))

    try:
        kml = kml%tuple(blocks)
    except:
        kml = kml%tuple(list(blocks))

    f = open(path_kml,'w')
    f.write(kml)
    f.close()
    print ('.kml generated.')

###################################################################################################ARGUMENTOS

host = '192.168.1.74'
user = 'siata_Oper'
passw = 'si@t@64512_operacional'
dbname = 'siata'

# host   = '192.168.1.100'
# user   = 'siata_Consulta'
# passwd = 'si@t@64512_C0nsult4'
# dbname = 'siata'

query = 'describe estaciones'
query = 'select codigo, nombreestacion,ciudad,latitude,longitude,estado,hd,red from estaciones where red in ("humedad","humedad_stevens","humedad_laderas_5te_rasp","humedad_stevens_laderas_rasp") and estado in ("A","P")'
df_metadata = hm.query_mysql(host,user,passw,dbname,query)

df_metadata = df_metadata.set_index('codigo')
df_metadata.index = list(map(int,df_metadata.index))
df_metadata.columns = ['nombreestacion', 'ciudad','latitude', 'longitude', 'estado', 'tipo_sensor', 'red']

#ASIGNACIONES
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

g0 = '''<Placemark>
<styleUrl>#testIcon197</styleUrl>
<name>{0}</name>
<ExtendedData><SchemaData schemaUrl="#Humedad_new">
    <SimpleData name="Codigo">{1}</SimpleData>
    <SimpleData name="Municipio">{2}</SimpleData>
    <SimpleData name="url_imagen">{3}</SimpleData>
    <SimpleData name="Latitud">{4}</SimpleData>
    <SimpleData name="Longitud">{5}</SimpleData>
    <SimpleData name="icon">http://www.siata.gov.co/iconos/humedad/humedad_naranja.png</SimpleData>
    <SimpleData name="info">Los sensores de humedad del suelo miden el contenido volumétrico de agua en el suelo (CAS), es decir, la proporción [%] entre el volumen de agua y el volumen total de una esfera de suelo (alcance del sensor). Cuantificar la humedad en el suelo permite entender el estado de saturación de las laderas y su respuesta hidrológica ante eventos de precipitación. Referencia del sensor: 5TE, Decagon Devices.</SimpleData>
    <SimpleData name="G_3_H"></SimpleData>
    <SimpleData name="G_24_H"></SimpleData>
    <SimpleData name="G_72_H"></SimpleData>
    <SimpleData name="G_30_D"></SimpleData>
    <SimpleData name="G_3_H"></SimpleData>
    <SimpleData name="G_24_H"></SimpleData>
    <SimpleData name="G_72_H"></SimpleData>
    <SimpleData name="G_30_D"></SimpleData>
    <SimpleData name="G_3_H_ConVolAgua">{7}</SimpleData>
    <SimpleData name="G_24_H_ConVolAgua">{8}</SimpleData>
    <SimpleData name="G_72_H_ConVolAgua">{9}</SimpleData>
    <SimpleData name="G_30_D_ConVolAgua">{10}</SimpleData>
</SchemaData></ExtendedData>
    <Point><coordinates>{5},{4}</coordinates></Point>
</Placemark>
'''
g1 = '''<Placemark>
<styleUrl>#testIcon197</styleUrl>
<name>{0}</name>
<ExtendedData><SchemaData schemaUrl="#Humedad_new">
    <SimpleData name="Codigo">{1}</SimpleData>
    <SimpleData name="Municipio">{2}</SimpleData>
    <SimpleData name="url_imagen">{3}</SimpleData>
    <SimpleData name="Latitud">{4}</SimpleData>
    <SimpleData name="Longitud">{5}</SimpleData>
    <SimpleData name="icon">http://www.siata.gov.co/iconos/humedad/humedad_naranja.png</SimpleData>
    <SimpleData name="info">Los sensores de humedad del suelo miden el contenido volumétrico de agua en el suelo (CVA), es decir, la proporción [%] entre el volumen de agua y el volumen total de una esfera de suelo (alcance del sensor). Cuantificar la humedad en el suelo permite entender el estado de saturación de las laderas y su respuesta hidrológica ante eventos de precipitación. Referencia del sensor: 5TE, Decagon Devices.</SimpleData>
    <SimpleData name="G_3_H"></SimpleData>
    <SimpleData name="G_24_H"></SimpleData>
    <SimpleData name="G_72_H"></SimpleData>
    <SimpleData name="G_30_D"></SimpleData>
    <SimpleData name="G_3_H"></SimpleData>
    <SimpleData name="G_24_H"></SimpleData>
    <SimpleData name="G_72_H"></SimpleData>
    <SimpleData name="G_30_D"></SimpleData>
    <SimpleData name="G_3_H_ConVolAgua">{7}</SimpleData>
    <SimpleData name="G_24_H_ConVolAgua">{8}</SimpleData>
    <SimpleData name="G_72_H_ConVolAgua">{9}</SimpleData>
    <SimpleData name="G_30_D_ConVolAgua">{10}</SimpleData>
</SchemaData></ExtendedData>
    <Point><coordinates>{5},{4}</coordinates></Point>
</Placemark>
'''

g2 = '''<Placemark>
<styleUrl>#testIcon197</styleUrl>
<name>{0}</name>
<ExtendedData><SchemaData schemaUrl="#Humedad_new">
    <SimpleData name="Codigo">{1}</SimpleData>
    <SimpleData name="Municipio">{2}</SimpleData>
    <SimpleData name="url_imagen">{3}</SimpleData>
    <SimpleData name="Latitud">{4}</SimpleData>
    <SimpleData name="Longitud">{5}</SimpleData>
    <SimpleData name="icon">http://www.siata.gov.co/iconos/humedad/humedad_naranja.png</SimpleData>
    <SimpleData name="info">Los sensores de humedad del suelo miden el contenido volumétrico de agua en el suelo (CVA), es decir, la proporción [%] entre el volumen de agua y el volumen total de una esfera de suelo (alcance del sensor). Cuantificar la humedad en el suelo permite entender el estado de saturación de las laderas y su respuesta hidrológica ante eventos de precipitación. Referencia del sensor: Hydraprobe, Stevens Water.</SimpleData>
    <SimpleData name="G_3_H"></SimpleData>
    <SimpleData name="G_24_H"></SimpleData>
    <SimpleData name="G_72_H"></SimpleData>
    <SimpleData name="G_30_D"></SimpleData>
    <SimpleData name="G_3_H"></SimpleData>
    <SimpleData name="G_24_H"></SimpleData>
    <SimpleData name="G_72_H"></SimpleData>
    <SimpleData name="G_30_D"></SimpleData>
    <SimpleData name="G_3_H_ConVolAgua">{7}</SimpleData>
    <SimpleData name="G_24_H_ConVolAgua">{8}</SimpleData>
    <SimpleData name="G_72_H_ConVolAgua">{9}</SimpleData>
    <SimpleData name="G_30_D_ConVolAgua">{10}</SimpleData>
</SchemaData></ExtendedData>
    <Point><coordinates>{5},{4}</coordinates></Point>
</Placemark>
'''
blocks = []
codes = df_metadata.estado[df_metadata.estado == 'A'].index
print (df_metadata.estado)
path_figs = 'http://siata.gov.co/nicolasl/figs_operacionales/humedad/'
path_kml_format = '/media/nicolas/Home/Jupyter/Soraya/git/Alarmas/06_Crones/files/Humedad_kmlformat.kml'
path_kml = '/media/nicolas/Home/Jupyter/Soraya/git/Alarmas/06_Crones/files/humedaddelsuelo.kml'

######################################################################################################EJECUCION
write_kml_humedad(codes,df_metadata,path_figs,path_kml_format,path_kml,g0,g1,g2)

res = os.system('rsync -r -v -a -z -e ssh %s hidrologia@192.168.1.74:/var/www/kml/01_Redes'%(path_kml))
if res == 0:
    print ('Se copia .kml en hidrologia@192.168.1.74:/var/www/kml/01_Redes/')
else:
    print ('No se copia .kml en hidrologia@192.168.1.74:/var/www/kml/01_Redes/')