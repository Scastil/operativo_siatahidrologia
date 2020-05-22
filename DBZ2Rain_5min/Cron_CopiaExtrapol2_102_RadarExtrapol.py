#!/usr/bin/env python
import numpy as np
import os 
import datetime as dt
import pandas as pd
import glob
import smtplib


horaInicio = dt.datetime.now()

def rad_extrapol():
    #||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    #||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    #||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    #||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    print ('############################### DEFINE RUTAS #########################\n')

    #---------------------------------------
    #Rutas de trabajo
    rutaRadar = '/media/nicolas/Home/nicolas/101_RadarClass/'
    rutaExtrapol = '/media/extrapol/'
    rutaCopia = '/media/nicolas/Home/nicolas/102_RadarExtrapol/'
    rutaAqui = '/media/nicolas/Home/Jupyter/Soraya/git/Alarmas/06_Crones/radar_otroformato/'
    #Borra los datos que se tengan en rutaCopia
    try:
            os.system('rm '+rutaCopia+'*.bin')
            print ('Aviso: Rutas definidas y archivos viejos borrados.\n')
    except:
            print ('Aviso: Rutas definidias, no hay archivos viejos. \n')

    #||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    #||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    #||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    #||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    print ('############################## COPIA ARCHIVOS NUEVOS DE EXTRAPOLACION #####################\n')

    # Busca ultima fecha y de acuerdo a eso obtiene quien (de los ultimos 5 nc en rutaRadar) es la ultima imagen de radar 
    # que debe tener extrapolacion, porque no necesariamente /extrapol y rutaRadar y rutaExtrapol
    # estan sincronizados.
    Ahora = (dt.datetime.now() + pd.Timedelta('5 hours')).strftime('%Y%m%d')
    #lista en un txt todos los barridos del dia.
    os.system('ls -l '+rutaRadar+Ahora+'* >'+rutaAqui+'Ultimos.txt')

    #Mira la lista de los ultimos 
    f = open(rutaAqui+'Ultimos.txt','r')
    L = f.readlines()
    f.close()
    #escoge solo los ultimos 5  barridos de radar - 55min
    L = [i for i in L if i.endswith('120.nc\n')]
    L = L[-5:]

    #Encuentra la cantidad de nc en rutaExtrapol de los ultimos 5 barridos en rutaRadar y se los asigna en un
    #dic.
    ListExtrapol = os.listdir(rutaExtrapol)
    Nbarridos = {}
    for l in L:
        fechaText = l.split('/')[-1].split('_')[0] 
        Son = [i for i in ListExtrapol if i.startswith(fechaText)]
        Nbarridos.update({fechaText: len(Son)})
    ksort = Nbarridos.keys()
    ksort =  list(np.sort(list(ksort)))
    #Elige el ultimo barrido con mas cantidad de extrapolados, ese es el paso de tiempo inicial de
    #la extrapolacion
    Flag = True
    for k in ksort[::-1]:
        # si hay barridos de extrapolacion para al menos la proxima hora (12*5 = 1h), elige la fecha
        if Nbarridos[k]>12 and Flag:
            Fecha = k
            Flag = False
            print ('Fecha seleccionada: '+Fecha)
            print ('Cantidad de archivos: %s'%Nbarridos[k])

    #Para la fecha elegida, lista los archivos que hay
    ListExtrapol = os.listdir(rutaExtrapol)
    ListExtrapol = [i for i in ListExtrapol if i.startswith(Fecha)]
    ListExtrapol.sort()
    #Crea labels de fechas a las que corresponderia cada barrido de extrapolacion 
    ListaMinutos = [dt.datetime.strptime(l.split('_')[0], '%Y%m%d%H%M') + dt.timedelta(minutes = float(l.split('_')[-1].split('.')[0])) for l in ListExtrapol]
    ListaMinutosText = [i.strftime('%Y%m%d%H%M')+'_007_120.bin' for i in ListaMinutos]
    #Copia los archivos de extrapolacion escogidos en ruta provisional, con la fecha a la que
    #corresponderia la extrapolacion
    for i,j in zip(ListExtrapol, ListaMinutosText):
        comando = 'cp '+rutaExtrapol+i+' '+rutaCopia+j
        os.system(comando)
    print ('Aviso: Archivos copiados correctamente en %s \n'%rutaCopia)

    #||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    #||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    #||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    #||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    print ('############################## CONVIERTE ARCHIVOS NUEVOS A RAIN, CONV Y STRA ###########\n')

    #esta condicion debe ser de la edad de los archivos en rutaCopia
    ListaViejos = glob.glob(rutaRadar+'*_extrapol.nc')
    #Busca archivos viejos de extrapolacion en la lista de archivos convertidos a lluvia
    if len(ListaViejos)>0:
            for i in ListaViejos:
                    os.system('rm '+i)
            print ('Aviso: Archivos de extrapolacion viejos han sido borrados de %s'%rutaCopia)
    else:
            print ('Aviso: No hay archivos viejos para borrar de extrapolacion.')

    #Fechas
    FechaI = ListaMinutos[0].strftime('%Y-%m-%d')
    FechaF = ListaMinutos[-1].strftime('%Y-%m-%d')
    Hora1 = ListaMinutos[0].strftime('%H:%M')
    Hora2 = ListaMinutos[-1].strftime('%H:%M')

    #Rutas del comando de conversion 
    rutaCodigo = '/media/nicolas/Home/Jupyter/Soraya/git/Alarmas/06_Crones/radar_otroformato/Radar2RainConvStra.py'
    rutaNC = '/media/nicolas/Home/nicolas/101_RadarClass/'

    #Ejecuta el comando de conversion -  pasa de dbz a mm.
    print ('Aviso: Conviertiendo entre: '+FechaI+' '+Hora1+' y '+FechaF+' '+Hora2+ '\n')
    comando = rutaCodigo+' '+FechaI+' '+FechaF+' '+rutaCopia+' '+rutaRadar+' -1 '+Hora1+' -2 '+Hora2+' -e'
    os.system(comando)
    print ('\n')


########################################################################################################################
#EJECUCION
########################################################################################################################

#try:
#ejecucion
rad_extrapol()

################
#Correo - Soraya
################

#Se revisan los archivos en la ruta donde se deben estar actualizando.
now = dt.datetime.now()
ago = now-dt.timedelta(minutes=10)
ruta='/media/nicolas/Home/nicolas/102_RadarExtrapol/'
times=[]

for root, dirs,files in os.walk(ruta):
    #se itera sobre todos los archivos en la ruta.
    for fname in files:
        # se llega a cada archivo
        path = os.path.join(root, fname)
        #se obtiene la fecha de modificacion de cada path
        mtime = dt.datetime.fromtimestamp(os.stat(path).st_mtime)
        #si hay alguno modificado en los ultimos 'ago' minutos entonces se guarda.
        if mtime > ago:
            times.append(mtime.strftime('%Y%m%d-%H:%M'))
#             print('%s modified %s'%(path, mtime))

# si no hay ningun archivo modificado en los ultimos 'ago' minutos manda un correo a Soraya
if len(times)==0:
    print ('no se ha modificado ningun archivo en los ultimos %s min en la ruta %s'%(ago,ruta))
    to='scgiraldo11@gmail.com'
    Asunto='SC_CamposExtrapol12 no se ha ejecutado'
    Mensaje='No se han generado archivos .nc de radarExtrapol en Amazonas (192.168.1.12): '+ruta+'en los ultimos 20 min. #mensajedeprueba.'
    gmail_user = 'scgiraldo11@gmail.com'
    gmail_pwd = '12345s.oraya'
    smtpserver = smtplib.SMTP("smtp.gmail.com",587)
    smtpserver.ehlo()
    smtpserver.starttls()
    smtpserver.ehlo
    smtpserver.login(gmail_user, gmail_pwd)
    header = 'To:' + to + '\n' + 'From: ' + gmail_user + '\n' + 'Subject:'+Asunto+' \n'
    msg = header + '\n'+ Mensaje +'\n\n'
    smtpserver.sendmail(gmail_user, to , msg)
    smtpserver.close()

# si hay, crea el .prc en donde debe estar por defecto.
else:
    print ('genera PRC y copia en SAL')
    ############
    ####PRC#####
    ############
    f = open('/media/nicolas/Home/Jupyter/Soraya/git/Alarmas/06_Crones/radar_otroformato/SC_CamposExtrapol12.prc','w') #opens file with name of "test.txt"
    #linea 1: nombre prc
    f.write('SC_CamposExtrapol12\n')
    #linea 2: descripcion
    f.write('Generacion de archivos .nc de radarExtrapol en Amazonas (192.168.1.12): en '+ruta+'\n')
    #linea 3: cada cuanto se corre el proceso
    f.write('5\n')
    #- fecha de generacion del prc
    #linea 4: ano en 4 digitos
    f.write(dt.datetime.now().strftime('%Y')+'\n')
    #linea 5: mes en 2 digitos
    f.write(dt.datetime.now().strftime('%m')+'\n')
    #linea 6: dia en 2 digitos
    f.write(dt.datetime.now().strftime('%d')+'\n')
    #linea 7: hora en 2 digitos
    f.write(dt.datetime.now().strftime('%H')+'\n')
    #linea 8: minutos en 2 digitos
    f.write(dt.datetime.now().strftime('%M')+'\n')
    #linea 9: numero de ejecuciones limite en que el prc no se ha ejecutado
    f.write('4\n') # 20  min.
    #linea 10: lista de correos adicionales al de los operacionales al que se quiere enviar
    f.write('scgiraldo11@gmail.com \n')
    #linea 11: mensaje 
    f.write('No se han generado archivos .nc de radarExtrapol en Amazonas (192.168.1.12): '+ruta+' en los ultimos 20 min.')
    #se termina de crear .prc
    f.close()
    # Se copia el .prc en la ruta donde debe estar en el servidor
    #Para que esto funcione se debe hacer ssh-copy-id desde el usuario e ip donde se envia hacia el usuario  e ip donde se recibe para que el login quede automatico
    os.system('rsync -r -v -a -z -e ssh /media/nicolas/Home/Jupyter/Soraya/git/Alarmas/06_Crones/radar_otroformato/SC_CamposExtrapol12.prc hidrologia@192.168.1.74:/home/torresiata/SIVP/archivosPRC/')

#Presenta el tiempo que le tomo ejecutarse
horaFin = dt.datetime.now()
elapsed = horaFin - horaInicio
print ('Tiempo transcurrido: '+str(elapsed.seconds) + ' segundos')

##------------------------------------------------
## grafica all radar extent - radar-obs y extrapol
##-----------------------------------------------
#
#print ('/n')
#print ('----------------------------------------------')
#print ('Grafica allradarextent - Radar Obs y Extrapol')
#print ('/n')
#
#import matplotlib
#matplotlib.use('Agg')
#import pylab as pl
#import funciones_sora as fs
#
#def plot_extrapol(idlcolors=True):
#    # inputs acumula radar
#
#    Dt=300.
#    nc_basin= '/media/nicolas/maso/Mario/basins/260.nc'
#    codigos = [260]
#    accum=False;path_tif=None;meanrain_ALL=True;save_bin=False;path_res=None,
#    umbral=0.005;rutaNC='/media/nicolas/Home/nicolas/101_RadarClass/'
#    path_figs= '/media/nicolas/Home/Jupyter/Soraya/Op_Alarmas/Result_to_web/radar/acum_radar/prueba/'
#
#    starts = [fs.round_time(dt.datetime.now()) - pd.Timedelta('30m'),fs.round_time(dt.datetime.now()) ]
#    ends = [fs.round_time(dt.datetime.now()), fs.round_time(dt.datetime.now()) + pd.Timedelta('30m')]
#    figsnames = ['30minbefore_allradarextent','30minahead_allradarextent']
#
#    for start,end,figname in zip(starts,ends,figsnames):
#        # Acumula radar.
#        print start,end
#        print start+ pd.Timedelta('5h'),end+ pd.Timedelta('5h')
#        print ('/n')
#        dflol,radmatrix = fs.get_radar_rain(start,end,Dt,nc_basin,codigos,all_radextent=True)
#        # inputs fig
#        path_figure =  path_figs+figname+'.png'
#        rad2plot = radmatrix.T
#        window_t='30m'
#        #fig
#        fs.plot_allradarextent(rad2plot,window_t,idlcolors=idlcolors,path_figure=path_figure,extrapol_axislims=True)
#
##ejecucion
#print ('/n')
#print ('start: %s'%(dt.datetime.now()))
#plot_extrapol()
#print ('/n')
#print ('end: %s'%(dt.datetime.now()))
#
