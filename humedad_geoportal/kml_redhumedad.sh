#!/bin/bash

date
appdir=`dirname $0`
logfile=$appdir/kml_redhumedad.log
lockfile=$appdir/kml_redhumedad.lck
pid=$$

echo $appdir

function kml_redhumedad {
python3 /media/nicolas/Home/Jupyter/Soraya/git/Alarmas/06_Crones/kml_redhumedad.py
}


(
        if flock -n 301; then
                cd $appdir
                kml_redhumedad
                echo $appdir $lockfile
                rm -f $lockfile
        else
            	echo "`date` [$pid] - Script is already executing. Exiting now." >> $logfile
        fi
) 301>$lockfile

exit 0