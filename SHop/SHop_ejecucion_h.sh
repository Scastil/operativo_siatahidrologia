#!/bin/bash

date
appdir=`dirname $0`
logfile=$appdir/SHop_ejecucion_h.log
lockfile=$appdir/SHop_ejecucion_h.lck
pid=$$

echo $appdir

function SHop_ejecucion_h {
    python3 /media/nicolas/Home/Jupyter/Soraya/git/Alarmas/06_Crones/SHop_ejecucion_h.py
}


(
        if flock -n 421; then #7min
                cd $appdir
                SHop_ejecucion_h
                echo $appdir $lockfile
                rm -f $lockfile
        else
            	echo "`date` [$pid] - Script is already executing. Exiting now." >> $logfile
        fi
) 421>$lockfile #7min

exit 0