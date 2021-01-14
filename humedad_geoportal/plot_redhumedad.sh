#!/bin/bash

date
appdir=`dirname $0`
logfile=$appdir/plot_redhumedad.log
lockfile=$appdir/plot_redhumedad.lck
pid=$$

echo $appdir

function plot_redhumedad {

python3 /media/nicolas/Home/Jupyter/Soraya/git/Alarmas/06_Crones/plot_redhumedad.py

}


(
        if flock -n 301; then
                cd $appdir
                plot_redhumedad
                echo $appdir $lockfile
                rm -f $lockfile
        else
            	echo "`date` [$pid] - Script is already executing. Exiting now." >> $logfile
        fi
) 301>$lockfile

exit 0