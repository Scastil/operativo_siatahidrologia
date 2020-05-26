#!/bin/bash

date
appdir=`dirname $0`
logfile=$appdir/plot_radar.log
lockfile=$appdir/plot_radar.lck
pid=$$

echo $appdir

function plot_radar {

/media/nicolas/Home/.py3/bin/python3 /media/nicolas/Home/Jupyter/Soraya/git/Alarmas/06_Crones/plot_radar.py

}


(
        if flock -n 181; then
                cd $appdir
                plot_radar
                echo $appdir $lockfile
                rm -f $lockfile
        else
            	echo "`date` [$pid] - Script is already executing. Exiting now." >> $logfile
        fi
) 181>$lockfile

exit 0
