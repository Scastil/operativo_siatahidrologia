#!/bin/bash

date
appdir=`dirname $0`
logfile=$appdir/assess_RadRisk.log
lockfile=$appdir/assess_RadRisk.lck
pid=$$

echo $appdir

function assess_RadRisk {
	python3 /media/nicolas/Home/Jupyter/Soraya/git/Alarmas/06_Crones/assess_RadRisk.py
}


(
        if flock -n 301; then
                cd $appdir
                assess_RadRisk
                echo $appdir $lockfile
                rm -f $lockfile
        else
            	echo "`date` [$pid] - Script is already executing. Exiting now." >> $logfile
        fi
) 301>$lockfile

exit 0