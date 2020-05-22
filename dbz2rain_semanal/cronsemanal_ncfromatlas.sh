#!/bin/bash

date
appdir=`dirname $0`
logfile=$appdir/cronsemanal_ncfromatlas.log
lockfile=$appdir/cronsemanal_ncfromatlas.lck
pid=$$

echo $appdir

function cronsemanal_ncfromatlas {

/media/nicolas/Home/.py3/bin/python3 /media/nicolas/Home/Jupyter/Soraya/git/Alarmas/06_Crones/cronsemanal_ncfromatlas.py

}


(
        if flock -n 605000; then
                cd $appdir
                cronsemanal_ncfromatlas
                echo $appdir $lockfile
                rm -f $lockfile
        else
            	echo "`date` [$pid] - Script is already executing. Exiting now." >> $logfile
        fi
) 605000>$lockfile

exit 0
