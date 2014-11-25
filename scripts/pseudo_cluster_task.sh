#!/bin/sh

TIME_TO_SLEEP="0"
TASK_STATUS=""

while getopts "ht:s:" option 
do
    case "$option" in
        t)
            TIME_TO_SLEEP="$OPTARG"
            ;;
        s)
            TASK_STATUS="$OPTARG"
            ;;
        h)
            echo
            echo  "This program do nothing during -t seconds"
            echo  "Please show -t and -s key."
            echo  "   -t secs  - number of seconds to sleep"
            echo  "   -s state - task state in original queue"
            echo  "              original queue is extracted"
            echo  "              from file with statistics."
            echo  "   -h - Printing this message"
            echo  

            exit 0
            ;;
        \?)
            echo "You should specifie -t and -s parameters or -h for help"

            exit 1
            ;;
    esac

done

echo "  Time to sleep $TIME_TO_SLEEP seconds"

if [ TIME_TO_SLEEP != "0" ]
then
    sleep $TIME_TO_SLEEP
fi


