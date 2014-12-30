#! /bin/sh

TARGET_TEXT="IPVS: WLC: no destination available"
ROOT_DIR=$(pwd)
PROBLEM_FILE_PATH=$ROOT_DIR/tmp_check_mcluster_valid

if [ -f "$PROBLEM_FILE_PATH" ]; then
	rm $PROBLEM_FILE_PATH
fi

i=0

while (( $i < 5 ));
do 
	cat /var/log/messages | grep "`date --date="$i minutes ago" +%b" "%e" "%H:%M`" | grep -Ei "$TARGET_TEXT"  >> $PROBLEM_FILE_PATH 
	i=$((i+1)) 
done

EXPECTED_VALUE=0
COUNT=`cat $PROBLEM_FILE_PATH | wc -l`

if [ $COUNT -eq $EXPECTED_VALUE ]; then
        echo true
else
        echo false
fi

rm $PROBLEM_FILE_PATH
