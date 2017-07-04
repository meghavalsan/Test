#!/bin/bash

 # Note 'LOAD DATA LOCAL' command is not supported by default in MySQL.
 # To activate it do one of below:
 # Either in ~/.my.cnf or <mysql_installation_path>/my.cnf add following line under [mysql] group
 #      local-infile=1
 #
 #     OR
 # 
 # while invoking MySQL shell use --local-infile option
 #    e.g.    mysql --local-infile -uuser -p
 #

#set -x 

DBNAME=PAM
MYSQL_USER=root
MYSQL_PASS=root
INPUT_BASE_DIR=/home/user/Auto-Grid/PAM/data/test_data
tenant_name="FPL"
tenant_query="select id from tenant where name='FPL'"
tenant_id=`mysql -u${MYSQL_USER} -p${MYSQL_PASS} ${DBNAME} --skip-column-names <<< ${tenant_query}`


for d in `ls -d ${INPUT_BASE_DIR}/*/`
do
	echo 'Checking directory ' ${d}

	for f in `ls ${d}/*`
	do
		echo ' Processing ' ${f}
		if [[ ${f} == *"EDNA"* ]]
		then
			#Get last token after '/' i.e. file name
			fname=${f##*/}
			#Extract feeder id from file name  - feederid.csv
			feederId="$( cut -d '.' -f 1 <<< "$fname" )"
			query="LOAD DATA LOCAL INFILE '${f}' INTO TABLE edna FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n' IGNORE 1 LINES (ExtendedId, @time_var, value, valuestring, status) SET timestamp_utc=STR_TO_DATE(@time_var, '%m/%d/%Y %r'), feeder=${feederId}, score=UNIX_TIMESTAMP(timestamp_utc), tenant_id=${tenant_id}"
			mysql -u${MYSQL_USER} -p${MYSQL_PASS} ${DBNAME} << eof
			${query}
eof
		elif [[ ${f} == *"AMI"* ]]
		then
			query="LOAD DATA LOCAL INFILE '${f}' INTO TABLE ami FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n' IGNORE 1 LINES (@fld1, @fld2, @fld3, @fld4, @fld5, @fld6, @fld7, @fld8, @fld9, @fld10, @fld11) SET feeder=@fld2, meter=@fld6, event=@fld7, timestamp_utc=STR_TO_DATE(@fld8, '%m/%d/%Y %r'), score=UNIX_TIMESTAMP(timestamp_utc), tenant_id=${tenant_id}"
			mysql -u${MYSQL_USER} -p${MYSQL_PASS} ${DBNAME} << eof
			${query}
eof
		elif [[ ${f} == *"SCADA"* ]]
		then
			query="LOAD DATA LOCAL INFILE '${f}' INTO TABLE scada FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n' IGNORE 1 LINES (feeder, OBSERV_DATA, @time_var) SET timestamp_utc=STR_TO_DATE(@time_var, '%m/%d/%Y %r'), score=UNIX_TIMESTAMP(timestamp_utc), tenant_id=${tenant_id}"
			mysql -u${MYSQL_USER} -p${MYSQL_PASS} ${DBNAME} << eof
			${query}
eof
		fi
	done
done

#Tickets files are present in input directory itself. 

for f in `ls ${INPUT_BASE_DIR}/TICKETS*`
do
	echo ' Processing ' ${f}
		
	query="LOAD DATA LOCAL INFILE '${f}' INTO TABLE tickets FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n' IGNORE 1 LINES (@fld1, @fld2, @fld3, @fld4, @fld5, @fld6, @fld7, @fld8, @fld9, @fld10, @fld11,@fld12, @fld13, @fld14, @fld15, @fld16, @fld17, @fld18, @fld19, @fld20, @fld21, @fld22)
	        SET feeder=@fld2, dwTicketId=@fld1,troubleTicketId=@fld3,interruptionTypeCode=@fld5,interruptionCauseCode=@fld8,supportCode=@fld7,cmi=@fld9,repairActionType=@fld12,repairActionDescription=@fld14,powerOff_utc=STR_TO_DATE(@fld10, '%m-%d-%Y %H:%i:%s'),powerRestore_utc=STR_TO_DATE(@fld11, '%m-%d-%Y %H:%i:%s'), score=UNIX_TIMESTAMP(powerOff_utc), tenant_id=${tenant_id}"
	mysql -u${MYSQL_USER} -p${MYSQL_PASS} ${DBNAME} << eof
	${query}
eof
done

