#!/bin/sh

sinfo --version > slurm_version
GENERATED=Generated:`date +%FT%T`

printf "db initialized 2019-12-01\n"
CLUSTERNAME=$(scontrol show config | awk '/ClusterName/ {print $NF}')
OUT=sacct.out.${CLUSTERNAME}.start=2023-10-01T00:00.no-identifiers.txt
FIELDS="jobidraw%9,jobid,priority,qos,partition,nnodes%6,ntasks,submit%24,eligible%24,start%24,end%24,timelimit%24,state,reservation,nodelist,flags%36,submitline,account,user,"
ARGS="-a -X -p"
START="--start=2023-10-01T00:00"

time sacct --format=${FIELDS} ${ARGS} ${START} --delimiter={ > ${OUT}

#chmod 0444 slurm_version  ${OUT}

# TODO These can be the same, right?
sed 's/,/|/g' ${OUT} > out.txt
sed 's|{|,|g' out.txt > out2.txt
iconv -c -f us-ascii -t UTF-8//TRANSLIT out2.txt > ${OUT}
rm out.txt
rm out2.txt

#python3 src/__main__.py -i${OUT} -t36 -n${CLUSTERNAME} -c368