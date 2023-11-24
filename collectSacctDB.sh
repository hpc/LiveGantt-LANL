#!/bin/sh

sinfo --version > slurm_version

CLUSTERNAME=$(scontrol show config | awk '/ClusterName/ {print $NF}')
OUT=sacct.out.${CLUSTERNAME}.start=2023-10-01T00:00.no-identifiers.txt
FIELDS="jobidraw%9,priority,qos,partition,nnodes%6,ntasks,submit%24,eligible%24,start%24,end%24,timelimit%24,state,reservation,nodelist,flags%36"
ARGS="-a -X -p"
START="--start=2023-10-01T00:00"

time sacct --format=${FIELDS} ${ARGS} ${START} --delimiter=/ > ${OUT}

sed 's/,/|/g' ${OUT} > out.txt
sed 's|/|,|g' out.txt > ${OUT}

#python3 src/__main__.py -i${OUT} -t36 -n${CLUSTERNAME} -c368
