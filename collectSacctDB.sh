#!/bin/sh


sinfo --version > slurm_version
GENERATED=Generated:`date +%FT%T`
\rm -f Generated:* >/dev/null 2>&1
touch ${GENERATED}

printf "db initialized 2019-12-01\n"
CLUSTERNAME=$(scontrol show config | awk '/ClusterName/ {print $NF}')
OUT=/tmp/sacct.out.${CLUSTERNAME}.start=2019-12-01T00:00.no-identifiers.txt
FIELDS="jobid%9,priority,qos,partition,nnodes%6,ntasks,submit%24,eligible%24,start%24,end%24,timelimit%24,state,reservation,flags%36"
ARGS="-a -X -p"
START="--start=2019-12-01T00:00"

time sacct --format=${FIELDS} ${ARGS} ${START} > ${OUT}

chmod 0444 slurm_version  ${GENERATED} ${OUT}

sed 's/|/,/g' ${OUT} > temp.txt && mv temp.txt ${OUT}

# TODO Sanitization

python3 -m livegantt ${OUT}