#!/bin/sh

sinfo --version > slurm_version
# TODO Make sure that naming is dynamic & unblocked
GENERATED=Generated:`date +%FT%T`
\rm -f Generated:* >/dev/null 2>&1
touch ${GENERATED}

printf "db initialized 2019-12-01\n"
CLUSTERNAME=$(scontrol show config | awk '/ClusterName/ {print $NF}')
OUT=sacct.out.${CLUSTERNAME}.start=2023-10-01T00:00.no-identifiers.txt
FIELDS="jobid%9,priority,qos,partition,nnodes%6,ntasks,submit%24,eligible%24,start%24,end%24,timelimit%24,state,reservation,nodelist,flags%36"
ARGS="-a -X -p"
START="--start=2023-10-01T00:00"

time sacct --format=${FIELDS} ${ARGS} ${START} > ${OUT}

chmod 0444 slurm_version  ${GENERATED} ${OUT}

# TODO I've gotta do that
sed 's/,/|/g' ${OUT} > out.txt
sed 's|/|,|g' out.txt > ${OUT}

python3 src/__main__.py -i${OUT} -t36 -n${CLUSTERNAME} -c368