#!/bin/bash
#

# @source: git clone https://github.com/hpc/Evalys-LANL.git
###
###
#
# trigger with, for example:
# crontab entry:
#    37 3,16 1,16 * * sh -c "[ -x ${SACCT_DIR}/bin/collectSacctDB.sh ] && \
#             ( cd ${SACCT_DIR}/$(hostname -s); ${SACCT_DIR}/bin/collectSacctDB.sh )"
#
# To Do:
#   - self emit --help
#   - argument processing:
#     - set SACCT_DIR
#     - set DEFAULT_INITIALIZED date
#     - set timeouts, esp. for json sacct query
#   - verify env
#     - slurm env & commands appear reasonable

set -e
set -u

### find earliest db job record
### default to:
DEFAULT_INITIALIZED="2023-03-15"
findEarliestDBJobRecord() {
	local first_recs_limit=3
	local epoch=1970-01-01T00:00
	local _ts=""
	# in case the data base was just initialized but there were jobs running (in state save loc dir), strip out those that have 'None'
	for _x in $(timeout ${TIMEOUT_LONG} sacct -a -X --start=${epoch} --end=now --format="start%28,end%28" -n | grep -v None | head -${first_recs_limit})
	do
		if [[ "${_x}" =~ : ]] ; then
			# likely a timestamp, strip out the time leaving only the date
			_ts=${_x%%T*}
			break
		fi
	done
	if [[ -z "${_ts}" ]] ; then
		_ts=${DEFAULT_INITIALIZED}
	fi
	echo ${_ts}
	return 0
}

SLURM_VERSION="SlurmVersion"
TSTAMP_YMD=`date +%Y-%b-%d`
TSTAMP=`date +%FT%T`
TIMEOUT_SHORTLONG_MULTIPLIER=10
TIMEOUT_SHORT=1
TIMEOUT_LONG=$((${TIMEOUT_SHORT} * ${TIMEOUT_SHORTLONG_MULTIPLIER}))
TIMEOUT_UNITS="m"
TIMEOUT=${TIMEOUT_SHORT}${TIMEOUT_UNITS}
TIMEOUT_LONG=${TIMEOUT_LONG}${TIMEOUT_UNITS}
TMPDIR=/tmp/$(basename $0 .sh).$(id -u -n).$$
TMPFILE=${TMPDIR}/delim-conv.$$
MSG_INCOMPLETE="output not complete"
MSG_JOBID_MISMATCH="jobid mismatch in output formats"
GENERATED="Generated"
CLUSTERNAME=$(scontrol show config | awk '/ClusterName/ {print $NF}')
INITIALIZED="$(findEarliestDBJobRecord)"
OUT=sacct.out.${CLUSTERNAME}.start=${INITIALIZED}T00:00.not-anonymized.txt
OUT_DELIM=${TMPDIR}/${OUT/txt/delim}
OUT_JSON=${TMPDIR}/${OUT/txt/json}
OUT=${TMPDIR}/${OUT}
WARNING=${TMPDIR}/Warning:
DELTA="∆"
COMMA=","
SINGLE_QUOTE="'"
READ_ONLY="0444"

FIELDS="jobidraw%9,jobid,priority,qos,partition,nnodes%6,ntasks,submit%24,eligible%24,start%24,end%24,timelimit%24,state,reservation,nodelist,flags%36,submitline%-7,account,user,consumedenergy,consumedenergyraw,failednode%-80"
ARGS=" -a -X "
P_ARGS=" ${ARGS} -p --delimiter='${DELTA}' "
JSON_ARGS=" ${ARGS} --json "
JSON_TAIL_LINES=275

START_PREFIX=" --start="
START_SUFFIX="T00:00"
START="${START_PREFIX}${INITIALIZED}${START_SUFFIX}"


Prep() {
	# sanity check
	date -d "${INITIALIZED}" +%s >/dev/null 2>&1
	if [ $? -ne 0 ] ; then
		echo date failure determining when the db was initialized. >/dev/tty
		exit 1
	fi
	if [ -z "${INITIALIZED}" ] ; then
		INITIALIZED=${DEFAULT_INITIALIZED}
		echo initialized: ${DEFAULT_INITIALIZED} >/dev/tty
	fi
	mkdir -p ${TMPDIR}
}

CollectSlurmData() {
	# json takes much longer
	DEBUG_TIME="/usr/bin/time -v"
	timeout ${TIMEOUT_LONG} ${DEBUG_TIME}	sacct ${JSON_ARGS} ${START}			> ${OUT_JSON}	&
	timeout ${TIMEOUT}			sacct --format=${FIELDS} ${ARGS} ${START}	> ${OUT}	&
	timeout ${TIMEOUT}			sacct --format=${FIELDS} ${P_ARGS} ${START}	> ${OUT_DELIM}	&
	wait
}

ConvertDelimiters() {
	sed "s/${COMMA}/|/g" ${OUT_DELIM} | sed "s/^/'/" | sed "s/$/'/"	> ${TMPFILE}
	sed "s|${DELTA}|${COMMA}|g" ${TMPFILE}				> ${OUT_DELIM}${DELTA}
	iconv -c -f us-ascii -t UTF-8//TRANSLIT ${OUT_DELIM}${DELTA}	> ${OUT_DELIM}
	rm -f ${TMPFILE} ${OUT_DELIM}${DELTA}
}

CheckValid() {
	local rc=0
	last_jobid_txt=$(tail -1 ${OUT} | awk '{print $1;}')
	last_jobid_delim=$(tail -1 ${OUT_DELIM} | awk -F "${COMMA}" '{print $1;}' | sed "s/${SINGLE_QUOTE}//g" )

	# XXX JSON *not* fatal: See SchedMD ticket# 20797, https://support.schedmd.com/show_bug.cgi?id=20797
	last_jobid_json=$(tail -${JSON_TAIL_LINES} ${OUT_JSON} | grep job_id | grep -v '"job_id": 0,' | sed 's/,$//' | sort | uniq | awk '{print $2;}' )


	if [[ "${last_jobid_txt}" != "${last_jobid_json}" ]] ; then
		(\
                 echo ${MSG_JOBID_MISMATCH} 		;\
                 echo "	txt:	${last_jobid_txt}"	;\
                 echo "	json:	${last_jobid_json}" ) | tee ${WARNING}txt,json
		 rc=1
	 fi

	if [[ "${last_jobid_txt}" != "${last_jobid_delim}" ]] ; then
	       (\
                echo ${MSG_JOBID_MISMATCH}		;\
                echo "	txt:	${last_jobid_txt}"	;\
                echo "	delim:	${last_jobid_delim}"	) | tee ${WARNING}txt,delim
	       rc=2
	fi
	if [[ "${last_jobid_json}" != "${last_jobid_delim}" ]] ; then
		(\
                 echo ${MSG_JOBID_MISMATCH}		;\
                 echo "	json:	${last_jobid_json}"	;\
                 echo "	delim:	${last_jobid_delim}"	) | tee ${WARNING}json,delim
		rc=3
	fi

	if [[ "${rc}" -ne 0 ]] ; then
		return ${rc}
	fi

	if [[ -e ${TSTAMP_YMD}/${GENERATED} ]] ; then
		rc=10
	else
		# if incomplete or in error, leave behind all of the breadcrumb temporary files
		if [[ -s ${OUT} && -s ${OUT_DELIM} && -s ${OUT_JSON} ]] ; then
			mkdir -p ${TSTAMP_YMD}
                        echo ${TSTAMP} > ${TSTAMP_YMD}/${GENERATED}
			sinfo --version | awk '{print $2}' > ${TMPDIR}/${SLURM_VERSION}
			mv ${TMPDIR}/* ${TSTAMP_YMD}/

			# completed successfully, seal it
			( cd ${TSTAMP_YMD}; chmod 0444 * )
			trap "rm -rf ${TMPDIR}" 0
		else

			echo "${MSG_INCOMPLETE} See: ${TMPDIR}" >/dev/tty
			rc=11
		fi
	fi
	return ${rc}
}

main() {
	local rc=0
	Prep
	CollectSlurmData
	ConvertDelimiters
	CheckValid
	rc=$?
	exit ${rc}
}

main $*
exit $?

#python3 src/__main__.py -i${OUT} -t36 -n${CLUSTERNAME} -c368
