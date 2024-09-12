#!/bin/bash
#

# @source: git clone https://github.com/hpc/Evalys-LANL.git
###
###
#
# trigger with, for example:
# crontab entry:
#    37 3 1,16 * * sh -c "[ -x ${SACCT_DIR}/bin/collectSacctDB.sh ] && \
#             ( cd ${SACCT_DIR}/$(hostname -s); ${SACCT_DIR}/bin/collectSacctDB.sh )"
#
# To Do:
#   - self emit --help
#   - argument processing:
#     - set SACCT_DIR
#     - set DEFAULT_INITIALIZED date
#     - set timeouts, esp. for json sacct query
#     - set breadcrumb, previous timeout used, esp. if previous run had no json output
#     - check for timeout_long breadcrumb, increase from TIMEOUT_LONG if found
#     - SKIP_JSON=false
#   - verify env
#     - slurm env & commands appear reasonable
#   - refine collect/timeout/wait
#     - slice query into smaller time chunks if no json output in a TIMEOUT period
#     - instead of sleeping, poll for output, possibly warn, if no progress made
#       then if no json ever collected, leave timeout_long breadcrumb

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

SKIP_JSON="true"

TIMEOUT_SAFE_MIN=5
# tune for your db responsiveness and size (TIMEOUT_SHORTLONG_MULTIPLIER)
TIMEOUT_SHORTLONG_MULTIPLIER=20
TIMEOUT_SHORT=${TIMEOUT_SAFE_MIN}
TIMEOUT_LONG=$((${TIMEOUT_SHORT} * ${TIMEOUT_SHORTLONG_MULTIPLIER}))
TIMEOUT_UNITS="m"
TIMEOUT=${TIMEOUT_SHORT}${TIMEOUT_UNITS}
TIMEOUT_LONG=${TIMEOUT_LONG}${TIMEOUT_UNITS}

#Exit codes
EX_OK=0
EX_TXTJSON_MISMATCH=1
EX_TXTDELIM_MISMATCH=2
EX_JSONDELIM_MISMATCH=3
EX_OVERWRITE=10
EX_INCOMPLETE=11
EX_PREP_DATE=20

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
ISATTY=""

FIELDS="jobidraw%9,jobid,priority,qos,partition,nnodes%6,ntasks,submit%24,eligible%24,start%24,end%24,timelimit%24,state,reservation,nodelist,flags%36,submitline%-7,account,user,consumedenergy,consumedenergyraw,failednode%-80"
ARGS=" -a -X "
P_ARGS=" ${ARGS} -p --delimiter='${DELTA}' "
JSON_ARGS=" ${ARGS} --json "
JSON_TAIL_LINES=275

START_PREFIX=" --start="
START_SUFFIX="T00:00"
START="${START_PREFIX}${INITIALIZED}${START_SUFFIX}"


Prep() {
	if [[ -c /dev/tty ]] ; then
		ISATTY=">/dev/tty"
	fi
	# sanity check
	date -d "${INITIALIZED}" +%s >/dev/null 2>&1
	if [ $? -ne ${EX_OK} ] ; then
		echo date failure determining when the db was initialized. ${ISATTY}
		exit ${EX_PREP_DATE}
	fi
	if [ -z "${INITIALIZED}" ] ; then
		INITIALIZED=${DEFAULT_INITIALIZED}
		echo initialized: ${DEFAULT_INITIALIZED} ${ISATTY}
	fi
	mkdir -p ${TMPDIR}
}

CollectSlurmData() {
	# json takes much (much) longer
	#DEBUG_TIME="/usr/bin/time -v"
	# timeout ${TIMEOUT_LONG} ${DEBUG_TIME}	sacct ${JSON_ARGS} ${START} > ${OUT_JSON} &
	if [[ -z "${SKIP_JSON}" ]] ; then
                nohup timeout ${TIMEOUT_LONG} sacct ${JSON_ARGS} ${START} > ${OUT_JSON}	&
	fi

	# these don't take very long
	timeout ${TIMEOUT} sacct --format=${FIELDS} ${ARGS} ${START}	> ${OUT} &
	timeout ${TIMEOUT} sacct --format=${FIELDS} ${P_ARGS} ${START}	> ${OUT_DELIM} &
	wait
	return
}

ConvertDelimiters() {
	sed "s/${COMMA}/|/g" ${OUT_DELIM} | sed "s/^/'/" | sed "s/$/'/"	> ${TMPFILE}
	sed "s|${DELTA}|${COMMA}|g" ${TMPFILE}				> ${OUT_DELIM}${DELTA}
	iconv -c -f us-ascii -t UTF-8//TRANSLIT ${OUT_DELIM}${DELTA}	> ${OUT_DELIM}
	rm -f ${TMPFILE} ${OUT_DELIM}${DELTA}
	return
}

CheckValid() {
	local rc=${EX_OK}
	local last_jobid_json=""
	last_jobid_txt=$(tail -1 ${OUT} | awk '{print $1;}')
	last_jobid_delim=$(tail -1 ${OUT_DELIM} | awk -F "${COMMA}" '{print $1;}' | sed "s/${SINGLE_QUOTE}//g" )

	# XXX JSON *not* fatal: See SchedMD ticket# 20797, https://support.schedmd.com/show_bug.cgi?id=20797
	if [[ -z "${SKIP_JSON}" ]] ; then
	        last_jobid_json=$(tail -${JSON_TAIL_LINES} ${OUT_JSON} | grep job_id | grep -v '"job_id": 0,' | sed 's/,$//' | sort | uniq | awk '{print $2;}' )

        	if [[ -n "${last_jobid_json}" ]] ; then
        		if [[ "${last_jobid_txt}" != "${last_jobid_json}" ]] ; then
        			(\
                                 echo ${MSG_JOBID_MISMATCH} 		;\
                                 echo "	txt:	${last_jobid_txt}"	;\
                                 echo "	json:	${last_jobid_json}" ) | tee ${WARNING}txt,json
                                rc=${EX_TXTJSON_MISMATCH}
                        fi
	                if [[ "${last_jobid_json}" != "${last_jobid_delim}" ]] ; then
		                (\
                                 echo ${MSG_JOBID_MISMATCH}		;\
                                 echo "	json:	${last_jobid_json}"	;\
                                 echo "	delim:	${last_jobid_delim}"	) | tee ${WARNING}json,delim
		                rc=${EX_JSONDELIM_MISMATCH}
	                fi
	        else
		        # ...for TIMEOUT_LONG
		        wait
	        fi
	fi ## SKIP_JSON

	if [[ "${last_jobid_txt}" != "${last_jobid_delim}" ]] ; then
	       (\
                echo ${MSG_JOBID_MISMATCH}		;\
                echo "	txt:	${last_jobid_txt}"	;\
                echo "	delim:	${last_jobid_delim}"	) | tee ${WARNING}txt,delim
               rc=${EX_TXTDELIM_MISMATCH}
	fi

	if [[ "${rc}" -eq ${EX_OK} ]] ; then
                if [[ -e ${TSTAMP_YMD}/${GENERATED} ]] ; then
                        rc=${EX_OVERWRITE}
                else
		        # if incomplete or in error, leave behind all of the breadcrumb temporary files
		        # we record the slurm version as the output format and fields may differ between slurm versions
			if [ -s "${OUT}" -a -s "${OUT_DELIM}" -a \( -s "${OUT_JSON}" -o "${SKIP_JSON}" \) ] ; then
			        mkdir -p ${TSTAMP_YMD}
                                echo ${TSTAMP} > ${TSTAMP_YMD}/${GENERATED}
			        sinfo --version | awk '{print $2}' > ${TMPDIR}/${SLURM_VERSION}
			        mv ${TMPDIR}/* ${TSTAMP_YMD}/

			        # completed successfully, seal it
			        ( cd ${TSTAMP_YMD}; chmod ${READ_ONLY} * )
			        trap "rm -rf ${TMPDIR}" 0
		        else
			        echo "${MSG_INCOMPLETE} See: ${TMPDIR}" ${ISATTY}
			        rc=${EX_INCOMPLETE}
		        fi
	        fi
	fi
	return ${rc}
}

main() {
	local rc=${EX_OK}
	Prep
	CollectSlurmData
	ConvertDelimiters
	CheckValid	# potentially long to return (TIMEOUT_LONG)
	rc=$?
	exit ${rc}
}

main $*
exit $?

#python3 src/__main__.py -i${OUT} -t36 -n${CLUSTERNAME} -c368
#vi: set background=dark paste
