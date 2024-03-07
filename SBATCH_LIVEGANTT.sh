#!/bin/bash

#Submit this script with: sbatch filename

#SBATCH --time=:30:00   # walltime
#SBATCH --nodes=1   # number of nodes
#SBATCH --ntasks=1   # number of processor cores (i.e. tasks)
#SBATCH --job-name=LiveGantt   # job name
#SBATCH --account=vhafener   # account name
#SBATCH --qos=hpcdev   # qos name
#SBATCH --no-requeue   # do not requeue when preempted and on node failure
#SBATCH --signal=23@60  # send signal to job at [seconds] before end


# LOAD MODULEFILES, INSERT CODE, AND RUN YOUR PROGRAMS HERE

livegantt_version="1.0"
current_date=$(date +'--start=%Y-%m-%dT00:00')
outfile="sacct.out.$(scontrol show config | awk '/ClusterName/ {print $NF}').start=$(date +'%Y-%m-%d')T00:00.no-identifiers.txt"
bundle="/users/vhafener/LiveGantt/oci_images/livegantt_v${livegantt_version}/rootfs"

exec /users/vhafener/livegantt/collectSacctSB.sh

# Capture the process ID of the last background command
pid_collect=$!

# Wait for a.sh to finish
wait $pid_collect

# Launch b.sh after a.sh has finished
srun --container $bundle bash -c "python3 src/__main__.py -i/Users/vhafener/Repos/LiveGantt/${outfile} -o/Users/vhafener/Repos/LiveGantt/Charts/ -t168 -c508"


