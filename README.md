# LiveViz 

### Local Installation
1. First, clone https://gitlab.newmexicoconsortium.org/lanl-ccu/evalys and run:
```python3 -m pip install ./evalys```
2. Then, clone this repo.
3. After cloning, install everything in `requirements.txt`, except for evalys (you've already installed the modified version of this component)
4. There are two ways to run the program from here.
## Command line operation - single-cluster operation
You can call LiveGantt via the command line using a wide range of arguments. 

```python3 src/__main__.py -i/Users/vhafener/Repos/LiveGantt/sacct.out.rocinante.start=2024-01-01T00:00.no-identifiers.txt -o/Users/vhafener/Repos/LiveGantt/Charts/ -t168 -c508 -kFalse```
This line launches LiveGantt with the inputfile(`-i`) "/Users/vhafener/Repos/LiveGantt/sacct.out.rocinante.start=2024-01-01T00:00.no-identifiers.txt" the output folder location(`-o`) "/Users/vhafener/Repos/LiveGantt/Charts/" the timespan (`-t`) of 168 hours, the node count (`-c`) of 508, and cache (`-k`) disabled "False"  

## Traditional operation - single- or multi-cluster operation
Edit the parameters in lines 130-150 of `src/__main__.py` to match the clusters you want to generate charts for, and the proper paths for the input files and such. Launch by running:
```python3 src/__main__.py```

Here's a closer look at the visualization parameters for a cluster:

```

# Roci
inputpath = "/Users/vhafener/Repos/LiveGantt/sacct.out.rocinante.start=2023-12-01T00:00.no-identifiers.txt"
outputpath = "/Users/vhafener/Repos/LiveGantt/Charts/"
timeframe = 1440
count = 508
cache = True

clear_cache = False
coloration_set = ["power", "project", "exitstate", "partition",
                  "wait"]  # Options are "default", "project", "user", "user_top_20", "sched", "wait", and "dependency"
vizset.append((inputpath, outputpath, timeframe, count, cache, clear_cache, coloration_set))

```

* ```inputpath``` - this line defines the absolute path of the file that you've generated with `collectSacctDB.sh`
* ```outputpath``` - this line defines the absolute path of the folder in which the output charts will be placed
* ```timeframe``` - this line defines the timeframe in hours to visualize. This is counted backwards from the time at the end of the data collected in `inputpath`
* ```count``` - node count of the cluster
* ```cache``` - if a cache for this inputpath does not exist write to it. If it does exist, load the cached inputfile. This saves a lot of time from the sanitization process
* ```clear cache``` - delete the existing cache for this inputpath
* ```coloration_set``` - this field contains a set of strings that define which coloration schemes to create charts for
* The final line is necessary to append this set of values to the overall vizset. Because of this structure, you can add as many clusters as you want

## Automation of LiveGantt runs using Slurm
LiveGantt can also be launched using Slurm and the scrontab functionality. This functionality is in progress and has not been completed or finalized/tested yet. 

The file `./SBATCH_LIVEGANTT.sh` is a sample SBATCH script which should generate charts for the system it is run on for the past week. The goal of this code is to be launched weekly via scontab. 

Build.sh builds and exports a container from the Dockerfile to an oci image located in the oci_images folder. 