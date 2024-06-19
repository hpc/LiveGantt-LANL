# LiveGantt

Copyright Notice
----------------
LiveGantt authored by Vivian Hafener. Copyright © 2024 Triad National Security, LLC.
Release Approved Under O#4697 - LANL-contributed enhancements to BatSim toolstack.

## Local Installation
1. First, clone https://github.com/hpc/Evalys-LANL and run:
```python3 -m pip install ./evalys``` It is critical that you install from this version locally instead of the evalys pip package.
2. Then, clone this repo.
3. After cloning, install everything in `requirements.txt`, except for evalys (you've already installed the modified version of this component)
4. There are two ways to run the program from here.

## Collecting sacct data from your cluster
1. Copy collectSacctDB.sh a node to your cluster that runs Slurm, and make it executable
2. Run it, and take note of the file that it's created. 
3. Copy that file back to the system you intend to run LiveGantt on. This will be your 'inputpath'. 
4. If you run into trouble with sanitization.py or during primary LiveGantt operation due to data formatting, sanitization.py is where all of the incoming data is formatted into something readable by Evalys and LiveGantt. 

## Traditional operation - single- or multi-cluster operation
Edit the parameters in lines 130-150 of `src/__main__.py` to match the clusters you want to generate charts for, and the proper paths for the input files and such. Launch by running:
```python3 src/__main__.py```

Here's a closer look at the visualization parameters for a cluster:

```

# Roci
inputpath = "/Users/vhafener/Repos/LiveGantt/sacct.out.rocinante.start=2023-12-01T00:00.no-identifiers.txt"
outputpath = "/Users/vhafener/Repos/LiveGantt/Charts/"
timeframe = 1440
count = 300
cache = True

clear_cache = False
coloration_set = ["power"", "exitstate", "partition",
                  "wait"]  # Options are "default", "partition", "wait", "power", "exitstate", "wasted_time". If you leave this blank, it will only compute a utilization line chart.
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

<!-- ## Command line operation - single-cluster operation
You can call LiveGantt via the command line using a wide range of arguments. 

```python3 src/__main__.py -i/Users/vhafener/Repos/LiveGantt/sacct.out.rocinante.start=2024-01-01T00:00.no-identifiers.txt -o/Users/vhafener/Repos/LiveGantt/Charts/ -t168 -c508 -kFalse```
This line launches LiveGantt with the inputfile(`-i`) "/Users/vhafener/Repos/LiveGantt/sacct.out.rocinante.start=2024-01-01T00:00.no-identifiers.txt" the output folder location(`-o`) "/Users/vhafener/Repos/LiveGantt/Charts/" the timespan (`-t`) of 168 hours, the node count (`-c`) of 508, and cache (`-k`) disabled "False"   -->

## Automation of LiveGantt runs using Slurm
LiveGantt can also be launched using Slurm and the scrontab functionality. This functionality is in progress and has not been completed or finalized/tested yet. 

The file `./SBATCH_LIVEGANTT.sh` is a sample SBATCH script which should generate charts for the system it is run on for the past week. The goal of this code is to be launched weekly via scontab. 

Build.sh builds and exports a container from the Dockerfile to an oci image located in the oci_images folder. Eventually this will be moved to Charliecloud.

## Adding a New Data Field From Slurm
1. Add field to the FIELDS line of collectSacctDB.sh. Ensure that field length is such that data does not get cropped short.
2. Add field to sanitization.py. The DB file created through collectSacctDB.sh is imported to sanitizing_df, then has a number of operations done to it, then is moved to formatted_df. 
3. Usually you will have to do some formatting on the data for it to work smoothly. Sanitization.py contains many examples of ways to apply different formattings to data to make it work better.
4. The last line of sanitization.py controls which fields are then returned back to the main program after sanitization. Ensure that your field is added here.
## Adding a New Coloration Method
### Components of a Coloration Method
The following section steps through the places where code must be added in order to add a new coloration method, ordered in the order in which they are called during program execution. 
#### Column Definitions
By default, Evalys will include the columns below in the dataframes that are used to create a chart. This is used to minimize the amount of data that is being passed through steps that can be run thousands of times when plotting a chart. If you need additional columns for your coloration method, you'll have to define it manually.

```
COLUMNS = (
        "jobID",
        "allocated_resources",
        "execution_time",
        "finish_time",
        "starting_time",
        "submission_time",
        "purpose",
    )
```

The first element of adding a new coloration method is defining a column_mapping within the buildDf function, located in the Evalys repo, at evalys/viso/gantt.py. Within the definition for the column_mapping dictionary, add another line, following the existing format. Here's a breakdown of a sample new line:
```
"newmethod": self.COLUMNS + ("newdatapoint",),
```
Adding this line to the end of the dictionary will create a mapping between the coloration method "newmethod", and will append the column "newdatapoint" to that pre-set list of columns. You can define as many additional columns as you want. 
#### Coloration Middleman
The coloration middleman is a function that provides the _plot_job function with the function that will be used to colorize the job rectangles. It uses multiple dictionaries to match the provided coloration method and edge coloration method against a set of defined functions that provide the coloration method for each of those methods. 

To add a new method, add a line to the end of the coloration_methods dictionary, located in the Evalys repo at evalys/visu/gantt, within the GanttVisualization class definition, in the _coloration_middleman function. Below is an example of how this could look for our new method:

```
"newmethod": self._return_newmethod_rectangle,
```
#### Returning a Colorized Rectangle
Next you will need to define a method that returns a rectangle that has been colorized based on some criteria. Here's where there's a number of different ways to implement this. I'll provide several examples from the codebase to illustrate different ways of doing this. 

###### Default
By default, Evalys colorizes jobs using a round robin method that results in a roughly random coloration of jobs, with decent enough contrast between adjacent jobs. This round robin coloration is called from the `self.colorer` portion of the return line. 
```
def _return_default_rectangle(self, job, x0, duration, height, itv, num_projects=None, num_users=None,
                                  num_top_users=None, partition_count=None, edge_color="black"):
        return self._create_rectangle(job, x0, duration, height, itv, self.colorer, edge_color=edge_color)
```
Here's a closer look at that round robin coloration function:
```
@staticmethod
    def round_robin_map(job, palette):
        """
        :return: a color to apply to :job based on :palette
        """
        return palette[job["uniq_num"] % len(palette)]
```
This takes the job that it is provided and the palette that has been defined and returns the color to apply to the job based on the parameters. 

###### Partition Rectangle
This snippet generates a palette based on the number of partitions on the cluster, then returns a rectangle colored based on the partition, with the opacity (alpha) set based on the job's project, or account.
```
def _return_partition_rectangle(self, job, x0, duration, height, itv, num_projects=None, num_users=None,
                                    num_top_users=None, partition_count=None, edge_color="black"):
        global PALETTE_USED
        PALETTE_USED = core.generate_palette(partition_count)
        return self._create_rectangle(job, x0, duration, height, itv, self.partition_color_map,
                                      alpha=job["normalized_account"], edge_color=edge_color,
                                      palette=PALETTE_USED)
```
###### Exitstate Rectangle
The following code parses the "success" field of the job and colorizes the rectangle based on the value of that field.

```
 def _return_success_rectangle(self, job, x0, duration, height, itv, num_projects=None, num_users=None,
                                  num_top_users=None, partition_count=None, edge_color="black"):
        edge_color="black"
        if "COMPLETED" in job["success"]:
            return self._create_rectangle(job, x0, duration, height, itv, lambda _: "#35F500", facecolor="#35F500",
                                          edge_color=edge_color)

        elif "TIMEOUT" in job["success"]:
            return self._create_rectangle(job, x0, duration, height, itv, lambda _: "#f6acc9", facecolor="#f6acc9",
                                          edge_color=edge_color)

        elif "CANCELLED" in job["success"]:
            return self._create_rectangle(job, x0, duration, height, itv, lambda _: "#FF8604", facecolor="#FF8604",
                                          edge_color=edge_color)

        elif "FAILED" in job["success"]:
            return self._create_rectangle(job, x0, duration, height, itv, lambda _: "#FF0000", facecolor="#FF0000",
                                          edge_color=edge_color)

        elif "NODE_FAIL" in job["success"]:
            return self._create_rectangle(job, x0, duration, height, itv, lambda _: "#FFF700", facecolor="#FFF000",
                                          edge_color=edge_color)

        elif "RUNNING" in job["success"]:
            return self._create_rectangle(job, x0, duration, height, itv, lambda _: "#AE00FF", facecolor="#AE00FF",
                                          edge_color=edge_color)

        elif "OUT_OF_MEMORY" in job["success"]:
            return self._create_rectangle(job, x0, duration, height, itv, lambda _: "#0019FF", facecolor="#0019FF",
                                          edge_color=edge_color)
```
###### Power Rectangle
The following code utilizes a red-green palette and returns a matching value based on the job's normalized power per node hour field.
```
 def _return_power_rectangle(self, job, x0, duration, height, itv, num_projects=None, num_users=None,
                               num_top_users=None, partition_count=None, edge_color="black"):
        global PALETTE_USED
        PALETTE_USED = core.generate_redgreen_palette(100)
        return self._create_rectangle(job, x0, duration, height, itv, self.power_color_map, edge_color=edge_color, palette=PALETTE_USED)
```

#### Contact Us
Do you use this tool on your cluster? Do you want to use it but it's not working for you? Email me! Send me your cluster pictures! vhafener@lanl.gov
