import datetime
import getopt
import hashlib
import os
import sys
import time

import batvis.utils
import matplotlib.pyplot as plt
import pandas
import pandas as pd

import sanitization

from evalys.utils import cut_workload
from evalys.visu.gantt import plot_gantt_df
from procset import ProcInt

# Set the font size to prevent overlap of datestamps
plt.rcParams.update({'font.size': 6})


def main(argv):
    # TODO UPDATE CLI OPTIONS
    inputpath = ""
    timeframe = 0
    count = 0
    # Parse provided arguments and set variables to their values
    try:
        opts, args = getopt.getopt(
            argv,
            "i:t:c:",
            [
                "ipath=",
                "timeframe=",
                "count=",
            ],
        )
    # If options provided are incorrect, fail out
    except getopt.GetoptError:
        print(
            "Option error! Please see usage below:\npython3 -m livegantt -i <inputpath> -t <timeframe> -c <Node count>")
        sys.exit(2)
    for opt, arg in opts:
        if opt == "-i":
            inputpath = arg
        elif opt in "-t":
            timeframe = int(arg)
        elif opt in "-c":
            count = int(arg)

    # Debug options below

    # Chicoma
    # inputpath = "/Users/vhafener/Repos/LiveGantt/sacct.out.chicoma.start=2023-12-01T00:00.no-identifiers.txt"
    # timeframe = 72
    # count = 1792
    # cache = True
    # clear_cache = False
    # coloration = "dependency"  # Options are "default", "project", "user", "user_top_20", and "dependency"

    # # TODO Implement width for high-res wide charts

    # Snow
    inputpath = "/Users/vhafener/Repos/LiveGantt/sacct.out.snow.start=2023-12-01T00:00.no-identifiers.txt"
    timeframe = 36
    count = 368
    cache = True
    clear_cache = False
    coloration = "dependency"  # Options are "default", "project", "user", "user_top_20", and "dependency"


    # Fog
    # inputpath = "/Users/vhafener/Repos/LiveGantt/sacct.out.fog.start=2023-10-01T00:00.no-identifiers.txt"
    # timeframe = 142
    # count=32
    # cache = True
    # clear_cache = False
    # coloration = "project"  # Options are "default", "project", "user", "user_top_20", and "dependency"


    # Produce the chart
    ganttLastNHours(inputpath, timeframe, count, cache, clear_cache, coloration)

    # Cleanup workdir
    # os.remove("out.txt")
    # os.remove(inputpath)


def ganttLastNHours(outJobsCSV, hours, clusterSize, cache=False, clear_cache=False, coloration="default"):
    """
    Plots a gantt chart for the last N hours
    :param hours: the number of hours from the most recent time entry to the first included time entry
    :param outfile: the file to write the produced chart out to
    :return:
    """
    clusterName = outJobsCSV.split(".")[2]
    # Print some basic information on the operating parameters
    print("\nLiveGantt Initialized!\n")
    print("Input file:\t" + outJobsCSV)
    print("Hours:\t" + str(hours))
    print("Cluster name:\t" + clusterName)
    print("Size of cluster:\t" + str(clusterSize))
    print("\nDetermining chart window...")
    # Open the output file and figure out what columns hold 'Start' and 'End' time values
    with open(outJobsCSV) as f:
        header = f.readlines()[0].split(",")
        indices = []
        for i, elem in enumerate(header):
            if 'Start' in elem:
                indices.append(i)
        startColIndex = indices[0]
        for i, elem in enumerate(header):
            if 'End' in elem:
                indices.append(i)
        endColIndex = indices[1]

    # Determine chart start and end times
    # Set the end time of the chart to the value determined by seekLastLine
    chartEndTime = seekLastLine(outJobsCSV, endColIndex, startColIndex, -1)
    eightHours = datetime.timedelta(hours=hours)
    chartStartTime = chartEndTime - eightHours

    # Print dates and times of chart start & end
    print("Start of chart window:\t" + str(chartStartTime))
    print("End of chart window:\t" + str(chartEndTime))
    # Sanitize the data from the inputfile

    cache_name = outJobsCSV + "_sanitized_cache.csv"
    if clear_cache:
        os.remove(cache_name)

    if cache is True:
        input_file_hash = calculate_sha256(outJobsCSV)
        if os.path.isfile(cache_name):
            print("Cache exists! Hashing ...")
            cache_hash = calculate_sha256(cache_name.removesuffix("_sanitized_cache.csv"))
            if input_file_hash == cache_hash:
                print("Cache valid! Loading df from cache...")
                start_time_task = time.time()
                df = pd.read_csv(cache_name)
                df = sanitization.cache_column_typing(df)
                end_time_task = time.time()
                duration_task = end_time_task - start_time_task
                print("Cache loaded in " + str(duration_task) + "s")

            else:
                print("Cache invalid!")
                df = sanitization.sanitizeFile(outJobsCSV)
                df.to_csv(cache_name)
        else:
            df = sanitization.sanitizeFile(outJobsCSV)
            df.to_csv(cache_name)

    else:
        df = sanitization.sanitizeFile(outJobsCSV)

    maxJobLen = batvis.utils.getMaxJobLen(df)
    # Cut the jobset to the size of the window
    cut_js = cut_workload(df, chartStartTime - maxJobLen, chartEndTime + maxJobLen)
    # Reconstruct a total jobset dataframe from the output of the cut_workload function
    totalDf = pandas.concat([cut_js["workload"], cut_js["running"], cut_js["queue"]])
    # Plot the DF
    # matchlist = ["chicoma", "rocinante"]
    # TODO Dependency isn't easy to read - use a more visible highlight
    project_count = totalDf["account"].unique().size
    user_count = totalDf["user"].unique().max()
    user_top_20_count = totalDf["user_id"].unique().size
    if coloration == "project" and project_count is None:
        print("Dataset must contain more than zero projects! Fix or change coloration parameter.")
        sys.exit(2)
    elif coloration == "user" and user_count is None:
        print("Dataset must contain more than zero users! Fix or change coloration parameter.!")
        sys.exit(2)
    elif coloration == "user_top_20" and user_top_20_count is None:
        print("Dataset must contain more than zero top_20_users! Fix or change coloration parameter.!")
        sys.exit(2)
    if clusterName != "chicoma" and clusterName != "rocinante":
        plot_gantt_df(totalDf, ProcInt(0, clusterSize - 1), chartStartTime, chartEndTime,
                      title="Schedule for cluster " + clusterName + " at " + chartEndTime.strftime(
                          '%H:%M:%S on %d %B, %Y'), dimensions=setDimensions(nodeCount=clusterSize),
                      colorationMethod=coloration, num_projects=project_count, num_users=user_count, num_top_users=8,
                      resvSet=parse_reservation_set(totalDf))
    else:
        plot_gantt_df(totalDf, ProcInt(1000, clusterSize + 1000 - 1), chartStartTime, chartEndTime,
                      title="Schedule for cluster " + clusterName + " at " + chartEndTime.strftime(
                          '%H:%M:%S on %d %B, %Y'), dimensions=setDimensions(nodeCount=clusterSize),
                      colorationMethod=coloration, num_projects=project_count, num_users=user_count, num_top_users=8,
                      resvSet=parse_reservation_set(totalDf))
    # Save the figure out to a name based on the end time
    plt.savefig(
        "./" + chartStartTime.strftime('%Y-%m-%dT%H:%M:%S') + "-" + chartEndTime.strftime(
            '%Y-%m-%dT%H:%M:%S') + "_" + coloration + ".png",
        dpi=300,
    )
    # Close the figure
    plt.close()


def parse_reservation_set(df):
    reservation_set = []
    for index, row in df.iterrows():
        if row["purpose"] == "reservation":
            reservation_set.append(row)
    return reservation_set


def calculate_sha256(filename):
    sha256_hash = hashlib.sha256()
    sha256_hash.update(filename.encode('utf-8'))
    return sha256_hash.hexdigest()


def seekLastLine(outJobsCSV, endColIndex, startColIndex, index):
    """
    Find the last line in the CSV file that contains a job with a valid Start or End time and return that time value
    :param outJobsCSV: The CSV file with jobs in which to search
    :param endColIndex: The index of the column containing the 'End' Values
    :param startColIndex: The index of the column containing the 'Start' values
    :param index: Index used to keep track of level of recursion. Program recurses through the last & next-last line until it finds one that has a valid time value.
    :return: In the specific time format used, the latest time value in the entire CSV file.
    """
    with open(outJobsCSV) as f:
        last_line = f.readlines()[index].split(
            ",")  # This could be done by seeking backwards from the end of the file as a binary, but for now this
        # seems to take under 10 milliseconds, so I don't care about that level of optimization yet

        # If the last job hasn't ended yet
        if last_line[endColIndex] == "Unknown":
            # But if that job has started, return a time value
            if last_line[startColIndex] != "Unknown":
                return datetime.datetime.strptime(last_line[startColIndex], '%Y-%m-%dT%H:%M:%S')
            # But if it hasn't started yet, recurse to the previous job
            else:
                return seekLastLine(outJobsCSV, endColIndex, startColIndex, index=index - 1)
        # If the last job has ended, return the time value
        else:
            return datetime.datetime.strptime(last_line[endColIndex], '%Y-%m-%dT%H:%M:%S')


def setDimensions(nodeCount=0):
    threshold_a = 48
    threshold_b = 600
    threshold_c = 1500
    if nodeCount <= threshold_a:
        return (12, 8)
    elif nodeCount > threshold_a and nodeCount <= threshold_b:
        # TODO Smallscalar
        return (12, 10)
    elif nodeCount > threshold_b and nodeCount <= threshold_c:
        # TODO Medscalar
        return (12, 18)
    elif nodeCount > threshold_c:
        # TODO Largescalar
        return (12, 35)


if __name__ == '__main__':
    main(sys.argv[1:])
