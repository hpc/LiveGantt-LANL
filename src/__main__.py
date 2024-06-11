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
import seaborn as sns
from evalys.jobset import JobSet

from evalys.utils import cut_workload
from evalys.visu.gantt import plot_gantt_df
from procset import ProcInt

# Set the font size to prevent overlap of datestamps
plt.rcParams.update({"font.size": 6})


def main(argv):
    inputpath = ""
    timeframe = 0
    count = 0
    cache = False
    clear_cache = False
    coloration = ["default"]
    vizset = []

    # Parse provided arguments and set variables to their values
    try:
        opts, args = getopt.getopt(
            argv,
            "i:o:t:c:k:x:h",
            [
                "ipath=",
                "opath=",
                "timeframe=",
                "count=",
                "cache=",
                "clear_cache=",
                "coloration=",
            ],
        )
    # If options provided are incorrect, fail out
    except getopt.GetoptError:
        print(
            "Option error! Please see usage below:\npython3 -m livegantt -i <inputpath> -t <timeframe> -c <Node count>"
        )
        sys.exit(2)
    for opt, arg in opts:
        if opt == "-i":
            inputpath = arg
        elif opt in "-o":
            outputpath = arg
        elif opt in "-t":
            timeframe = int(arg)
        elif opt in "-c":
            count = int(arg)
        elif opt in "-k":
            cache = bool(arg)
        elif opt in "-x":
            clear_cache = bool(arg)
        elif opt in "-h":
            coloration = str(arg)

    # Debug options below

    # Chicoma
    inputpath = "/Users/vhafener/Repos/LiveGantt/sacct.out.chicoma.start=2024-01-01T00:00.no-identifiers.txt"
    outputpath = "/Users/vhafener/Repos/LiveGantt/Charts/"
    timeframe = 3799
    count = 1792
    cache = True
    clear_cache = True
    projects_in_legend = False
    coloration_set = ["default", "wait", "partition", "sched", "power", "wasted_time"]
    # # # coloration_set = ["default", "user", "user_top_20", "sched", "wait", "partition", "dependency"]  # Options are "default", "sched", "wait", "partition", "wasted_time", "power"
    vizset.append(
        (
            inputpath,
            outputpath,
            timeframe,
            count,
            cache,
            clear_cache,
            coloration_set,
            projects_in_legend,
        )
    )
    # Snow
    # inputpath = "/Users/vhafener/Repos/LiveGantt/sacct.out.snow.start=2024-01-01T00:00.no-identifiers.txt"
    # outputpath = "/Users/vhafener/Repos/LiveGantt/Charts/"
    # timeframe = 168
    # count = 368
    # cache = True
    # clear_cache = False
    # projects_in_legend=True
    # coloration_set = ["default", "power", "sched", "wait", "partition", "exitstate"]
    # # # coloration_set = ["default", "project", "user", "user_top_20", "sched", "wait", "partition",
    # # #                   "dependency"]  # Options are "default", "project", "user", "user_top_20", "sched", "wait", "partition", and "dependency"
    # vizset.append((inputpath, outputpath, timeframe, count, cache, clear_cache, coloration_set, projects_in_legend))
    #
    # # Fog
    # inputpath = "/Users/vhafener/Repos/LiveGantt/sacct.out.fog.start=2023-10-01T00:00.no-identifiers.txt"
    # timeframe = 142
    # count=32
    # cache = True
    # clear_cache = False
    # projects_in_legend=False
    # coloration = "power"  # Options are "default", "project", "user", "user_top_20", "sched", "wait", and "dependency"
    #
    # # Roci
    # inputpath = "sacct.out.rocinante.start=2024-06-06T00:00.no-identifiers.txt"
    # outputpath = "/Users/vhafener/Repos/LiveGantt/Charts/"
    # timeframe = 72
    # count = 508
    # cache = False
    # clear_cache = False
    # projects_in_legend=True
    # coloration_set = ["default", "power", "sched", "wait", "partition", "exitstate"]  # Options are "default", "project", "user", "user_top_20", "sched", "wait", and "dependency"
    # vizset.append((inputpath, outputpath, timeframe, count, cache, clear_cache, coloration_set, projects_in_legend))
    # # Roci
    # inputpath = "/Users/vhafener/Repos/LiveGantt/sacct.out.rocinante.start=2023-12-01T00:00.no-identifiers.txt"
    # outputpath = "/Users/vhafener/Repos/LiveGantt/Charts/"
    # timeframe = 1080
    # count = 508
    # cache = True
    #
    # clear_cache = False
    # coloration_set = ["power", "project", "exitstate", "partition",
    #                   "wait"]  # Options are "default", "project", "user", "user_top_20", "sched", "wait", and "dependency"
    # vizset.append((inputpath, outputpath, timeframe, count, cache, clear_cache, coloration_set))
    # Roci
    # inputpath = "sacct.out.rocinante.start=2023-11-01T00_00.no-identifiers.txt"
    # outputpath = "/Users/vhafener/Repos/LiveGantt/Charts/"
    # timeframe = 4851
    # count = 508
    # cache = False
    # projects_in_legend = True
    # clear_cache = False
    # coloration_set = ["wait"]  # Options are "default", "project", "user", "user_top_20", "sched", "wait", and "dependency"
    # vizset.append((inputpath, outputpath, timeframe, count, cache, clear_cache, coloration_set, projects_in_legend))

    # Trinitite
    # inputpath = "/Users/vhafener/Repos/LiveGantt/sacct.out.trinitite.start=2024-01-01T00:00.no-identifiers.txt"
    # outputpath = "/Users/vhafener/Repos/LiveGantt/Charts/"
    # timeframe = 800
    # count = 200
    # cache = True
    # clear_cache = False
    # projects_in_legend=True
    # coloration_set = ["default", "power", "sched", "wait", "partition", "exitstate"]  # Options are "default", "project", "user", "user_top_20", "sched", "wait", and "dependency"
    # vizset.append((inputpath, outputpath, timeframe, count, cache, clear_cache, coloration_set, projects_in_legend))

    # Produce the chart
    for set in vizset:
        ganttLastNHours(set[0], set[1], set[2], set[3], set[4], set[5], set[6], set[7])

    # Cleanup workdir
    # os.remove("out.txt")
    # os.remove(inputpath)


def ganttLastNHours(
    outJobsCSV,
    outputpath,
    hours,
    clusterSize,
    cache=False,
    clear_cache=False,
    coloration_set=["default"],
    project_in_legend=True,
):
    """
    Plots a gantt chart for the last N hours
    :param hours: the number of hours from the most recent time entry to the first included time entry
    :param outputpath: the file to write the produced chart out to
    :return:
    """
    clusterName = outJobsCSV.split(".")[2]
    if outputpath is None:
        outputpath = (
            "Charts_for_"
            + clusterName
            + "_generated_"
            + datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        )
    else:
        outputpath = (
            outputpath
            + "/"
            + "Charts_for_"
            + clusterName
            + "_generated_"
            + datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        )
    check_output_dir_validity(outputpath)
    # Print some basic information on the operating parameters
    chartEndTime, chartStartTime = initialization(
        clusterName, clusterSize, hours, outJobsCSV
    )

    # Sanitize the data from the inputfile
    df = check_cache_and_return_df(cache, clear_cache, outJobsCSV)

    maxJobLen = batvis.utils.getMaxJobLen(df)
    # Cut the jobset to the size of the window
    cut_js = cut_workload(df, chartStartTime - maxJobLen, chartEndTime + maxJobLen)
    # Reconstruct a total jobset dataframe from the output of the cut_workload function
    totalDf = pandas.concat([cut_js["workload"], cut_js["running"], cut_js["queue"]])
    totalDf, user_top_20_count = calculate_top_N(totalDf)

    edgeMethod = "default"

    project_count = totalDf["account"].unique().size
    user_count = totalDf["user"].unique().max()
    partition_count = totalDf["partition"].unique().size

    for coloration in coloration_set:
        terminate_if_conditions_not_met(
            coloration, project_count, user_count, user_top_20_count
        )
        if clusterName != "chicoma" and clusterName != "rocinante":
            plot_gantt_df(
                totalDf,
                ProcInt(0, clusterSize - 1),
                chartStartTime,
                chartEndTime,
                title="Schedule for cluster "
                + clusterName
                + " at "
                + chartEndTime.strftime("%H:%M:%S on %d %B, %Y"),
                dimensions=setDimensions(nodeCount=clusterSize, hours=hours),
                colorationMethod=coloration,
                num_projects=project_count,
                num_users=user_count,
                num_top_users=user_top_20_count,
                resvSet=parse_reservation_set(totalDf),
                partition_count=partition_count,
                edgeMethod=edgeMethod,
                project_in_legend=project_in_legend,
            )
        else:
            plot_gantt_df(
                totalDf,
                ProcInt(1000, clusterSize + 1000 - 1),
                chartStartTime,
                chartEndTime,
                title="Schedule for cluster "
                + clusterName
                + " at "
                + chartEndTime.strftime("%H:%M:%S on %d %B, %Y"),
                dimensions=setDimensions(nodeCount=clusterSize, hours=hours),
                colorationMethod=coloration,
                num_projects=project_count,
                num_users=user_count,
                num_top_users=user_top_20_count,
                resvSet=parse_reservation_set(totalDf),
                partition_count=partition_count,
                edgeMethod=edgeMethod,
                project_in_legend=project_in_legend,
            )
        # Save the figure out to a name based on the end time
        if coloration == "partition":
            dpi = 800
        else:
            dpi = 500

        plt.xlabel("Time")
        plt.ylabel("Node ID")

        plt.tight_layout()
        if coloration == "exitstate":
            sns.rugplot(
                data=totalDf[totalDf["failedNode"].notnull()],
                x="starting_time",
                y="failedNode",
                color="r",
            )

        plt.savefig(
            outputpath
            + "/"
            + chartStartTime.strftime("%Y-%m-%dT%H:%M:%S")
            + "-"
            + chartEndTime.strftime("%Y-%m-%dT%H:%M:%S")
            + "_"
            + coloration
            + ".png",
            dpi=dpi,
        )
        # Close the figure
        plt.close()
    utilization = True
    if utilization:
        for index, row in totalDf.iterrows():
            if row["purpose"] == "reservation":
                totalDf.drop(labels=index, axis=0, inplace=True)
        totalJS = JobSet.from_df(totalDf)
        totalJS.plot(
            with_gantt=False,
            windowStartTime=chartStartTime,
            windowFinishTime=chartEndTime,
            with_details=False,
        )
        plt.savefig(
            outputpath
            + "/"
            + chartStartTime.strftime("%Y-%m-%dT%H:%M:%S")
            + "-"
            + chartEndTime.strftime("%Y-%m-%dT%H:%M:%S")
            + "_"
            + "utilization"
            + ".png",
            dpi=dpi,
        )
        # Close the figure
        plt.close()


def terminate_if_conditions_not_met(
    coloration, project_count, user_count, user_top_20_count
):
    """
    This function checks that the requisite parameters are set for the given coloration method.
    :param coloration:
    :param project_count:
    :param user_count:
    :param user_top_20_count:
    :return:
    """
    if coloration == "project" and project_count is None:
        print(
            "Dataset must contain more than zero projects! Fix or change coloration parameter."
        )
        sys.exit(2)
    elif coloration == "user" and user_count is None:
        print(
            "Dataset must contain more than zero users! Fix or change coloration parameter.!"
        )
        sys.exit(2)
    elif coloration == "user_top_20" and user_top_20_count is None:
        print(
            "Dataset must contain more than zero top_20_users! Fix or change coloration parameter.!"
        )
        sys.exit(2)


def check_output_dir_validity(outputpath):
    """
    Checks if the outputdir is a valid output point, if not it creates it.
    :param outputpath:
    :return:
    """
    if not os.path.isdir(outputpath) and not os.path.isfile(outputpath):
        os.mkdir(outputpath)


def initialization(clusterName, clusterSize, hours, outJobsCSV):
    """
    Returns the end and start of the chart window
    :param clusterName: Name of the cluster
    :param clusterSize: Size of the cluster
    :param hours: Size of the chart window in hours
    :param outJobsCSV: The path to the output file
    :return: the end and start times of the chart window
    """
    print("\nLiveGantt Initialized!\n")
    print("Input file:\t" + outJobsCSV)
    print("Hours:\t" + str(hours))
    print("Cluster name:\t" + clusterName)
    print("Size of cluster:\t" + str(clusterSize))
    print("\nDetermining chart window...")
    # Open the output file and figure out what columns hold 'Start' and 'End' time values
    endColIndex, startColIndex = parse_start_and_end(outJobsCSV)
    # Determine chart start and end times
    # Set the end time of the chart to the value determined by seekLastLine
    chartEndTime = seekLastLine(outJobsCSV, endColIndex, startColIndex, -1)
    eightHours = datetime.timedelta(hours=hours)
    chartStartTime = chartEndTime - eightHours
    # Print dates and times of chart start & end
    print("Start of chart window:\t" + str(chartStartTime))
    print("End of chart window:\t" + str(chartEndTime))
    return chartEndTime, chartStartTime


def check_cache_and_return_df(cache, clear_cache, outJobsCSV):
    """
    Checks for an existing cache for the outputfile, if one is found, return a dataframe loaded from cache.
    :param cache: boolean value indicating whether to check for a cache
    :param clear_cache: boolean value indicating whether to clear any existing cache
    :param outJobsCSV: the path of the target file
    :return: Return a dataframe containing the sanitized file output, either from cache or from a new cache.
    """
    cache_name = outJobsCSV + "_sanitized_cache.csv"
    if clear_cache:
        if os.path.isfile(cache_name):
            os.remove(cache_name)
            print("Old cache removed!")
        else:
            print("No cache file found!")
    if cache is True:
        input_file_hash = calculate_sha256(outJobsCSV)
        if os.path.isfile(cache_name):
            print("Cache exists! Hashing ...")
            cache_hash = calculate_sha256(
                cache_name.removesuffix("_sanitized_cache.csv")
            )
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
                print("Wrote new cache!")

        else:
            df = sanitization.sanitizeFile(outJobsCSV)
            df.to_csv(cache_name)
            print("Wrote new cache!")

    else:
        df = sanitization.sanitizeFile(outJobsCSV)
    return df


def parse_start_and_end(outJobsCSV):
    """
    Parses the column indexes the 'Start' and 'End' dataframe columns, as these indexes are unknown and possibly variant prior to sanitization
    :param outJobsCSV: the path of the target file
    :return: the indexes of the start and end columns of the dataframe
    """
    with open(outJobsCSV) as f:
        header = f.readlines()[0].split(",")
        indices = []
        for i, elem in enumerate(header):
            if "Start" in elem:
                indices.append(i)
        startColIndex = indices[0]
        for i, elem in enumerate(header):
            if "End" in elem:
                indices.append(i)
        endColIndex = indices[1]
    return endColIndex, startColIndex


def parse_reservation_set(df):
    """
    Generate and return a set containing all of the DF rows which are reservations.
    :param df:
    :return:
    """
    reservation_set = []
    for index, row in df.iterrows():
        if row["purpose"] == "reservation":
            reservation_set.append(row)
    return reservation_set


def calculate_top_N(formatted_df):
    """
    Used to calculate a top percentage of users in a column
    :param formatted_df:
    :return:
    """
    # Calculate the 30% most frequent usernames
    top_usernames = formatted_df["username"].value_counts().nlargest(20).index
    # Create a mapping of usernames to unique user IDs
    username_to_user_id = {
        username: i for i, username in enumerate(top_usernames, start=1)
    }
    # Apply the mapping to create a new 'user_id' column
    formatted_df["user_id"] = (
        formatted_df["username"].map(username_to_user_id).fillna(0)
    )
    # If there are any usernames not in the top 30%, set their 'user_id' to '0'
    formatted_df.loc[~formatted_df["username"].isin(top_usernames), "user_id"] = 0
    formatted_df["user_id"] = formatted_df["user_id"].astype(int)
    user_top_N_count = formatted_df["user_id"].unique().size
    return formatted_df, user_top_N_count


def calculate_sha256(filename):
    """
    Return a sha256 hash of the specified filename. This is used to check cache freshness
    :param filename: Name of file to hash
    :return: Hash
    """
    sha256_hash = hashlib.sha256()
    sha256_hash.update(filename.encode("utf-8"))
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
            ","
        )  # This could be done by seeking backwards from the end of the file as a binary, but for now this
        # seems to take under 10 milliseconds, so I don't care about that level of optimization yet

        # If the last job hasn't ended yet
        if last_line[endColIndex] == "Unknown":
            # But if that job has started, return a time value
            if last_line[startColIndex] != "Unknown":
                return datetime.datetime.strptime(
                    last_line[startColIndex], "%Y-%m-%dT%H:%M:%S"
                )
            # But if it hasn't started yet, recurse to the previous job
            else:
                return seekLastLine(
                    outJobsCSV, endColIndex, startColIndex, index=index - 1
                )
        # If the last job has ended, return the time value
        else:
            return datetime.datetime.strptime(
                last_line[endColIndex], "%Y-%m-%dT%H:%M:%S"
            )


def setDimensions(nodeCount=0, hours=24):
    """
    Set the dimensions of the plot
    :param nodeCount: Size of the cluster
    :param hours: Size of the window
    :return: Dimensions to use for the plot
    """
    threshold_a = 48
    threshold_b = 600
    threshold_c = 1500

    if nodeCount <= threshold_a:
        return hours * 0.5, 12
    elif threshold_a < nodeCount <= threshold_b:
        return hours * 0.5, 12
    elif threshold_b < nodeCount <= threshold_c:
        return hours * 0.5, 18
    elif nodeCount > threshold_c:
        return hours * 0.5, 35


if __name__ == "__main__":
    main(sys.argv[1:])
