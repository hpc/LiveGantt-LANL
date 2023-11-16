import getopt
import os
import sys
import datetime
import batvis.utils
import matplotlib
import pandas
import sanitization

from evalys.utils import cut_workload
from evalys.visu.gantt import plot_gantt_df
from procset import ProcInt

# Set the font size to prevent overlap of datestamps
matplotlib.pyplot.rcParams.update({'font.size': 6})


def main(argv):
    inputpath = ""
    timeframe = 0
    name = "None"
    count = 0
    # Parse provided arguments and set variables to their values
    try:
        opts, args = getopt.getopt(
            argv,
            "i:t:n:c:",
            [
                "ipath=",
                "timeframe=",
                "name=",
                "count=",
            ],
        )
    # If options provided are incorrect, fail out
    except getopt.GetoptError:
        print("Option error! Please see usage below:\npython3 -m livegantt -i <inputpath> -t <timeframe> -n <Cluster name> -c <Node count>")
        sys.exit(2)
    for opt, arg in opts:
        if opt == "-i":
            inputpath = arg
        elif opt in "-t":
            timeframe = int(arg)
        elif opt in "-n":
            name = arg
        elif opt in "-c":
            count = int(arg)

    # Debug options below
    # inputpath = "/Users/vhafener/Repos/LiveGantt/sacct.out.snow.start=2023-10-01T00:00.no-identifiers.txt"
    # timeframe = 36
    # name = "Snow"
    # count = 368

    # Produce the chart
    ganttLastNHours(inputpath, timeframe, name, count)

    # Cleanup workdir
    # os.remove("out.txt")
    # os.remove(inputpath)


def ganttLastNHours(outJobsCSV, hours, clusterName, clusterSize):
    """
    Plots a gantt chart for the last N hours
    :param hours: the number of hours from the most recent time entry to the first included time entry
    :param outfile: the file to write the produced chart out to
    :return:
    """
    # Print some basic information on the operating parameters
    print("Input file:\t" + outJobsCSV)
    print("Hours:\t" + str(hours))
    print("Cluster name:\t" + clusterName)
    print("Size of cluster:\t" + str(clusterSize))

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
    df = sanitization.sanitizeFile(outJobsCSV)
    maxJobLen = batvis.utils.getMaxJobLen(df)
    # Cut the jobset to the size of the window
    cut_js = cut_workload(df, chartStartTime - maxJobLen, chartEndTime + maxJobLen)
    # Reconstruct a total jobset dataframe from the output of the cut_workload function
    totalDf = pandas.concat([cut_js["workload"], cut_js["running"], cut_js["queue"]])
    # TODO Use iPython for interactivity??? Ask steve first.
    # Plot the DF
    plot_gantt_df(totalDf, ProcInt(0, clusterSize - 1), chartStartTime, chartEndTime,
                  title="Schedule for Cluster " + clusterName + " at " + chartEndTime.strftime('%H:%M:%S on %d %B, %Y'))
    # Save the figure out to a name based on the end time
    # matplotlib.pyplot.show()
    matplotlib.pyplot.savefig(
        "./" + chartEndTime.strftime('%Y-%m-%dT%H:%M:%S') + ".png",
        dpi=300,
    )
    # Close the figure
    matplotlib.pyplot.close()



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
        # seems to take under 10 milliseconds so I don't care about that level of optimization yet

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


if __name__ == '__main__':
    main(sys.argv[1:])
