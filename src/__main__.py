import getopt
import sys
import datetime

import batvis.utils
import matplotlib
import pandas
from evalys.utils import cut_workload
from evalys.visu.gantt import plot_gantt_df
from procset import ProcInt

import sanitization


def main(argv):
    inputpath = ""
    timeframe=0
    name="None"
    count=0
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
    except getopt.GetoptError:
        print("Option error! Please see usage below:\npython3 -m livegantt -i <inputpath>")
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

    # Debug option below
    # inputpath = "/Users/vhafener/Repos/LiveGantt/sacct.out.snow.start=2023-10-01T00:00.no-identifiers.txt"
    # Produce the chart
    ganttLastNHours(inputpath, timeframe, name, count)


def ganttLastNHours(outJobsCSV, hours, clusterName, clusterSize):
    """
    Plots a gantt chart for the last N hours
    :param hours: the number of hours from the most recent time entry to the first included time entry
    :param outfile: the file to write the produced chart out to
    :return:
    """
    print("Input file:\t" + outJobsCSV)
    print("Hours:\t" + str(hours))
    print("Cluster name:\t"+clusterName)
    print("Size of cluster:\t" + str(clusterSize))
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

    chartEndTime = seekLastLine(outJobsCSV, endColIndex, startColIndex, -1)

    # Normalize time here
    eightHours = datetime.timedelta(hours=hours)
    chartStartTime = chartEndTime - eightHours
    print("Start of chart window:\t"+str(chartStartTime))
    print("End of chart window:\t"+str(chartEndTime))
    # Sanitize the data from the inputfile
    df = sanitization.sanitizeFile(outJobsCSV)
    maxJobLen = batvis.utils.getMaxJobLen(df)
    # js = JobSet.from_df(df, resource_bounds=(0, 1489))
    # Cut the jobset
    cut_js = cut_workload(df, chartStartTime - maxJobLen, chartEndTime + maxJobLen)
    totalDf = pandas.concat([cut_js["workload"], cut_js["running"], cut_js["queue"]])

    plot_gantt_df(totalDf, ProcInt(0, clusterSize - 1), chartStartTime, chartEndTime,
                  title="Schedule for Cluster " + clusterName + " at " + chartEndTime.strftime('%H:%M:%S on %d %B, %Y'))
    # cut_js.plot(with_gantt=True, simple=True)
    # matplotlib.pyplot.show()
    # TODO Scale this better
    matplotlib.pyplot.savefig(
        "./"+chartEndTime.strftime('%Y-%m-%dT%H:%M:%S')+".png",
        dpi=300,
    )
    matplotlib.pyplot.close()


def seekLastLine(outJobsCSV, endColIndex, startColIndex, index):
    with open(outJobsCSV) as f:
        last_line = f.readlines()[index].split(
            ",")  # This could be done by seeking backwards from the end of the file as a binary, but for now this
        # seems to take under 10 milliseconds so I don't care about that level of optimization yet

        if last_line[endColIndex] == "Unknown":
            if last_line[startColIndex] != "Unknown":
                return datetime.datetime.strptime(last_line[startColIndex], '%Y-%m-%dT%H:%M:%S')
            else:
                return seekLastLine(outJobsCSV, endColIndex, startColIndex, index=index - 1)
        else:
            return datetime.datetime.strptime(last_line[endColIndex], '%Y-%m-%dT%H:%M:%S')


if __name__ == '__main__':
    main(sys.argv[1:])
