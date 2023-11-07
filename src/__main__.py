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
from evalys.jobset import JobSet


# def main(argv):
def main():
    # inputpath = ""
    # try:
    #     opts, args = getopt.getopt(
    #         argv,
    #         "i",
    #         [
    #             "ipath=",
    #         ],
    #     )
    # except getopt.GetoptError:
    #     print("Option error! Please see usage below:\npython3 -m livegantt -i <inputpath>")
    #     sys.exit(2)
    # for opt, arg in opts:
    #     if opt == "-i":
    #         inputpath = arg

    # Debug option below
    inputpath = "/Users/vhafener/Repos/LiveGantt/sacct.out.snow.start=2023-10-01T00:00.no-identifiers.txt"
    # Produce the chart
    ganttLastNHours(inputpath, 72, "test.txt", "Rocinante")


def ganttLastNHours(outJobsCSV, hours, outfile, clusterName):
    """
    Plots a gantt chart for the last N hours
    :param hours: the number of hours from the most recent time entry to the first included time entry
    :param outfile: the file to write the produced chart out to
    :return:
    """
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
    print(chartStartTime)
    print(chartEndTime)
    print(eightHours.total_seconds())
    # TODO Normalize time
    # Sanitize the data from the inputfile
    df = sanitization.sanitizeFile(outJobsCSV)
    print(df)
    maxJobLen = batvis.utils.getMaxJobLen(df)
    # js = JobSet.from_df(df, resource_bounds=(0, 1489))
    # Cut the jobset
    # TODO Make sure that this cut is working as intended
    cut_js = cut_workload(df, chartStartTime - maxJobLen, chartEndTime + maxJobLen)
    totalDf = pandas.concat([cut_js["workload"], cut_js["running"], cut_js["queue"]])

    plot_gantt_df(totalDf, ProcInt(0,367), chartStartTime, chartEndTime, title="Status for cluster " + clusterName)
    # cut_js.plot(with_gantt=True, simple=True)
    matplotlib.pyplot.show()
    # matplotlib.pyplot.savefig(
    #     outfile,
    #     dpi=300,
    # )
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
                return seekLastLine(outJobsCSV, endColIndex, startColIndex, index=index-1)
        else:
            return datetime.datetime.strptime(last_line[endColIndex], '%Y-%m-%dT%H:%M:%S')

if __name__ == '__main__':
    main()
