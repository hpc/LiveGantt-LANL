import getopt
import sys
import datetime

import matplotlib
from evalys.utils import cut_workload
from evalys.visu.gantt import plot_gantt_df

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
    inputpath = "/Users/vhafener/Repos/fog_analysis/slurm_outfiles/roci/sacct.out.rocinante.start=2019-12-01T00:00.no" \
                "-identifiers.csv"
    # Produce the chart
    ganttLastNHours(inputpath, 8, "test.txt", "Rocinante")


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
            if 'End' in elem:
                indices.append(i)
        startColIndex = indices[0]
        for i, elem in enumerate(header):
            if 'Start' in elem:
                indices.append(i)
        endColIndex = indices[1]

    with open(outJobsCSV) as f:
        last_line = f.readlines()[-1].split(
            ",")  # This could be done by seeking backwards from the end of the file as a binary, but for now this
        # seems to take under 10 milliseconds so I don't care about that level of optimization yet

        if last_line[endColIndex] == "Unknown":
            chartEndTime = datetime.datetime.strptime(last_line[startColIndex], '%Y-%m-%dT%H:%M:%S')
        else:
            chartEndTime = datetime.datetime.strptime(last_line[endColIndex], '%Y-%m-%dT%H:%M:%S')
    # TODO Hmmmmmmm this time format will be somewhat annoying. My programs expect seconds since start, but that is not a number that I have. Should I convert to seconds then set t=0 to the amount of time backwards from 0?

    # Normalize time here
    eightHours = datetime.timedelta(hours=hours)
    chartStartTime = chartEndTime- eightHours
    print(chartStartTime)
    print(chartEndTime)
    # TODO Normalize time
    # Sanitize the data from the inputfile
    df = sanitization.sanitizeFile(outJobsCSV)
    print(df)
    maxJobLen = getMaxJobLen(df)
    js = JobSet.from_df(df, resource_bounds=None)
    # Cut the jobset
    cut_js = cut_workload(js.df, chartStartTime - maxJobLen, chartEndTime + maxJobLen)

    plot_gantt_df(cut_js, cut_js.res_bounds, chartStartTime, chartEndTime, title="Status for cluster "+clusterName)
    js.plot(with_gantt=True, simple=True)
    matplotlib.pyplot.show()
    # matplotlib.pyplot.savefig(
    #     outfile,
    #     dpi=300,
    # )
    matplotlib.pyplot.close()

def getMaxJobLen(totaldf):
    """
    Gets the length of the longest job in a dataframe

    :returns: the length of the longest job in the df
    """
    # TODO Does this even work? Test!
    maxJobLen = (totaldf["End"]-totaldf["Start"]).max() # I had to substitute execution time for End - Start. If I wind up having to just do that in preprocessing, then switch this back to keep it simple
    # print("Maximum job length parsed as: " + str(maxJobLen))
    return maxJobLen

if __name__ == '__main__':
    main()
