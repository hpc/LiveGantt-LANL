import matplotlib
from batvis.utils import getMaxJobLen
from evalys.utils import cut_workload
from evalys.visu.gantt import plot_gantt_df

import sanitization
from evalys.jobset import JobSet


def main():
    # Define the file to pull data from TODO, this will connect back to the shell script
    inputpath = "/Users/vhafener/Repos/fog_analysis/slurm_outfiles/roci/sacct.out.rocinante.start=2019-12-01T00:00.no" \
                "-identifiers.csv"
    # Produce the chart
    ganttLastNHours(inputpath, 8, "test.txt")


def ganttLastNHours(outJobsCSV, hours, outfile):
    """
    Plots a gantt chart for the last N hours
    :param hours: the number of hours from the most recent time entry to the first included time entry
    :param outfile: the file to write the produced chart out to
    :return:
    """
    with open(outJobsCSV) as f:
        header = f.readlines()[0].split(",")
        print(header)
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
            chartEndTime = last_line[startColIndex]
        else:
            chartEndTime = last_line[endColIndex]

    chartStartTime = chartEndTime - hours*3600
    # Sanitize the data from the inputfile
    df = sanitization.sanitizeFile(outJobsCSV)
    # TODO I'll need to adapt this so it'll work with my col names and such
    maxJobLen = getMaxJobLen(df)
    js = JobSet.from_df(df, resource_bounds=None)
    # Cut the jobset
    cut_js = cut_workload(js.df, chartStartTime - maxJobLen, chartEndTime + maxJobLen)

    # TODO I'll need to trim the plot too
    plot_gantt_df(cut_js, cut_js.res_bounds, chartStartTime, chartEndTime, title="Status for cluster X") # TODO put actual cluster name here
    js.plot(with_gantt=True, simple=True)
    matplotlib.pyplot.show()
    # matplotlib.pyplot.savefig(
    #     outfile,
    #     dpi=300,
    # )
    matplotlib.pyplot.close()


if __name__ == '__main__':
    main()
