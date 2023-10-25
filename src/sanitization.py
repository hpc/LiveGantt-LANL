import pandas as pd


def twenty22():
    """
    Defines a series of variables linked to the column names of the format being used
    :return:
    """
    global endState, wallclockLimit, reqNodes, submit, start, end, jobId
    endState = "State"
    wallclockLimit = "Timelimit"
    reqNodes = "NNodes"
    submit = "Submit"
    start = "Start"
    end = "End"
    jobId = "JobID"


def sanitizeFile(inputfile):
    """
    Sanitize the data provided from sacct.out in order to ensure that jobs that didn't exist or didn't fit expected bounds don't interfere with chart production.
    :param inputfile: The file to convert to CSV and sanitize job data from
    :return: The sanitized dataframe
    """

    # Using 2022 fog data
    twenty22()

    df = pd.read_csv(inputfile)
    df.head()

    # TODO I don't want to overfilter this. I can eventually see which ones of these actually make sense

    # Remove jobs that were cancelled
    df[endState] = df[endState].replace('^CANCELLED by \d+', 'CANCELLED', regex=True)
    # Remove jobs that have duplicate job IDs
    sanitizing_df = df.drop_duplicates(subset=[jobId], keep="last")
    # Remove jobs that requested 0 nodes
    sanitizing_df = sanitizing_df.loc[sanitizing_df[reqNodes] != 0]
    # Remove jobs that have a wallclocklimit of 0
    sanitizing_df = sanitizing_df.loc[sanitizing_df[wallclockLimit] != 0]
    # Remove jobs with the same start & end timestamps
    sanitizing_df = sanitizing_df.loc[sanitizing_df[end] != sanitizing_df[start]]
    # Remove jobs with an unknown end state
    sanitizing_df = sanitizing_df.loc[sanitizing_df[end] != "Unknown"]
    # Remove jobs with an unknown start state
    sanitizing_df = sanitizing_df.loc[sanitizing_df[start] != "Unknown"]
    # Remove jobs with an unknown submit state
    sanitizing_df = sanitizing_df.loc[sanitizing_df[submit] != "Unknown"]
    # Remove jobs that have a null start
    sanitizing_df = sanitizing_df.loc[~sanitizing_df[start].isna()]
    # Remove jobs that have an end that is not after the start
    sanitizing_df = sanitizing_df.loc[sanitizing_df[end] > sanitizing_df[start]]

    return sanitizing_df
