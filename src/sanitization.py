import pandas as pd
import datetime


def twenty22():
    """
    Defines a series of variables linked to the column names of the format being used
    :return:
    """
    # TODO I don't think this will be enough. I need to make sure that when I bring in the DF that I am actually making the columns into what evalys needs them to be - try to get it to match the old sim output data
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

    # TODO I don't want to overfilter this. I can eventually see which ones of these actually make sense for live data as opposed to sim data.

    # Remove jobs that were cancelled
    df[endState] = df[endState].replace('^CANCELLED by \d+', 'CANCELLED', regex=True)

    # Remove jobs that have duplicate job IDs
    # sanitizing_df = df.drop_duplicates(subset=[jobId], keep="last") # TODO Unstub?
    sanitizing_df=df # TODO Unstub
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

    formatted_df = sanitizing_df.rename(columns={
        'JobIDRaw': 'jobID',
        'Submit': 'submission_time',
        'NNodes': 'requested_number_of_resources',
        # 'Timelimit': 'requested_time', # TODO There are like not quite necessary but would be nice to have. Currently is stubbed later on.
        'State': 'success',
        'Start': 'starting_time',
        'End': 'finish_time',
        'NodeList': 'allocated_resources'
    })

    # Convert times into time format
    columns_to_convert = ['submission_time', 'starting_time', 'finish_time']
    # Loop through the specified columns and convert values to datetime objects
    for col in columns_to_convert: # TODO I could do __converters instead on pd.read_csv
        formatted_df[col] = formatted_df[col].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S'))

    formatted_df['allocated_resources'] = formatted_df['allocated_resources'].apply(lambda x: x.strip("fg[]").replace("|"," "))
    # Set default values for some columns
    formatted_df['workload_name'] = 'w0'
    formatted_df['purpose'] = 'job' # TODO I'm gonna need to either handle reservations or inject them as jobs or something
    formatted_df['execution_time'] = formatted_df['finish_time'] - formatted_df['starting_time']
    formatted_df['waiting_time'] = formatted_df['starting_time'] - formatted_df['submission_time']
    formatted_df['requested_time'] = formatted_df['execution_time']
    formatted_df['turnaround_time'] = formatted_df['finish_time'] - formatted_df['submission_time']
    formatted_df['stretch'] = formatted_df['turnaround_time'] / formatted_df['requested_time']

    # Reorder the columns to match the specified order
    formatted_df = formatted_df[[
        'jobID',
        'workload_name',
        'submission_time',
        'requested_number_of_resources',
        'requested_time',
        'success',
        'starting_time',
        'execution_time',
        'finish_time',
        'waiting_time',
        'turnaround_time',
        'stretch',
        'allocated_resources',
        'purpose',
    ]]

    return formatted_df
