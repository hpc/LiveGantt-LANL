import time

import pandas as pd
import datetime
from procset import ProcSet


def twenty22():
    """
    Defines a series of variables linked to the column names of the format being used
    :return:
    """
    global endState, wallclockLimit, reqNodes, submit, start, end, jobId, reservation, submitline, account, user
    endState = "State"
    wallclockLimit = "Timelimit"
    reqNodes = "NNodes"
    submit = "Submit"
    start = "Start"
    end = "End"
    jobId = "JobID"
    reservation = "Reservation"
    submitline = "SubmitLine"
    account = "Account"
    user = "User"


def cache_column_typing(formatted_df):
    """
    Properly formats columns for use when loading from cache dataframe
    :param formatted_df: the dataframe loaded from cache
    :return: the dataframe once being properly typed
    """
    # Convert times into the preferred time format
    columns_to_convert = ['submission_time', 'starting_time', 'finish_time']
    # Loop through the specified columns and convert values to datetime objects
    for col in columns_to_convert:  # TODO I could do __converters instead on evalys.read_csv but there's not a good reason to
        formatted_df[col] = formatted_df[col].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    # formatted_df['dependency_chain_head'] = formatted_df['dependency_chain_head'].astype(int)
    formatted_df['allocated_resources'] = formatted_df['allocated_resources'].apply(string_to_procset)

    formatted_df['execution_time'] = formatted_df['finish_time'] - formatted_df['starting_time']
    formatted_df['waiting_time'] = formatted_df['starting_time'] - formatted_df['submission_time']
    formatted_df['requested_time'] = formatted_df['execution_time']
    formatted_df['turnaround_time'] = formatted_df['finish_time'] - formatted_df['submission_time']
    formatted_df['stretch'] = formatted_df['turnaround_time'] / formatted_df['requested_time']

    return formatted_df


def strip_leading_zeroes(s):
    """
    Strip the leading zeroes from each resource value. Used in the allocated_resources column.
    :param s: The string from which to strip leading zeros.
    :return: The string with the values stipped from it.
    """
    values = s.split()
    stripped_values = []
    for value in values:
        parts = value.split('-')
        stripped_parts = [part.lstrip('0') for part in parts]
        stripped_value = '-'.join(stripped_parts)
        stripped_values.append(stripped_value)
    return ' '.join(stripped_values)


# Define a function to convert the string to a ProcSet
def string_to_procset(s):
    """
    Return a ProcSet parsed from a string
    :param s: String to convert
    :return: The resulting ProcSet
    """
    try:
        return ProcSet.from_str(s)
    except:
        return None


def sanitizeFile(inputfile):  # TODO I should only run dependency chain seeking in cases where I absolutely need to.
    """
    Sanitize the data provided from sacct.out in order to ensure that jobs that didn't exist or didn't fit expected bounds don't interfere with chart production.
    :param inputfile: The file to convert to CSV and sanitize job data from
    :return: The sanitized dataframe
    """
    print("\nSanitizing inputfile!\n")
    twenty22()

    print("Reading csv into DataFrame")
    df = pd.read_csv(inputfile)
    df.head()

    print("Filtering Nonexistant Jobs")
    # Jobs that have not ended yet, make them end now. This means that the chart will show jobs that are currently running, in addition to jobs that have finished.
    df[end] = df[end].replace('Unknown', datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))

    # Remove jobs that were cancelled
    df[endState] = df[endState].replace('^CANCELLED by \d+', 'CANCELLED', regex=True)
    # Remove jobs that have duplicate job IDs
    # sanitizing_df = df.drop_duplicates(subset=[jobId], keep="last") # TODO Do I need this? Unsure as of now.
    sanitizing_df = df
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
    sanitizing_df = sanitizing_df.loc[~sanitizing_df[user].isna()]
    # Remove jobs with an unknown submit state
    sanitizing_df = sanitizing_df.loc[sanitizing_df[submit] != "Unknown"]
    # Remove jobs that have a null start
    sanitizing_df = sanitizing_df.loc[~sanitizing_df[start].isna()]
    # Remove jobs that have an end that is not after the start
    sanitizing_df = sanitizing_df.loc[sanitizing_df[end] > sanitizing_df[start]]
    # Set the reservation field properly
    # TODO I can specify!!! For now it'll just be DAT,DST, and PreventMaint but in the future I can show it different for each!

    print("Formatting Reservation Strings")
    # Define the replacement rules using regular expressions
    replacement_rules = {
        'DST': 'reservation',
        'DAT.*': 'reservation',
        'PreventMaint$': 'reservation',
        None: 'job',
        '': 'job',
    }

    # Replace values in the "reservation" column based on the rules
    sanitizing_df[reservation] = sanitizing_df[reservation].replace(replacement_rules, regex=True)
    sanitizing_df.loc[~sanitizing_df[reservation].isin(['reservation', 'job']), reservation] = 'job'

    # Rename the columns in the incoming DF to the target names
    formatted_df = sanitizing_df.rename(columns={
        'JobIDRaw': 'jobID',
        'JobID': 'notRawJobID',
        'Partition': 'partition',
        'Submit': 'submission_time',
        'NNodes': 'requested_number_of_resources',
        'State': 'success',
        'Start': 'starting_time',
        'End': 'finish_time',
        'Eligible': 'eligible',
        'NodeList': 'allocated_resources',
        'Reservation': 'purpose',
        'SubmitLine': 'submitline',
        'Account': 'account',
        'User': 'user',
        'Flags' : 'flags',
        'ConsumedEnergyRaw' : 'consumedEnergy',
    })

    # Convert times into the preferred time format
    columns_to_convert = ['submission_time', 'starting_time', 'finish_time','eligible']
    # Loop through the specified columns and convert values to datetime objects
    for col in columns_to_convert:  # TODO I could do __converters instead on evalys.read_csv but there's not a good reason to
        formatted_df[col] = formatted_df[col].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S'))

    # Strip node titles from the allocated_resources space. This will need to be updated for every cluster it is run
    # on. Then replace the pipe separator used in the allocated resources field with a space, which is preferred for
    # parsing here-on-in
    formatted_df['allocated_resources'] = formatted_df['allocated_resources'].apply(
        lambda x: x.replace("f", "").replace("g", "").replace("[", "").replace("]", "").replace("s", "").replace("n",
                                                                                                                 "").replace(
            "i", "").replace("d", "").replace("|", " ")
    )

    # formatted_df['allocated_resources'] = formatted_df['allocated_resources'].apply(
    #     lambda x: x.strip("fg[]snid").replace("|", " "))
    # formatted_df['allocated_resources'] = formatted_df['allocated_resources'].apply(
    #     lambda x: x.strip("[]"))
    # Apply the strip_leading_zeros function to the 'allocated resources' column
    formatted_df['allocated_resources'] = formatted_df['allocated_resources'].apply(strip_leading_zeroes)
    # Apply the string_to_procset function to the allocated_resources column
    formatted_df['allocated_resources'] = formatted_df['allocated_resources'].apply(string_to_procset)
    # Set default values for some columns
    formatted_df['workload_name'] = 'w0'
    # Calculate waiting, execution, requested, turnaround, and stretch times
    formatted_df['execution_time'] = formatted_df['finish_time'] - formatted_df['starting_time']
    formatted_df['waiting_time'] = formatted_df['starting_time'] - formatted_df['submission_time']
    formatted_df['requested_time'] = formatted_df['execution_time']
    formatted_df['eligible_wait'] = formatted_df['starting_time'] - formatted_df['eligible']
    formatted_df['eligible_wait_seconds'] = formatted_df['eligible_wait'].dt.total_seconds()

    # Normalize the column to range from 0 to 1
    formatted_df['normalized_eligible_wait'] = 1 - (
                formatted_df['eligible_wait_seconds'] / formatted_df['eligible_wait_seconds'].max())

    formatted_df['turnaround_time'] = formatted_df['finish_time'] - formatted_df['submission_time']
    formatted_df['stretch'] = formatted_df['turnaround_time'] / formatted_df['requested_time']

    formatted_df['account_name'] = formatted_df['account']
    formatted_df['account'] = pd.factorize(formatted_df['account'])[0]
    formatted_df['normalized_account'] = 1 - (formatted_df['account'] / formatted_df['account'].max())
    formatted_df['username'] = formatted_df["user"]
    formatted_df['user'] = pd.factorize(formatted_df['user'])[0]
    formatted_df['partition_name'] = formatted_df['partition']
    formatted_df['partition'] = pd.factorize(formatted_df['partition'])[0]
    formatted_df['execution_time_seconds'] = formatted_df['execution_time'].apply(lambda x: x.total_seconds())
    formatted_df['PowerPerNodeHour'] = formatted_df['consumedEnergy']/(formatted_df['requested_number_of_resources']*formatted_df['execution_time_seconds']) # TODO Does this work out to a unit of energy per node hour?

    formatted_df['dependency'] = formatted_df['submitline'].str.extract(r'(?:afterany|afterok):(\d+)', expand=False)

    # If you want to convert the 'Dependency' column to numeric type
    formatted_df['dependency'] = pd.to_numeric(formatted_df['dependency'], errors='coerce')

    # Drop the original 'submitline' column if needed
    formatted_df.drop(columns=['submitline'], inplace=True)

    # Convert the 'Dependency' column to string to handle NaN values
    formatted_df['dependency'] = formatted_df['dependency'].astype(str)
    formatted_df['jobID'] = formatted_df['jobID'].astype(int)
    formatted_df['dependency'] = formatted_df['dependency'].apply(
        lambda x: x.split(".")[0]
    )
    formatted_df['dependency_chain_head'] = formatted_df['dependency']

    # def find_chain_head(job_id, dependency):
    #     """
    #     Seek through the job dependency chain to find the head of the chain
    #     :param job_id:
    #     :param dependency:
    #     :return:
    #     """
    #     # If there is no dependency, return the current JobID
    #     if pd.isna(dependency) or dependency == 'nan':
    #         return job_id
    #     else:
    #         # Recursively find the head of the dependency chain
    #         next_dependency = formatted_df.loc[formatted_df['notRawJobID'] == dependency]['dependency']
    #         is_empty = next_dependency.empty
    #         if next_dependency.empty:
    #             return job_id
    #         else:
    #             return find_chain_head(dependency, next_dependency.values[0])
    #
    # # Create the 'dependency_chain_head' column
    # start_time_task = time.time()
    # formatted_df['dependency_chain_head'] = formatted_df.apply(
    #     lambda row: find_chain_head(row['notRawJobID'], row['dependency']), axis=1)

    # formatted_df['dependency_chain_head'] = formatted_df['dependency_chain_head'].apply(lambda x: int(x) if isinstance(x, int) else int(x.split('+')[0]) if '+' in x else int(x))

    # end_time_task = time.time()
    # duration_task = end_time_task - start_time_task
    # print("Spent " + str(duration_task) + " seconds seeking dependency chain.")
    formatted_df["flags"] = formatted_df["flags"].apply(lambda x: x.split("|"))

    # Reorder the columns to match the specified order
    formatted_df = formatted_df[[
        'jobID',
        'workload_name',
        'submission_time',
        'requested_number_of_resources',
        'requested_time',
        'partition',
        'partition_name',
        'success',
        'starting_time',
        'execution_time',
        'finish_time',
        'waiting_time',
        'turnaround_time',
        'stretch',
        'normalized_eligible_wait',
        'allocated_resources',
        'dependency',
        'dependency_chain_head',
        'purpose',
        'account',
        'normalized_account',
        'account_name',
        'user',
        'username',
        'flags',
        'PowerPerNodeHour',
        'consumedEnergy',
    ]]

    return formatted_df
