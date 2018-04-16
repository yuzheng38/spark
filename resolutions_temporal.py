#!/usr/bin/env python3

import string
from datetime import date, datetime

def resolve_temporal_date(line, ti):
    """
    ti = index of date/datetime attribute in the dataset
    line = one line/row of data
    """
    temp = line[ti]
    if temp.startswith('"') or temp.startswith('"'):
        temp = temp.strip(string.punctuation)

    # Might need to consider more formats with more datasets
    temp_res = None
    if "/" in temp:
        try:
            temp_res = datetime.strptime(temp, "%m/%d/%Y").date()
        except ValueError:
            try:
                temp_res = datetime.strptime(temp, "%m/%d/%Y %H:%M:%S").date()
            except ValueError:
                temp_res = datetime.strptime(temp, "%m/%d/%Y %H:%M").date() # this is citibike different format.... sigh
    elif "-" in temp:
        try:
            temp_res = datetime.strptime(temp, "%Y-%m-%d").date()
        except ValueError:
            temp_res = datetime.strptime(temp, "%Y-%m-%d %H:%M:%S").date()

    temp_res = datetime.strftime(temp_res, "%Y-%m-%d")

    line.append(temp_res) # temporal res added to the end
    return line

def resolve_temporal_datetime(line, ti_date, ti_time):
    """
    ti_date = index of date attribute in the dataset
    ti_time = index of time attribute in the dataset
    line = one line/row of data
    """
    if ti_date != ti_time:
        date = line[ti_date].strip(string.punctuation)
        time = line[ti_time].strip(string.punctuation)
        temp = date + " " + time
    else:
        temp = line[ti_date].strip(string.punctuation)

    # Might need to consider more formats with more datasets
    temp_res = None
    if "/" in temp:
        try:
            temp_res = datetime.strptime(temp, "%m/%d/%Y")
        except ValueError:
            temp_res = datatime.strptime(temp, "%m/%d/%Y %H:%M:%S")
    elif "-" in temp:
        try:
            temp_res = datetime.strptime(temp, "%Y-%m-%d")
        except ValueError:
            temp_res = datetime.strptime(temp, "%Y-%m-%d %H:%M:%S")

    temp_res = datetime.strftime(temp_res, "%Y-%m-%d %H:%M:%S")

    line.append(temp_res) # temporal res added to the end
    return line


def resolve_temporal_weekday(line, ti):
    """
    ti = index of date/datetime attribute in the dataset
    line = one line/row of data
    """
    #TODO
    pass

def resolve_temporal_weekday_vs_weekend(line, ti):
    """
    ti = index of date/datetime attribute in the dataset
    line = one line/row of data
    """
    #TODO
    pass
