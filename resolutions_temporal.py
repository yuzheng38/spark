#!/usr/bin/env python3

from datetime import date, datetime

def resolve_temporal_date(line, ti):
    """
    ti = index of date/datetime attribute in the dataset
    line = one line/row of data
    """
    temp = line[ti].strip() #? strip?
    # NEED to consider different formats
    temp_res = datetime.strptime(temp, "%m/%d/%Y").date()
    #TODO

    temp_res = datetime.strftime(temp_res, "%Y-%m-%d")

    line.append(temp_res) # temporal res added to the end
    return line

def resolve_temporal_datetime(line, ti):
    """
    ti = index of date and/or time attributes in the dataset
    line = one line/row of data
    """
    #TODO
    pass

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
