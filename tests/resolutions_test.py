#!/usr/bin/env python3

import os
import sys
import string

from datetime import date, datetime

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import resolutions_temporal as rt
import resolutions_spatial as st

def test_basic_temporal():
    line = ["2015-01-15 19:05:39"]
    result = rt.resolve_temporal_date(line, 0)
    print(result)

    line = ["2015-01-15"]
    result = rt.resolve_temporal_datetime(line, 0, 0)
    print(result)

    line = ["2015-01-15", "11:20:05"]
    result = rt.resolve_temporal_datetime(line, 0, 1)
    print(result)

    line = ["2014-02-01 00:00:00"]
    result = rt.resolve_temporal_datetime(line, 0, 0)
    print(result)

    line = ["382","2014-02-01 00:00:00","2014-02-01 00:06:22","294","Washington Square E","40.73049393","-73.9957214","265","Stanton St & Chrystie St","40.72229346","-73.99147535","21101","Subscriber","1991","1"]
    result = rt.resolve_temporal_date(line, 1)
    print(result)


if __name__ == "__main__":
    test_basic_temporal()
    print("{} test_basic_temporal() passed".format(__file__))


    # test_basic_temporal()
    # print("{} test_basic_spatial() passed".format(__file__))
