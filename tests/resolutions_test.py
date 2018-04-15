#!/usr/bin/env python3

from datetime import date, datetime

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import resolutions_temporal as rt
import resolutions_spatial as st

def test_basic_temporal():
    #TODO
    pass


if __name__ == "__main__":
    test_basic_temporal()
    print("{} test_basic_temporal() passed".format(__file__))


    test_basic_temporal()
    print("{} test_basic_spatial() passed".format(__file__))
