import sys
import os
from bufresohmsg_py import *


def bufr2mqtt(bufr_file) -> str :
    """
    This function create an e-soh-message-spec json schema from BUFR file.

    Keyword arguemnts:
    bufr_file : BUFR file path

    Return:
    str -- a json in string format

    Raises:
    ---
    """

    ret_str = bufresohmsg_py(bufr_file)
    return ret_str


if __name__ == "__main__":


    test_path = "../test/test_data/SYNOP_BUFR_2718.bufr"
    msg = ""

    #init_bufrtables_py("/usr/share/eccodes/definitions/bufr/tables/0/wmo/")
    init_bufrtables_py("")

    if len(sys.argv) > 1 :
        for i,file_name in enumerate(sys.argv) :
            if i > 0 :
                if os.path.exists(file_name) :
                    msg = bufr2mqtt(file_name)
                    print(msg)
                else :
                    print("File not exists: {0}".format(file_name))
                    exit(1)

    else :
        msg = bufr2mqtt(test_path)
        print(msg)

    destroy_bufrtables_py()

    exit(0)

