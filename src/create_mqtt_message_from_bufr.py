import sys
import os
import uuid
import json
from eccodes import *


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

    bufr_keys = {
        "metadata_id": ["blockNumber","stationNumber","stateIdentifier","nationalStationNumber"],
        "height" : ["heightOfStationGroundAboveMeanSeaLevel","heightOfBarometerAboveMeanSeaLevel","heightOfStation","height"],
        "properties" : ["edition","masterTableNumber","bufrHeaderCentre","bufrHeaderSubCentre","updateSequenceNumber","dataCategory","internationalDataSubCategory","dataSubcategory",
                    "masterTablesVersionNumber","localTablesVersionNumber","numberOfSubsets","observedData","compressedData","unexpandedDescriptors"]
        }


    version_str = "v04"

    message_template = {
        "id" : str(uuid.uuid4()),
        "version" : version_str,

        "type" : "Feature",
        "geometry" : "null",
        "properties" : {}

        }

    # data_id
    message_template['properties'].update(data_id=os.path.basename(bufr_file))

    bf = open(bufr_file,'rb')
    bufr = codes_bufr_new_from_file(bf)

    # Error ?
    if bufr is None:
            return message_template

    # Prod date from Section0
    if codes_is_defined(bufr,'typicalDate') :
        bufr_msg_date = codes_get(bufr, 'typicalDate')
        if codes_is_defined(bufr,'typicalTime') :
            bufr_msg_time = codes_get(bufr, 'typicalTime')
            pubtime_str = bufr_msg_date[0:4] + '-' + bufr_msg_date[4:6] + '-' + bufr_msg_date[6:8] + 'T' + bufr_msg_time[0:2] + ':' + bufr_msg_time[2:4] + ':' + bufr_msg_time[4:6] + ".0"
            message_template['properties'].update(pubtime=pubtime_str)

    data_category = -1
    for prop in bufr_keys["properties"] :
        if codes_is_defined(bufr,prop) :
            # API bug, skip unexpandedDescriptors
            if data_category == 4 and prop == "unexpandedDescriptors" :
                continue
            upd_str = prop + "=" + str(codes_get(bufr,prop))
            if prop == "dataCategory" :
                data_category = codes_get(bufr,prop)
            message_template['properties'].update({ prop : codes_get(bufr,prop) })

    codes_set(bufr, 'unpack', 1)

    subsets = int(message_template['properties']['numberOfSubsets'])

    # Geometry
    if codes_is_defined(bufr,'latitude') :
        lat = codes_get_array(bufr, 'latitude')
        if codes_is_defined(bufr,'longitude') :
            lon = codes_get_array(bufr, 'longitude')
            height_exists = False
            for desc in bufr_keys["height"] :
                if codes_is_defined(bufr,desc) :
                    height_exists = True
                    hei = codes_get_array(bufr,desc)
                    break

    ret_str = ""

    for s in range(0,subsets) :
        ret_messages = message_template.copy()

        if s > 0 :
            ret_messages.update(id=str(uuid.uuid4()))

        if lat[s] < 90 and lat[s] > -90 and lon[s] < 180 and lon[s] > -180 :
            if height_exists :
                ret_messages['geometry'] = { 'type' : 'Point', 'coordinates' : [ round(lat[s],6) , round(lon[s],6) , round(hei[s],6) ]  }
            else :
                ret_messages['geometry'] = { 'type' : 'Point', 'coordinates' : [ round(lat[s],6) , round(lon[s],6) ]  }

        ret_messages['properties'].update({ 'dataSubset' : s })
        ret_str += "\n" + json.dumps(ret_messages,indent=2)

    return ret_str


if __name__ == "__main__":


    test_path = "../test/test_data/SYNOP_BUFR_2718.bufr"
    msg = ""

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

    exit(0)

