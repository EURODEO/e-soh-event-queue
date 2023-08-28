import sys
import os
import uuid
import json
import numpy
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
                    "masterTablesVersionNumber","localTablesVersionNumber","numberOfSubsets","observedData","compressedData","unexpandedDescriptors"],
        "datetime" : ["year","month","day","hour","minute","second","secondsWithinAMinuteMicrosecond"],
        "station_id" : ["blockNumber","stationNumber","stationOrSiteName","stateIdentifier","nationalStationNumber","aircraftFlightNumber","aircraftRegistrationNumberOrOtherIdentification","observationSequenceNumber","aircraftTailNumber","originationAirport","destinationAirport","shipOrMobileLandStationIdentifier"],
        "measure" : ["nonCoordinatePressure","pressureReducedToMeanSeaLevel","airTemperatureAt2M","dewpointTemperatureAt2M","airTemperature","dewpointTemperature"]

        }


    version_str = "v04"

    message_template = {
        "id" : "",
        "version" : version_str,

        "type" : "Feature",
        "geometry" : "null",
        "properties" : { "station_id" : {} , "measure" : {} }

        }

    # data_id
    message_template['properties'].update(data_id=os.path.basename(bufr_file))

    bf = open(bufr_file,'rb')

    ret_str = ""

    while 1 :

        bufr = codes_bufr_new_from_file(bf)

        # Error ?
        if bufr is None:
            return ret_str

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
                if prop == "unexpandedDescriptors" :
                    codes = codes_get_array(bufr,prop)
                else :
                    codes = codes_get(bufr,prop)
                if prop == "dataCategory" :
                    data_category = codes_get(bufr,prop)
                if type(codes) is numpy.ndarray :
                    ct = []
                    for c in codes :
                        ct.append(int(c))
                    message_template['properties'].update({ prop : [ ct ] })
                else :
                    message_template['properties'].update({ prop : codes })

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

        # datetime
        dt = {}
        for d_field in bufr_keys["datetime"] :
            if codes_is_defined(bufr,d_field) :
                dt[d_field] = codes_get_array(bufr, d_field)
            else :
                if d_field not in dt :
                    dt[d_field] = [0.0] * subsets

        # station_id
        st_id = {}
        for s_field in bufr_keys["station_id"] :
            if codes_is_defined(bufr,s_field) and not codes_is_missing(bufr,s_field) :
                st_id[s_field] = codes_get_array(bufr, s_field)

        # measure
        meas = {}
        meas_unit = {}
        for m_field in bufr_keys["measure"] :
            if codes_is_defined(bufr,m_field) and not codes_is_missing(bufr,m_field) :
                meas[m_field] = codes_get_array(bufr, m_field)
                meas_unit[m_field] =codes_get_array(bufr, m_field + "->units")

        for s in range(0,subsets) :
            ret_messages = message_template.copy()

            ret_messages.update(id=str(uuid.uuid4()))

            if lat[s] < 90 and lat[s] > -90 and lon[s] < 180 and lon[s] > -180 :
                if height_exists :
                    ret_messages['geometry'] = { 'type' : 'Point', 'coordinates' : [ round(lat[s],6) , round(lon[s],6) , round(hei[s],6) ]  }
                else :
                    ret_messages['geometry'] = { 'type' : 'Point', 'coordinates' : [ round(lat[s],6) , round(lon[s],6) ]  }

            ret_messages['properties'].update({ 'dataSubset' : s })

            # datetime
            datetime_str = ""

            if len(dt["year"]) == 1 :
                dt_year = dt["year"][0]
            else :
                dt_year = dt["year"][s]

            if len(dt["month"]) == 1 :
                dt_month = dt["month"][0]
            else :
                dt_month = dt["month"][s]

            if len(dt["day"]) == 1 :
                dt_day = dt["day"][0]
            else :
                dt_day = dt["day"][s]

            if len(dt["hour"]) == 1 :
                dt_hour = dt["hour"][0]
            else :
                dt_hour = dt["hour"][s]

            if len(dt["minute"]) == 1 :
                dt_minute = dt["minute"][0]
            else :
                dt_minute = dt["minute"][s]

            if len(dt["second"]) == 1 :
                dt_second = dt["second"][0]
            else :
                dt_second = dt["second"][s]

            if len(dt["secondsWithinAMinuteMicrosecond"]) == 1 :
                dt_microsecond = dt["secondsWithinAMinuteMicrosecond"][0]
            else :
                dt_microsecond = dt["secondsWithinAMinuteMicrosecond"][s]

            if dt_year > 1900 and dt_year < 3000 and dt_month >= 1 and  dt_month <= 12 and dt_day >= 1 and dt_day <= 31 :
                datetime_str = f'{dt_year:04}-{dt_month:02}-{dt_day:02}'
                if dt_hour >= 0 and dt_hour <= 23 :
                    datetime_str += f'T{dt_hour:02}'
                    if dt_minute >= 0 and dt_minute <= 59 :
                        datetime_str += f':{dt_minute:02}'
                        if dt_second >= 0 and dt_second <= 59 :
                            dt_sec = float(dt_second)
                            if dt_microsecond < 1.0 :
                                dt_sec += dt_microsecond
                            datetime_str += f':{dt_sec:04.6}'
                        else :
                            datetime_str += ":00.0"
                    else :
                        datetime_str += ":00:00.0"
                else :
                    datetime_str += "T00:00:00.0"

            ret_messages['properties'].update({ 'datetime' : datetime_str })

            #station_id
            for sid in st_id :
                ret_messages['properties']['station_id'].update({ str(sid) : str(st_id[sid][s]) })

            #measure
            for m in meas :
                ret_messages['properties']['measure'].update({ str(m) : str(meas[m][s]) + " " + str(meas_unit[m][s]) })

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

