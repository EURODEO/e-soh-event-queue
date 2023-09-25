/*
 * (C) Copyright 2023, Eumetnet
 *
 * This file is part of the E-SOH Norbufr BUFR en/decoder interface
 *
 * Author: istvans@met.no
 *
 */

#include <sstream>
#include <iomanip>

#include "NorBufrIO.h"
#include "WSI.h"

WSI::WSI()
{
    wigos_id_series = 0;
    wigos_issuer_id = 0;
    wigos_issue_num = 0;
    wigos_local_id = "";
}

std::string WSI::to_string() const
{
    std::stringstream ss;
    ss << wigos_id_series;
    ss << "-";
    ss << wigos_issuer_id;
    ss << "-";
    ss << wigos_issue_num;
    ss << "-";
    ss << wigos_local_id;

    return ss.str();

}

void WSI::setWigosIdSeries(int wid)
{
    wigos_id_series = wid;
}

void WSI::setWigosIssuerId(uint16_t wis )
{
    wigos_issuer_id = wis;
}

void WSI::setWigosIssueNum(uint16_t wisn )
{
    wigos_issue_num = wisn;
}

void WSI::setWigosLocalId(std::string wlid )
{
    wigos_local_id = NorBufrIO::strTrim(wlid);
}

void WSI::setWmoId(int wlid )
{
    std::stringstream ss;
    ss << std::setw(5) << std::setfill('0') << wlid;
    wigos_local_id = ss.str();
    setWigosIssuerId(20000);
}

std::ostream & operator<<(std::ostream & os,  const WSI & w)
{
    os << w.to_string();
    return os;
}

