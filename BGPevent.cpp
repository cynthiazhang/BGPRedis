//
//  BGPEvent.cpp
//  BGPGeo
//
//  Created by zhangxinyi on 2019/3/20.
//  Copyright © 2019年 zhangxinyi. All rights reserved.
//

#include "BGPGeopolitics.h"
using json = nlohmann::json;

string Event :: toJson(){
    json j;
    string str;
    j["time"] = time;

  if(type == PREFIX_ANN_AS){
        j["type"] = "PREFIX_ANN_AS";
        j["as"] = asn;
        j["preifixPeer"] = prefixPeer;
 
    }else if(type == PREFIX_REM_AS){
        j["type"] = "PREFIX_REM_AS";
        j["as"] = asn;
        j["preifixPeer"] = prefixPeer;

    }else if(type == OUTAGE_EV){
        j["type"] = "OUTAGE_EV";

/*
    }else if(type == OUTAGE_END){
        j["type"] = "OUTAGE_END";
*/
    }else if(type == HIJACK_EV){
        j["type"] = "HIJACK_EV";
        j["hijacking"] = asn;
        j["hijacked"] = hijackedAsn;


    }
/*
    }else if(type == HIJACK_END){
        j["type"] = "HIJACK_END";
        j["hijacking"] = asn;
        j["hijacked"] = hijackedAsn;

    }else if(type == NEW_PATH){
        j["type"] = "NEW_PATH";
        j["path"] = path->str();
        j["length"] = path->pathLength;
        
    }else if(type == PATH_INACTIVE){
        j["type"] = "PATH_INACTIVE";
        j["path"] =  path->str();
        j["length"] = path->pathLength;
    }
*/
    str = j.dump();
    return str;
}
