//
//  BGPEvent.h
//  BGPGeo
//
//  Created by zhangxinyi on 2019/3/20.
//  Copyright © 2019年 zhangxinyi. All rights reserved.
//

#ifndef BGPEvent_h
#define BGPEvent_h


#include "BGPGeopolitics.h"
#include "tbb/concurrent_vector.h"
#include "json.hpp"

using namespace std;
//using namespace boost;

enum EventType{PREFIX_ANN_AS, PREFIX_REM_AS, OUTAGE_EV, HIJACK_EV};

class AS;
class PrefixPeer;
class PrefixPath;

class Event{
public:
    EventType type;
    string s_type;
    AS *as;
    unsigned int asn;
    unsigned int time=0;
    string prefixPeer;
    unsigned int hijackedAsn;
    
    //PrefixPath *path;
    Event(){}
    string toJson();
};





#endif /* BGPEvent_h */
