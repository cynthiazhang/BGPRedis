//
// Created by Kave Salamatian on 25/11/2018.
//

#ifndef BGPGEOPOLITICS_CACHE_H
#define BGPGEOPOLITICS_CACHE_H
#include <sqlite3.h>
#include <set>
#include <map>
#include <thread>
#include "BGPGraph.h"
#include "BGPGeopolitics.h"
//#include "BGPTables.h"
#include "BGPEvent.h"
#include "tbb/concurrent_unordered_map.h"
#include "tbb/concurrent_unordered_set.h"
#include "BlockingQueue.h"
#include "apibgpview.h"
#include "threadsafe_queue.h"



using namespace std;
using namespace tbb;

//using namespace boost;

class RoutingTable;
class AS;
class Link;
class Path;
class BGPMessage;
class EventTable;

using namespace boost;
using namespace std;
using namespace tbb;

enum Status{CONNECTED=1, DISCONNECTED=2, OUTAGE=4, HIJACKED=8, HIJACKING=16};


class Trie;

class PrefixPath {
public:
    short int pathLength;
    short int shortPathLength;
    unsigned int *shortPath; //owned
    double risk = 0;
    Trie *activePrefixTrie;
    string collector;
    bool active= true;
    unsigned int lastChange = 0;
    unsigned int lastActive = 0;
    int announcementNum =0;
    int AADiff =0, AADup=0, WADup=0, WWDup=0, Flap=0, Withdraw=0;
    float coeff = 0.9;
    double globalRisk=0.0;
    double meanUp, meanDown;
    
    PrefixPath(BGPMessage *bgpMessage, unsigned int time);
    ~PrefixPath();
    double getScore() const;
    string str() const ;
    int size_of();
    unsigned int getPeer() const;
    unsigned int getDest() const;
    void addPrefix(bgpstream_pfx_t *pfx, unsigned int time);
    void erasePrefix(bgpstream_pfx_t *pfx, unsigned int time);
    bool checkPrefix(bgpstream_pfx_t *pfx);
    bool equal(PrefixPath* path) const;
};

class Trie;

class AS {
public:
    unsigned int asNum;
    string name="??";
    string country="??";
    string RIR="??";
    string ownerAddress="??";
    string lastChanged = "??";
    double risk=0.0, geoRisk=0.0, secuRisk=0.0,  otherRisk=0.0;
    unsigned long activePrefixNum=0;
    unsigned long allPrefixNum=0;
    Trie *activePrefixTrie;
//    MyThreadSafeMap<unsigned int,PrefixPeer *> activePrefixMap;
    Trie *inactivePrefixTrie;
    unsigned int cTime=0;
    bool observed=false;
    int status=0;
    
    //modified by zxy
    Threadsafe_queue<Event*> *queue;
    //end of modify
    
    AS(int asn);
    AS(int asn, string inname, string incountry, string inRIR, float risk);
    bool checkOutage();
    void disconnect();
    void reconnect();
    int size_of();
    void addPrefix(bgpstream_pfx_t *pfx, unsigned int time);
    void removePrefix(bgpstream_pfx_t *pfx, unsigned int time);
    
    
};

class Link{
public:
    unsigned int src;
    unsigned int dst;
    unsigned int cTime;
    Status status;
    bool active;
    MyThreadSafeHashMap<PrefixPath *, unsigned int> activePathsMap;
    Link(unsigned int src, unsigned int dst, unsigned int time): src(src), dst(dst), cTime(time){}
    string str(){
        return to_string(src)+"|"+to_string(dst);
    }
    int size_of(){
        int size =14;
        size += activePathsMap.size()*(4+8);
        return size;
    }
};


class Peer {
public:
    unsigned short int collectorNum;
    unsigned int asNum;
    
    Peer(unsigned short int collectorNum, unsigned int asNum): collectorNum(collectorNum), asNum(asNum){}
    string str(){
        return to_string(asNum)+'|'+to_string(collectorNum);
    }
};


class BGPCache {
public:
    int numActivePath=0;
    map<string, unsigned short int> collectors;
    MyThreadSafeMap<unsigned short int, Peer*> peersMap;
    MyThreadSafeMap<unsigned int, AS *> asCache;
    MyThreadSafeHashMap<Link *, unsigned int> linksMap;
    MyThreadSafeHashMap<PrefixPath *, unsigned int> pathsMap;
    std::thread apiThread;
    BlockingCollection<AS *> toAPIbgpbiew;
    BGPGraph *bgpg;
    BGPCache(string dbname, BGPGraph *g, map<string, unsigned short int> *collectors): bgpg(g), collectors(*collectors) {
        if (sqlite3_open(dbname.c_str(), &db) != SQLITE_OK) {
            cout << "Can't open database: " << sqlite3_errmsg(db) << endl;
        }
        apibgpview = new APIbgpview(db, toAPIbgpbiew, this);
        apiThread = std::thread(&APIbgpview::run, apibgpview);
    }

    int fillASCache();
    AS *updateAS(int asn, AS *as);
    AS *chkAS(unsigned int asn, unsigned int time);
    long size_of();
private:
    sqlite3 *db;
    APIbgpview *apibgpview;
};




#endif //BGPGEOPOLITICS_CACHE_H
