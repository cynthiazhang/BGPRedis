//
// Created by Kave Salamatian on 2018-12-01.
//

#include <sqlite3.h>
#include <set>
#include <map>
#include <boost/range/combine.hpp>
#include "BGPGraph.h"
#include "BGPTables.h"
#include "cache.h"
#include "BGPGeopolitics.h"

//using boost::get;
using namespace std;
using namespace tbb;




int BGPCache::fillASCache() {
    sqlite3_stmt *stmt;
    unsigned int asn;
    string name;
    string country;
    string RIR;
    float risk;
    int count = 0;
    string sql;

    cout << "Starting fill AS Cache" << endl;
    /* Create SQL statement */
    sql = "SELECT asNumber, name, country, RIR, riskIndex, geoRisk, perfRisk, secuRisk, otherRisk, observed FROM asn";
    /* Execute SQL statement */
    sqlite3_prepare(db, sql.c_str(), sql.length(), &stmt, NULL);
    bool done = false;
    while (!done) {
        switch (sqlite3_step(stmt)) {
            case SQLITE_ROW: {
                asn = atoi((const char *) sqlite3_column_text(stmt, 0));
                name = string((const char *) sqlite3_column_text(stmt, 1));
                country = string((const char *) sqlite3_column_text(stmt, 2));
                RIR = string((const char *) sqlite3_column_text(stmt, 3));
                risk = atof((const char *) sqlite3_column_text(stmt, 4));
                AS *as = new AS(asn, name, country, RIR, risk);
//                    cout << asn << "," << name << "," << country << "," << RIR << "," << risk << endl;
                asCache.insert(pair<int, AS*>(asn, as));
                count++;
                break;
            }
            case SQLITE_DONE: {
                done = true;
                break;
            }
        }
    }
    sqlite3_reset(stmt);
    sqlite3_finalize(stmt);
    cout << count << " row processed" << endl;
//    cout << contents << endl;
//        updateAS(174, asCache[174]);
    return 0;
}

AS *BGPCache::updateAS(int asn, AS *as) {
    if (as == NULL){
        as = new AS(asn, "XX", "XX", "XX", 0.0);
        toAPIbgpbiew.add(as);
    } else {
        toAPIbgpbiew.add(as);
    }
    return as;
}

AS *BGPCache::chkAS(unsigned int asn, unsigned int time){
    AS *as;
    map<unsigned int, AS*>::iterator it;
    map<int, boost::graph_traits<Graph>::vertex_descriptor >::iterator it1;
    concurrent_hash_map<int,boost::graph_traits<Graph>::vertex_descriptor>::accessor acc;
    concurrent_hash_map<int,boost::graph_traits<Graph>::vertex_descriptor>::const_accessor acc1;

    pair<bool, AS *> ret = asCache.find(asn);
    as = ret.second;
    if (!ret.first){
        as = updateAS(asn, NULL);
        asCache.insert(pair<unsigned int, AS *>(asn, as));
        boost::graph_traits<Graph>::vertex_descriptor v = bgpg->add_vertex(VertexP{to_string(as->asNum), as->country, as->name, as->cTime, 0, 0});
        bgpg->asnToVertex.insert(pair<int,boost::graph_traits<Graph>::vertex_descriptor >(as->asNum, v));
    }
    as->observed = true;
    as->cTime = time;
    return as;
}

long BGPCache::size_of(){
    long size1=0,size2=0,size3=0, size4=0, size5=0;
//    size1=prefixPeersMap.size()*(4+8);
    size2=pathsMap.size()*(4+8);
    size3=asCache.size()*(4+8);
    size4=pathsMap.size_of();
//    size5=prefixPeersMap.size_of();
    return size1+size2+size3+size4+size5;
}

PrefixPath::PrefixPath(BGPMessage *bgpMessage, unsigned int time){
    collector = bgpMessage->collector;
    shortPathLength = bgpMessage->shortPath.size();
    shortPath = new unsigned int[shortPathLength];
    int i =0;
    for (auto as:bgpMessage->shortPath){
        shortPath[i++]=as;
    }
    pathLength=bgpMessage->asPath.size();
    lastActive = time;
    lastChange = time;
    active = true;
    activePrefixTrie = new Trie();
}

PrefixPath::~PrefixPath(){
    delete shortPath;
}


int PrefixPath::size_of(){
    int size=13*4+3*8+1;;
    size += collector.length();
    size +=shortPathLength*4;
//    size +=activePrefixMap.size()*(4+8);
    return size;
}

double PrefixPath::getScore() const {
    return shortPathLength*1.0;
}


string PrefixPath::str() const {
    string pathString ="[";
    for (int i=0;i<shortPathLength;i++) {
        pathString += to_string(shortPath[i])+",";
    }
    pathString = pathString+"]";
    return pathString;
}

unsigned int PrefixPath::getPeer() const{
    return shortPath[0];
}

unsigned int PrefixPath::getDest() const{
    return shortPath[shortPathLength-1];
}

void PrefixPath::addPrefix(bgpstream_pfx_t *pfx, unsigned int time){
    activePrefixTrie->insert(pfx, NULL);
    if (!active){
        active = true;
        meanDown = coeff*meanDown+(1-coeff)*(time-lastChange);
        lastActive = time;
    }
    lastChange = time;
}

void PrefixPath::erasePrefix(bgpstream_pfx_t *pfx, unsigned int time){
    activePrefixTrie->remove(pfx);
    if (active && (activePrefixTrie->prefixNum()==0)){
        active = false;
        meanUp = coeff*meanUp+(1-active)*(time-lastActive);
    }
    lastChange = time;
}

bool PrefixPath::checkPrefix(bgpstream_pfx_t *pfx){
    return activePrefixTrie->check(pfx);
}
bool PrefixPath::equal(PrefixPath* path) const{
    if (path->shortPathLength == shortPathLength){
        for (int i=0; i<shortPathLength;i++){
            if (path->shortPath[i]!=shortPath[i]){
                return false;
            }
        }
    }
    return true;
}


AS::AS(int asn): asNum(asn){
    activePrefixTrie = new Trie();
    inactivePrefixTrie = new Trie();
}
AS::AS(int asn, string inname, string incountry, string inRIR, float risk): asNum(asn), name(inname), country(incountry), RIR(inRIR){
    activePrefixTrie = new Trie();
    inactivePrefixTrie = new Trie();
}

bool AS::checkOutage(){
    activePrefixNum = activePrefixTrie->prefixNum();
    allPrefixNum =  activePrefixNum+ inactivePrefixTrie->prefixNum();
    if ((activePrefixNum<0.3*allPrefixNum) &&(allPrefixNum>10))  {
        status |= OUTAGE;
        return true;
    }   else {
        if (status & OUTAGE) {
            status &= ~OUTAGE;
        }
        return false;
    }
}

void AS::disconnect(){
    status |= DISCONNECTED;
}

void AS::reconnect(){
    status &= ~DISCONNECTED;
}

int AS::size_of(){
    int size =4+4*8+4*4+1;
    size += name.length();
    size += country.length();
    size += RIR.length();
    //        for(auto it=activePrefixMap.begin();it!=activePrefixMap.end();it++)
    //size += activePrefixMap.size()*(4+8);
    return size;
}

void AS::addPrefix(bgpstream_pfx_t *pfx, unsigned int time){
    activePrefixTrie->insert(pfx, NULL);
    inactivePrefixTrie->remove(pfx);
    //added by zxy
    Event *prefixAnn = new Event();
    prefixAnn->type = PREFIX_ANN_AS;
    prefixAnn->s_type = "PREFIX_ANN_AS";
    prefixAnn->time = time;
    prefixAnn->asn = asNum;
    char buf[20];
    bgpstream_pfx_snprintf(buf, 20, (bgpstream_pfx_t *)pfx);
    string str(buf);
    prefixAnn->prefixPeer = str;
    queue->push(prefixAnn);
    //end of add
}

void AS::removePrefix(bgpstream_pfx_t *pfx, unsigned int time){
    //remove the prefixPeer from the AS
    activePrefixTrie->remove(pfx);
    inactivePrefixTrie->insert(pfx, NULL);
    //add by zxy
    Event *prefixRem= new Event();
    prefixRem->asn = asNum;
    prefixRem->time = time;
    prefixRem->s_type = "PREFIX_REM_AS";
    prefixRem->type = PREFIX_REM_AS;
    char buf[20];
    bgpstream_pfx_snprintf(buf, 20, (bgpstream_pfx_t *)pfx);
    string str(buf);
    prefixRem->prefixPeer = str;
    queue->push(prefixRem);
    //end of add
}
