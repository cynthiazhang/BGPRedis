//
// Created by Kave Salamatian on 2018-12-01.
//
#include "BGPGeopolitics.h"
#include "cache.h"


BGPMessage::BGPMessage(BGPCache *cache, int order): cache(cache), poolOrder(order){}

//BGPMessage::BGPMessage(bgpstream_elem_t *elem, std::string collector, BGPCache *cache): cache(cache){
//    fill(elem,collector, cache);
//}

void BGPMessage::fill(long order, bgpstream_elem_t *elem, unsigned int time, std::string incollector, BGPCache *cache){
    bgpstream_as_path_iter_t iter;
    bgpstream_as_path_seg_t *seg;
    unsigned int asn;
    std::map<std::string, Path *>::iterator it1;
    AS *as;
    double asRisk=0.0;
    bool found;

    messageOrder = order;
    category = None;
    prefixPath = NULL;
    newPath = false;
    asPath.clear();
    shortPath.clear();
    withdraw = false;
    type = elem->type;
    
    memcpy(&peerAddress, &elem->peer_ip , sizeof(bgpstream_addr_storage_t));
    memcpy(&nextHop, &elem->nexthop, sizeof(bgpstream_addr_storage_t));
    char buf[20];
    bgpstream_pfx_snprintf(buf, 20, (bgpstream_pfx_t *) &elem->prefix);
    memcpy(&pfx, &elem->prefix,sizeof(bgpstream_pfx_storage_t));
    std::string str(buf);
    timestamp = time;
    collector = incollector;
    peerASNumber = elem->peer_asn;
    bgpstream_as_path_iter_reset(&iter);
    while ((seg = bgpstream_as_path_get_next_seg(elem->as_path, &iter)) != NULL) {
        switch (seg->type) {
            case BGPSTREAM_AS_PATH_SEG_ASN:
                asn = ((bgpstream_as_path_seg_asn_t *) seg)->asn;
                asPath.push_back(asn);
                break;
            case BGPSTREAM_AS_PATH_SEG_SET:
                /* {A,B,C} */
                break;
            case BGPSTREAM_AS_PATH_SEG_CONFED_SET:
                /* [A,B,C] */
                break;
            case BGPSTREAM_AS_PATH_SEG_CONFED_SEQ:
                /* (A B C) */
                break;
        }
    }
    shortenPath();
    string pathStr=pathString();
    
    pair<bool, PrefixPath *> ret3 = cache->pathsMap.find(pathStr);
    found =ret3.first;
    prefixPath = ret3.second;
    if (!found){
        prefixPath = new PrefixPath(this, timestamp);
        prefixPath->addPrefix(( bgpstream_pfx_t *)&pfx, timestamp);
        prefixPath->risk=0.0;
        for (int i = 0; i < prefixPath->shortPathLength; i++) {
            asn = prefixPath->shortPath[i];
            as = cache->chkAS(asn, timestamp);
            asRisk= fusionRisks(as->geoRisk, as->secuRisk,  as->otherRisk);
            if (asRisk > prefixPath->risk){
                prefixPath->risk = asRisk ;
            }
        }
        cache->pathsMap.checkinsert(prefixPath);
    }
}

void BGPMessage::shortenPath() {
    unsigned int prev = 0;
    for (auto asn:asPath) {
        if (asn != prev) {
            shortPath.push_back(asn);
            prev = asn;
        }
    }
    shortPath.resize(shortPath.size());
}

string BGPMessage::pathString(){
    string pathStr = "[";
    for (auto asn:shortPath){
        pathStr +=to_string(asn)+",";
    }
     pathStr += "]";
    return pathStr;
}

void BGPMessage::preparePath(){
    
}

double BGPMessage::fusionRisks(double geoRisk, double secuRisk, double otherRisk){
    double stdRisk=1.0;
    if (stdRisk==0) {
        return 0;
    } else
        return secuRisk/(stdRisk*0.5+geoRisk*0.5);
}

unsigned int BGPMessage::getIP()
{
    return ntohl(pfx.address.ipv4.s_addr);
}

