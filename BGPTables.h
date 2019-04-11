//
// Created by Kave Salamatian on 2018-12-01.
//

#ifndef BGPGEOPOLITICS_BGPTABLES_H
#define BGPGEOPOLITICS_BGPTABLES_H

#include "cache.h"
#include "BGPGeopolitics.h"
#include "BlockingQueue.h"
#include "BGPEvent.h"
#include "bgpstream_utils_patricia.h"
#include <map>


class PrefixPath;
class PrefixPeer;
class EventTable;
class BGPSource;
using namespace boost;

struct pathComp {
    bool operator() (const PrefixPath* lhs, const PrefixPath* rhs) const {
        if (lhs->getPeer() == rhs->getPeer()){
            return false;
        }
        if (lhs->getScore()>rhs->getScore()){
            return false;
        } else {
            if (lhs->getScore()<rhs->getScore()){
                return true;
            } else {
                for (int i=0; i<lhs->shortPathLength;i++){
                    if (lhs->shortPath[i]<rhs->shortPath[i])
                        return true;
                }
                return false;
            }
        }
    }
};

typedef set<PrefixPath*,pathComp> PathsSet;



class CollectorElement {
public:
    PathsSet pathsSet;
    PrefixPath *bestPath= NULL;

    pair<bool,PrefixPath*> addPath(PrefixPath * prefixPath, unsigned int time){
        bool endOutage= false;
        PrefixPath* previous = NULL;
        if (bestPath == NULL){
            endOutage = true;
        }

        auto it =pathsSet.find(prefixPath);
        if (it == pathsSet.end()){
            pathsSet.insert(prefixPath);
            previous = NULL;
            endOutage = false;
        } else {
            if (*it != prefixPath) {
                previous = *it;
                pathsSet.erase(it);
                pathsSet.insert(prefixPath);
            }
        }
        bestPath = *(pathsSet.begin());
        return pair<bool,PrefixPath*> (endOutage, previous);
    }
    
    pair<bool,PrefixPath*> erasePath(PrefixPath * prefixPath, unsigned int time){
        PathsSet::iterator it;
        bool outage= false;
        PrefixPath *path;
        it= pathsSet.find(prefixPath);
        path = *it;
        if (it != pathsSet.end()){
            pathsSet.erase(it);
            if (pathsSet.size()>0){
                bestPath = *(pathsSet.begin());
            } else{
                //Prefix OUTAGE in Peer
                outage = true;
                bestPath = NULL;
            }
            return pair<bool,PrefixPath*>(outage,path);
        } else {
            return pair<bool,PrefixPath*>(false, NULL);
        }
    }

    PrefixPath *find(unsigned int peer){
        for (auto path:pathsSet){
            if (path->getPeer() == peer) {
                return path;
            }
        }
        return NULL;
    }
    
    long size_of(){
        long size = pathsSet.size();
        return size;
    }

private:
    mutable boost::shared_mutex mutex_;
};

class TrieElement{
public:
    MyThreadSafeMap<unsigned short int, CollectorElement*> collectors;
    set<unsigned int> asSet;
    PriorityBlockingCollection<BGPMessage *,  PriorityContainer<BGPMessage *, BGPMessageComparer>> inprocessQueue;
    mutable boost::shared_mutex mutex_;
    
    unsigned int cTime;
    pair<bool,PrefixPath*> addPath(unsigned short int collector, PrefixPath* prefixPath, unsigned int time){
        boost::upgrade_lock<shared_mutex> readLock(mutex_);
        CollectorElement *collectorElement;
        auto ret = collectors.find(collector);
        collectorElement = ret.second;
        if (!ret.first){
            collectorElement = new CollectorElement();
            boost::upgrade_to_unique_lock<shared_mutex> writeLock(readLock);
            collectorElement->addPath(prefixPath,time);
            collectors.insert(pair<unsigned short int, CollectorElement*>(collector, collectorElement));
        }
        return collectorElement->addPath(prefixPath,time);
    }

    pair<bool,PrefixPath*> erasePath(unsigned short int collector, PrefixPath* prefixPath, unsigned int time){
        bool globalOutage = false;
        pair<bool,PrefixPath*> colRet;
        
        boost::upgrade_lock<shared_mutex> readLock(mutex_);
        auto ret = collectors.find(collector);
        if (ret.first){
            boost::upgrade_to_unique_lock<shared_mutex> writeLock(readLock);
            colRet = ret.second->erasePath(prefixPath,time);
            if (colRet.first){
                //there is a peer outage
                globalOutage = true;
                for (auto p:collectors.map ){
                    if (p.second->bestPath != NULL){
                        globalOutage = false;
                        break;
                    }
                }
                return pair<bool,PrefixPath*>(globalOutage, colRet.second);
            }
        }
        return pair<bool,PrefixPath*>(false,NULL);
    }
    
    PrefixPath* find(unsigned short int collector, unsigned peer){
        boost::upgrade_lock<shared_mutex> readLock(mutex_);
        
        auto ret = collectors.find(collector);
        if (ret.first){
            return ret.second->find(peer);
        } else {
            return NULL;
        }
    }
    
    void addAS(unsigned int asNum){
        boost::upgrade_lock<shared_mutex> readLock(mutex_);
        boost::upgrade_to_unique_lock<shared_mutex> writeLock(readLock);
        asSet.insert(asNum);
    }
    
    void removeAS(unsigned int asNum){
        boost::upgrade_lock<shared_mutex> readLock(mutex_);
        boost::upgrade_to_unique_lock<shared_mutex> writeLock(readLock);
        asSet.erase(asNum);
    }
    
    bool checkHijack(){
        boost::upgrade_lock<shared_mutex> readLock(mutex_);
        if (asSet.size()>2)
            return true;
        return false;
    }

};

class Trie{
private:
    mutable boost::shared_mutex mutex_;
public:
    bgpstream_patricia_tree_t *pt;
    
    Trie(){
        /* Create a Patricia Tree */
        pt = bgpstream_patricia_tree_create(NULL);
    }
    
    void* insert(bgpstream_pfx_t *pfx, void *data){
        boost::unique_lock<boost::shared_mutex> lock(mutex_);
        void *tmp;
        bgpstream_patricia_node_t *node = bgpstream_patricia_tree_insert(pt,pfx);
        tmp= bgpstream_patricia_tree_get_user(node);
        bgpstream_patricia_tree_set_user(pt, node, data);
        return tmp;
    }
    
    void* search(bgpstream_pfx_t *pfx){
        boost::shared_lock<boost::shared_mutex> lock(mutex_);
        bgpstream_patricia_node_t *node = bgpstream_patricia_tree_search_exact(pt, pfx);
        if (node == NULL) {
            return NULL;
        } else {
            return bgpstream_patricia_tree_get_user(node);
        }
    }
    
    bool check(bgpstream_pfx_t *pfx){
        boost::shared_lock<boost::shared_mutex> lock(mutex_);
        bgpstream_patricia_node_t *node = bgpstream_patricia_tree_search_exact(pt, pfx);
        if (node ==NULL){
            return false;
        }
        return true;
    }
    
    void remove(bgpstream_pfx_t *pfx){
        boost::unique_lock<boost::shared_mutex> lock(mutex_);
        bgpstream_patricia_tree_remove(pt, pfx);
    }
    
    long prefixNum(){
        return bgpstream_patricia_prefix_count(pt, BGPSTREAM_ADDR_VERSION_IPV4)+bgpstream_patricia_prefix_count(pt, BGPSTREAM_ADDR_VERSION_IPV6);
    }
};

class BGPMessage;
class BGPCache;

class BGPTable{
public:
    Trie *routingTrie;
    BGPCache *cache;

    BGPTable(BGPCache *cache):  cache(cache){
        routingTrie = new Trie();
    }
    //modified by zxy
    //BGPMessage *update(BGPMessage *bgpMessage);
    //void updatePath(BGPMessage *bgpMessage, PrefixPath* previousPath);
    Threadsafe_queue<Event*> *queue;
    BGPMessage *update(BGPMessage *bgpMessage,Threadsafe_queue<Event*> *queue);
    void updatePath(BGPMessage *bgpMessage, PrefixPath* previousPath,Threadsafe_queue<Event*> *queue);
    //end of modify
   
    
    void updateBGPentry(PrefixPath* path, bgpstream_pfx_t *pfx, bool withdraw, unsigned int time);
    //modified by zxy
    //void setPathActive(PrefixPath *prefixPath, bgpstream_pfx_t *pfx, TrieElement *trieElement, unsigned int time);
    //void setPathNonActive(PrefixPath *prefixPath, bgpstream_pfx_t *pfx, TrieElement *trieElement, unsigned int time);
    void setPathActive(PrefixPath *prefixPath, bgpstream_pfx_t *pfx, TrieElement *trieElement, unsigned int time,Threadsafe_queue<Event*> *queue);
    void setPathNonActive(PrefixPath *prefixPath, bgpstream_pfx_t *pfx, TrieElement *trieElement, unsigned int time,Threadsafe_queue<Event*> *queue);
    //end of modify
    long size_of();
};

class BGPMessageComparer;
class TableFlagger{
public:
    BGPSource *bgpSource;
    //modified by zxy
    //TableFlagger(PriorityBlockingCollection<BGPMessage *,  PriorityContainer<BGPMessage *, BGPMessageComparer>>
    //      &infifo, BlockingCollection<BGPMessage *> &outfifo, BGPTables *bgpTables, BGPCache *cache, BGPSource *bgpSource, int version);
    TableFlagger(PriorityBlockingCollection<BGPMessage *,  PriorityContainer<BGPMessage *, BGPMessageComparer>>
                 &infifo, BlockingCollection<BGPMessage *> &outfifo, BGPTable *bgpTable, BGPCache *cache, BGPSource *bgpSource,
                 int version,Threadsafe_queue<Event*> *queue);

    void run();
private:
    BlockingCollection<BGPMessage *> &outfifo;
    PriorityBlockingCollection<BGPMessage *,  PriorityContainer<BGPMessage *, BGPMessageComparer>> &infifo, waitingQueue;

    BGPCache *cache;
    BGPTable *bgpTable;
    int version;
    //modified by zxy
    Threadsafe_queue<Event*> *tqueue;
    //end of modify
};





#endif //BGPGEOPOLITICS_BGPTABLES_H
