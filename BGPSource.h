//
// Created by Kave Salamatian on 17/11/2018.
//


#ifndef BGPGEOPOLITICS_BGPSTREAM_H
#define BGPGEOPOLITICS_BGPSTREAM_H
#include "cache.h"
#include "BGPGeopolitics.h"
#include "BlockingQueue.h"
#include <list>
#include <vector>
#include "BGPEvent.h"

class BGPCache;
class BGPMessage;
class BGPMessageComparer;


class BGPMessagePool{
public:
    BGPCache *cache;
    BlockingCollection<BGPMessage *> bgpMessages;
    int count = 0;
    int capacity;

    BGPMessagePool(BGPCache *cache, int capacity);
    BGPMessage* getBGPMessage(int order, bgpstream_elem_t *elem, unsigned int time, std::string collector, BGPCache* cache);
    void returnBGPMessage(BGPMessage* bgpMessage);
private:
    bool isPoolAvailable();
};


class Trie;
class BGPSource {
public:
    int mode = 0;
    int count = 0;
    //    concurrent_hash_map<unsigned int, BlockingCollection<BGPMessage *> *> inProcess;
    Trie *inProcess;
    bgpstream_t *bs;
    bgpstream_record_t  *record;
    PriorityBlockingCollection<BGPMessage *,  PriorityContainer<BGPMessage *, BGPMessageComparer>> &fifoQueue;
    int t_start, t_end;
    int version;
    BGPCache *cache;
    BGPMessagePool *bgpMessagePool;

    BGPSource(BGPMessagePool *bgpMessagePool,PriorityBlockingCollection<BGPMessage *,  PriorityContainer<BGPMessage *, BGPMessageComparer>> &fifo, int t_start, int t_end, std::map<std::string, unsigned short int> &collectors, std::string &captype, int version, BGPCache *cache);
    
    int run();
    void returnBGPMessage(BGPMessage* bgpMessage);
private:

};


#endif //BGPGEOPOLITICS_BGPSTREAM_H
