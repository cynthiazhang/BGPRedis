//
// Created by Kave Salamatian on 2018-12-03.
//
#include "BGPTables.h"
#include "BGPGeopolitics.h"
#include "BGPSource.h"

using namespace std;
//modified by zxy
//void BGPTable::updatePath(BGPMessage *bgpMessage, PrefixPath* previousPath){
void BGPTable::updatePath(BGPMessage *bgpMessage, PrefixPath* previousPath,Threadsafe_queue<Event*> *queue){
//end of modify
    PrefixPath *prefixPath= bgpMessage->prefixPath, *newPath;
    //*active=bgpMessage->activePrefixPath, *previous = bgpMessage->previousPrefixPath,
    unsigned int time = bgpMessage->timestamp;
    bgpstream_pfx_t *pfx = (bgpstream_pfx_t *)&bgpMessage->pfx ;
    TrieElement *trieElement = bgpMessage->trieElement;
    
    if ((bgpMessage->type == BGPSTREAM_ELEM_TYPE_ANNOUNCEMENT) || (bgpMessage->type == BGPSTREAM_ELEM_TYPE_RIB)){
        prefixPath->announcementNum++;
        if (bgpMessage->shortPath.size()>0){
            if (bgpMessage->withdraw){
                //This is an implicit withdraw
                bgpMessage->category = AADiff; // implicit withdrawal and replacement with different
                previousPath->AADiff++;
                previousPath->erasePrefix(pfx, time);
                setPathNonActive(previousPath, pfx, trieElement, time, queue);
                newPath = prefixPath;
            }
            if (!prefixPath->active){
                if (bgpMessage->timestamp - prefixPath->lastChange < 300) {
                    //This is a flap
                    prefixPath->Flap++;
                    bgpMessage->category = Flap;
                } else {
                    //this a WADup explicit withdrawal and replacement with identic
                    prefixPath->WADup++;
                    bgpMessage->category = WADup;
                }
                prefixPath->addPrefix(pfx, time);
            } else {
                if (prefixPath->checkPrefix(pfx)){
                    //the prefix is already in the path. This is an AADup
                    bgpMessage->category = AADup;
                    prefixPath->AADup++;
                } else {
                    prefixPath->addPrefix(pfx, time);
                }
            }
            setPathActive(prefixPath, pfx,trieElement, time, queue);
        }
    } else if (bgpMessage->type == BGPSTREAM_ELEM_TYPE_WITHDRAWAL){
        if (previousPath != NULL){
            bgpMessage->category = Withdrawn;
            previousPath->announcementNum++;
            //there is already an active path
            if (!previousPath->checkPrefix(pfx)){
                //it is a WWDup
                bgpMessage->category = WWDup;
                prefixPath->WWDup++;
            } else {
                previousPath->erasePrefix(pfx, time);
                setPathNonActive(previousPath, pfx, trieElement, time, queue);
            }
        }
    }
}

//modified by zxy
//BGPMessage *BGPTable::update(BGPMessage *bgpMessage){
BGPMessage *BGPTable::update(BGPMessage *bgpMessage,Threadsafe_queue<Event*> *queue){
//end of modify
    TrieElement *trieElement;
    bgpstream_pfx_t *pfx;
    bool endOutage=false, globalOutage=false;
    PrefixPath *path, *previousPath=NULL;
    unsigned int time;
    unsigned short int collector;
    pair<bool,PrefixPath*> ret;

    pfx=  (bgpstream_pfx_t *)&bgpMessage->pfx;
    path = bgpMessage->prefixPath;
    time = bgpMessage->timestamp;
    trieElement = bgpMessage->trieElement;
    collector = cache->collectors[bgpMessage->collector];
    if ((bgpMessage->type == BGPSTREAM_ELEM_TYPE_ANNOUNCEMENT) || (bgpMessage->type == BGPSTREAM_ELEM_TYPE_RIB)){
        ret = trieElement->addPath(collector, path,time);
        endOutage = ret.first;
        previousPath = ret.second;
    } else {
        if (bgpMessage->type == BGPSTREAM_ELEM_TYPE_WITHDRAWAL){
            ret = trieElement->erasePath(collector, path, time);
            globalOutage = ret.first;
            previousPath = ret.second;
        }
    }
    if (previousPath != NULL){
        // PreviousPath != NULL it is an implicit withdraw
        previousPath->erasePrefix(pfx, bgpMessage->timestamp);
        bgpMessage->withdraw = true;
    }
    trieElement->cTime = bgpMessage->timestamp;
    //modified by zxy
    //updatePath(bgpMessage, previousPath);
    updatePath(bgpMessage, previousPath, queue);
    //end of modify
    
    if (globalOutage){
        //There is a global prefix withdraw
        //added by zxy
        Event *GloOutageEv = new Event();
        GloOutageEv->type = OUTAGE_EV;
        GloOutageEv->s_type = "OUTAGE_EV";
        GloOutageEv->time = time;
        queue->push(GloOutageEv);
        //end of add
        for (auto it= trieElement->asSet.begin(); it != trieElement->asSet.end();it++){
            cache->asCache.find(*it).second->removePrefix(pfx, bgpMessage->timestamp);
        }
    }
    return bgpMessage;
    //updateEventTable(bgpMessage);
}


void BGPTable::setPathActive(PrefixPath *prefixPath, bgpstream_pfx_t *pfx, TrieElement* trieElement, unsigned int time, Threadsafe_queue<Event*> *queue){
    AS *as, *as1;
    unsigned int tmp, peer, asn;
    map<unsigned int, AS *>::iterator it;
    concurrent_unordered_set<PrefixPeer *>::const_iterator it5;
    map<int, boost::graph_traits<Graph>::vertex_descriptor>::iterator it6;
    
    asn = prefixPath->getDest();
    peer = prefixPath->getPeer();
    
    as=cache->asCache.find(asn).second;
    //added by zxy
    as->queue = queue;
    //end of add
    as->addPrefix(pfx, time);
    as->activePrefixNum = as->activePrefixTrie->prefixNum() ;
    as->allPrefixNum =  as->activePrefixNum+ as->inactivePrefixTrie->prefixNum();
    trieElement->addAS(as->asNum);
    if (trieElement->asSet.size()>1){
        if (trieElement->checkHijack()){
            as->status |= HIJACKING;
            for (auto it = trieElement->asSet.begin();it != trieElement->asSet.end();it++){
                if (*it != as->asNum){
                    as1 = cache->asCache.find(*it).second;
                    as1->status |= HIJACKED;
                    //added by zxy
                    Event *hijackEv = new Event();
                    hijackEv->type = HIJACK_EV;
                    hijackEv->s_type = "HIJACK_EV";
                    hijackEv->time = time;
                    hijackEv->asn = as->asNum;
                    hijackEv->hijackedAsn = as1->asNum;
                    queue->push(hijackEv);
                    //end of add
                }
            }
        }
    }
    if (prefixPath->shortPathLength>1){
        unsigned int src,dst;
        for (int i=0; i<prefixPath->shortPathLength-1;i++){
            src=prefixPath->shortPath[i];
            dst=prefixPath->shortPath[i+1];
            if (src>dst){
                tmp = src;
                src = dst;
                dst = tmp;
            }
            AS *srcAS= cache->asCache.find(src).second;
            AS *dstAS= cache->asCache.find(dst).second;
            string linkId= to_string(src)+"|"+to_string(dst);
            Link *link;
            bool found;
            pair<bool, Link *> ret = cache->linksMap.find(linkId);
            found = ret.first;
            link = ret.second;
            if (!found){
                link = new Link(src, dst, time);
                cache->linksMap.checkinsert(link);
            }
            link->activePathsMap.checkinsert(prefixPath);
            link->cTime = time;
            boost::graph_traits<Graph>::vertex_descriptor v0, v1;
            v0=0;
            auto ret1 = cache->bgpg->asnToVertex.find(src);
            if (ret1.first){
                v0=ret1.second;
            } else {
                boost::graph_traits<Graph>::vertex_descriptor v0 = cache->bgpg->add_vertex(VertexP{to_string(srcAS->asNum), srcAS->country, srcAS->name, srcAS->cTime, 0, 0});
                cache->bgpg->asnToVertex.insert(pair<int,boost::graph_traits<Graph>::vertex_descriptor >(srcAS->asNum, v0));
            }
            v1=0;
            ret1 = cache->bgpg->asnToVertex.find(dst);
            if (ret1.first){
                v1=ret1.second;
            } else {
                boost::graph_traits<Graph>::vertex_descriptor v1 = cache->bgpg->add_vertex(VertexP{to_string(dstAS->asNum), dstAS->country, dstAS->name, dstAS->cTime, 0, 0});
                cache->bgpg->asnToVertex.insert(pair<int,boost::graph_traits<Graph>::vertex_descriptor >(dstAS->asNum, v1));
            }
            if (cache->bgpg->set_edge(v0,v1, EdgeP{(long)link->activePathsMap.size(), 1, link->cTime})) {
                srcAS->reconnect();
                dstAS->reconnect();
            }
        }
    }
    boost::graph_traits<Graph>::vertex_descriptor v;
    auto ret2 = cache->bgpg->asnToVertex.find(asn);
    v=0;
    if (ret2.first){
        v = ret2.second;
    }
    cache->bgpg->set_vertex(v,VertexP{to_string(as->asNum), as->country, as->name, time, (int)as->activePrefixNum, (int)as->allPrefixNum });
}

void BGPTable::setPathNonActive(PrefixPath *prefixPath, bgpstream_pfx_t *pfx, TrieElement* trieElement, unsigned int time,Threadsafe_queue<Event*> *queue){
    unsigned int tmp, peer, asn;
    AS *as;
    set<bgpstream_pfx_storage_t *>::iterator it5;
    concurrent_hash_map<int,boost::graph_traits<Graph>::vertex_descriptor>::accessor acc2;
    Link * link;
    asn = prefixPath->getDest();
    peer = prefixPath->getPeer();
    if (prefixPath->shortPathLength>1){
        unsigned int src,dst;
        AS *srcAS,*dstAS;
        for (int i=0; i<prefixPath->shortPathLength-1;i++){
            src=prefixPath->shortPath[i];
            dst=prefixPath->shortPath[i+1];
            if (src>dst){
                tmp = src;
                src = dst;
                dst = tmp;
            }
            srcAS= cache->asCache.find(src).second;
            dstAS= cache->asCache.find(dst).second;
            string linkId = to_string(src) + "|" + to_string(dst);
            bool found;
            pair<bool, Link *> ret = cache->linksMap.find(linkId);
            found = ret.first;
            link = ret.second;
            if (!found){
                link = new Link(src, dst, time);
                cache->linksMap.checkinsert(link);
            }
            link->activePathsMap.erase(prefixPath->str());
            link->cTime = time;
            boost::graph_traits<Graph>::vertex_descriptor v0, v1;
            auto ret1 = cache->bgpg->asnToVertex.find(src);
            v0 = ret1.second;
            ret1=cache->bgpg->asnToVertex.find(dst);
            v1 = ret1.second;
            acc2.release();
            if (link->activePathsMap.size() == 0){
                cache->bgpg->remove_edge(v0, v1);
                if (cache->bgpg->in_degree(v0)==0) {
                    srcAS->disconnect();
                }
                if (cache->bgpg->in_degree(v1)==0) {
                    dstAS->disconnect();
                }
            } else{
                boost::graph_traits<Graph>::edge_descriptor e;
                cache->bgpg->set_edge(v0,v1, EdgeP{(long)link->activePathsMap.size(), 1, link->cTime});
            }
        }
        as = dstAS;
    } else {
        as = cache->asCache.find(prefixPath->getDest()).second;
    }
    as->cTime = time;
    auto ret2=cache->bgpg->asnToVertex.find(asn);
    boost::graph_traits<Graph>::vertex_descriptor v = ret2.second;
    cache->bgpg->set_vertex(v,VertexP{to_string(as->asNum), as->country, as->name, time, (int)as->activePrefixNum, (int)as->allPrefixNum } );
}



long BGPTable::size_of(){
    long size=4+4*8;
    return size;
}

void TableFlagger::run(){
    BGPMessage *bgpMessage,*bgpMessage1;
    TrieElement *trieElement;
    PriorityBlockingCollection<BGPMessage *,  PriorityContainer<BGPMessage *, BGPMessageComparer>> *queue;
     while(true){
        if (infifo.try_take(bgpMessage, std::chrono::milliseconds(120000))==BlockingCollectionStatus::TimedOut){
            break;
        } else {
            bgpstream_pfx_t *pfx = (bgpstream_pfx_t *)&(bgpMessage->pfx);
            trieElement= (TrieElement *)bgpTable->routingTrie->search(pfx);
            if (trieElement == NULL){
                trieElement= new TrieElement();
                bgpTable->routingTrie->insert(pfx, trieElement);
            }
            bgpMessage->trieElement = trieElement;
            queue = &trieElement->inprocessQueue;
            if (!queue->is_empty()) {
                queue->add(bgpMessage);
            } else {
                bgpMessage->preparePath();
                waitingQueue.add(bgpMessage);
            }
            while (!waitingQueue.is_empty()){
                waitingQueue.take(bgpMessage1);
                //modified by zxy
                //bgpMessage = bgpTable->update(bgpMessage1);
                bgpMessage = bgpTable->update(bgpMessage1,tqueue);
                //end of modify
            
                outfifo.add(bgpMessage1);
                bgpSource->bgpMessagePool->returnBGPMessage(bgpMessage1);
                if (!bgpMessage1->trieElement->inprocessQueue.is_empty()){
                    queue->take(bgpMessage1);
                    waitingQueue.add(bgpMessage1);
                }
            }
        }
    }
    cout<< "Data Famine Table"<<endl;

}

//void BGPSource::returnBGPMessage(BGPMessage* bgpMessage){
//    BGPMessage* bgpMessage1;
//    bgpstream_pfx_t *pfx= (bgpstream_pfx_t *)&(bgpMessage->pfx);
//    BlockingCollection<BGPMessage *> *queue, unreturnedQueue;
//    queue=(BlockingCollection<BGPMessage *> *)inProcess->search(pfx);
//    if (queue != NULL){
//        if (!queue->is_empty()){
//            queue->take(bgpMessage1);
//            bgpMessage1->preparePath();
//            auto status = fifoQueue.try_add(bgpMessage1);
//            if (status == BlockingCollectionStatus::TimedOut){
//                waitingQueue.add(bgpMessage1);
//            }
//        } else {
//            inProcess->remove(pfx);
//            delete queue;
//        }
//    }
//}

//modified by zxy
//TableFlagger::TableFlagger(PriorityBlockingCollection<BGPMessage *,  PriorityContainer<BGPMessage *, BGPMessageComparer>>
//                           &infifo, BlockingCollection<BGPMessage *> &outfifo, BGPTable *bgpTable, BGPCache *cache,
//                           BGPSource *bgpSource, int version): cache(cache), infifo(infifo), outfifo(outfifo),
//                           version(version),bgpSource(bgpSource), bgpTable(bgpTable){}
TableFlagger::TableFlagger(PriorityBlockingCollection<BGPMessage *,  PriorityContainer<BGPMessage *, BGPMessageComparer>>
                           &infifo, BlockingCollection<BGPMessage *> &outfifo, BGPTable *bgpTable, BGPCache *cache,
                           BGPSource *bgpSource,int version,Threadsafe_queue<Event*> *queue): cache(cache), infifo(infifo), outfifo(outfifo), version(version),bgpSource(bgpSource), bgpTable(bgpTable),tqueue(queue){}


