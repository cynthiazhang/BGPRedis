//
// Created by Kave Salamatian on 24/11/2018.
//

#ifndef BGPGEOPOLITICS_BGPGEOPOLITICS_H
#define BGPGEOPOLITICS_BGPGEOPOLITICS_H
#include "stdint.h"
extern "C" {
#include "bgpstream.h"
}
//#include "cache.h"
#include <set>
#include <mutex>
#include <condition_variable>
#include <list>
#include <vector>
#include <boost/thread/shared_mutex.hpp>
#include "threadsafe_queue.h"
#include "BGPEvent.h"

using namespace std;

class BGPCache;
class PrefixPeer;
class PrefixPath;
class Path;
class TrieElement;

enum Category{None, AADiff,AADup, WADup, WWDup, Flap, Withdrawn};


class BGPMessage{
public:
    int poolOrder;
    long messageOrder;
    string collector;
    bgpstream_elem_type_t  type;
    uint32_t  timestamp;
    bgpstream_addr_storage_t  peerAddress;
    uint32_t  peerASNumber;
    bgpstream_addr_storage_t  nextHop;
    bgpstream_pfx_storage_t pfx;
    TrieElement* trieElement=NULL;
    vector<unsigned int> asPath;
    vector<unsigned int> shortPath;
    BGPCache *cache;
    PrefixPath *prefixPath=NULL;
    bool newPath = false;
    bool withdraw = false;
    Category category = None;

    BGPMessage(BGPCache *cache, int order);
    void fill(long order, bgpstream_elem_t *elem, unsigned int time, string collector, BGPCache *cache);
    void preparePath();
    double fusionRisks(double geoRisk, double secuRisk, double otherRisk);
    void shortenPath();
    string pathString();
    unsigned int getIP();
};

class BGPMessageComparer {
public:
    BGPMessageComparer() {}

    int operator() (const BGPMessage* bgpMessage, const BGPMessage* new_bgpMessage) {
        if (bgpMessage->messageOrder < new_bgpMessage->messageOrder)
            return -1;
        else if (bgpMessage->messageOrder > new_bgpMessage->messageOrder)
            return 1;
        else
            return 0;
    }
};

template < typename T> class ThreadSafeSet{
public:
    std::set<T> set;
    bool insert(const T& val){
        boost::unique_lock<boost::shared_mutex> lock(mutex_);
        return set.insert(val).second;
    }
    
    long erase(const T& val){
        boost::unique_lock<boost::shared_mutex> lock(mutex_);
        return set.erase(val);
    }
    
    long size(){
        boost::shared_lock<boost::shared_mutex> lock(mutex_);
        return set.size();
    }
    
    typename std::set<T>::iterator find(const T& val){
        boost::shared_lock<boost::shared_mutex> lock(mutex_);
        return set.find(val);
    }
    
    typename std::set<T>::iterator end(){
        boost::shared_lock<boost::shared_mutex> lock(mutex_);
        return set.end();
    }
    
    typename std::set<T>::iterator begin(){
        return set.begin();
    }
    
    long size_of(){
        boost::shared_lock<boost::shared_mutex> lock(mutex_);
        long sum=0;
        for_each(set.begin(), set.end(),[&] (T val) {
            sum += val->size_of();});
        return sum;
    }
    
private:
    mutable boost::shared_mutex mutex_;
};

template< typename KeyType,typename ValueType> class MyThreadSafeMap{
private:
    mutable boost::shared_mutex mutex_;
public:
    map<KeyType, ValueType> map;
    
    bool update(pair<KeyType,ValueType> p){
        boost::unique_lock<boost::shared_mutex> lock(mutex_);
        map[p.first] = p.second;
        return true;
    }
    
    pair<bool, ValueType> insert(pair<KeyType,ValueType> p){
        boost::unique_lock<boost::shared_mutex> lock(mutex_);
        auto ret=map.insert(p);
        ValueType tmp=ret.first->second;
        bool done = ret.second;
        return pair<bool,ValueType>(done,tmp);
    }
    
    bool erase(KeyType key){
        boost::unique_lock<boost::shared_mutex> lock(mutex_);
        return map.erase(key);
    }
    
    long size(){
        boost::shared_lock<boost::shared_mutex> lock(mutex_);
        return map.size();
    }
    
    pair<bool, ValueType> find(const KeyType key){
        boost::shared_lock<boost::shared_mutex> lock(mutex_);
        auto it=map.find(key);
        if (it == map.end()){
            return pair<bool, ValueType>(false, (ValueType)NULL);
        } else {
            return pair<bool, ValueType>(true, it->second);
        }
    }
    
    long size_of(){
        boost::shared_lock<boost::shared_mutex> lock(mutex_);
        long sum=0;
        for_each(map.begin(), map.end(),[&] (pair<KeyType, ValueType> p) {
            sum += p.second->size_of();});
        return sum;
        
    }
    
};


template< typename ValueType, typename HashSize> class MyThreadSafeHashMap{
private:
    mutable boost::shared_mutex mutex_;
public:
    multimap<HashSize, ValueType> map;
    HashSize hash(string str){
        HashSize h=(HashSize)std::hash<string>{}(str);
        return h;
    }
    
    HashSize checkinsert(ValueType& value){
        bool found;
        ValueType val;
        string str=value->str();
        HashSize h = hash(str);
        boost::unique_lock<boost::shared_mutex> lock(mutex_);
        typename std::multimap<HashSize, ValueType>::iterator beginit, endit, it;
        auto ret=map.equal_range(hash(str));
        found = false;
        val = NULL;
        for(it=ret.first;it!=ret.second;it++){
            if (it->second->str()==str){
                found=true;
                val=it->second;
                break;
            }
        }
        if (!found){
            map.insert(pair<HashSize, ValueType>(h, value));
        }
        return h;
    }
    
    HashSize insert(ValueType& value){
        string str=value->str();
        HashSize h = hash(str);
        boost::unique_lock<boost::shared_mutex> lock(mutex_);
        map.insert(pair<HashSize, ValueType>(h, value));
        return h;
    }
    
    
    bool erase(string str){
        boost::upgrade_lock<boost::upgrade_mutex> lock(mutex_);
        typename std::multimap<HashSize, ValueType>::iterator beginit, endit, it;
        pair<typename multimap<HashSize, ValueType>::iterator,typename multimap<HashSize, ValueType>::iterator>(beginit,endit) = map.equal_range(hash(str));
        for(it=beginit;it!=endit;it++){
            if (it->second->str()==str){
                boost::upgrade_to_unique_lock<boost::shared_mutex> writeLock(lock);
                map.erase(it);
                return true;
            }
        }
        return false;
    }
    
    long size(){
        boost::shared_lock<boost::shared_mutex> lock(mutex_);
        return map.size();
    }
    
    pair<bool, ValueType> find(const string str){
        boost::shared_lock<boost::shared_mutex> lock(mutex_);
        typename std::multimap<HashSize, ValueType>::iterator beginit, endit, it;
        auto ret=map.equal_range(hash(str));
        for(it=ret.first;it!=ret.second;it++){
            if (it->second->str()==str){
                return  std::make_pair(true,it->second);
            }
        }
        return std::make_pair(false,(ValueType)NULL);
    }
    
    long size_of(){
        boost::shared_lock<boost::shared_mutex> lock(mutex_);
        long sum=0;
        for_each(map.begin(), map.end(),[&] (pair<HashSize, ValueType> p) {
            sum += p.second->size_of();});
        return sum;
    }
};

#endif //BGPGEOPOLITICS_BGPGEOPOLITICS_H

