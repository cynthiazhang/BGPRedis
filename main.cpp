#include <iostream>
#include <thread>
#include <list>
#include "BlockingQueue.h"
#include "BGPGeopolitics.h"
#include "BGPSource.h"
#include "cache.h"
#include "BGPGraph.h"
#include "BGPTables.h"
#include "BGPSaver.h"
#include "BGPRedis.hpp"

class Wrapper {
    std::thread source, table1, table2, table3, table4, table5,table6,save, redisthread;
public:
    Wrapper(int t_start, int t_end, int dumpDuration, std::map<std::string, unsigned short int>& collectors ,  std::string& captype, int version) {
        PriorityBlockingCollection<BGPMessage *,  PriorityContainer<BGPMessage *, BGPMessageComparer>> toTableFlag(1000);
        BlockingCollection<BGPMessage *> toSaver(70000);
        BGPGraph g;
        //EventTable eventTable;
        BGPCache cache("resources/as.sqlite", &g,&collectors);
        BGPMessagePool bgpMessagePool(&cache,20000);
        BGPTable *bgpTable = new BGPTable(&cache);
        int numofWorkers = 8;
        vector<std::thread> workers(numofWorkers);
        
        //added by zxy
        Threadsafe_queue<Event*> queue;
        //end of add
        
        cache.fillASCache();
        BGPSource *bgpsource = new BGPSource(&bgpMessagePool, toTableFlag,  t_start, t_end, collectors ,  captype, 4, &cache);
        //modified by zxy
        //TableFlagger *tableFlagger = new TableFlagger(toTableFlag, toSaver, bgpTable, &cache, bgpsource, 4);
        TableFlagger *tableFlagger = new TableFlagger(toTableFlag, toSaver, bgpTable, &cache, bgpsource, 4, &queue);
        
        Redis *redis = new Redis(&queue);
        if(redis->connect("127.0.0.1", 6379))
            redisthread = std::thread(&Redis::run,redis);
        //end of modify

        ScheduleSaver *saver = new ScheduleSaver(&g, t_start, dumpDuration, toSaver, &cache, bgpsource, bgpTable);
        source = std::thread(&BGPSource::run, bgpsource);
        for (int i=0;i<numofWorkers;i++){
            workers[i]=std::thread(&TableFlagger::run, tableFlagger);
        }
        save = std::thread(&ScheduleSaver::run, saver);
        for (int i=0;i<numofWorkers;i++){
            workers[i].join();
        }
        source.join();
        redisthread.join();
    }
    
};


int main(int argc, char **argv) {
    int start =1553630415-24*60*60;
    int end = start+12*60*60;

    std::string mode ="WR";
    int dumpDuration =60;
    
    std::map<std::string, unsigned short int > collectors;
    collectors.insert(pair<string, unsigned short int >("rrc00",0));
    collectors.insert(pair<string, unsigned short int >("rrc01",1));
    collectors.insert(pair<string, unsigned short int >("rrc02",2));
    collectors.insert(pair<string, unsigned short int >("rrc03",3));
    collectors.insert(pair<string, unsigned short int >("rrc04",4));
    collectors.insert(pair<string, unsigned short int >("rrc05",5));
    collectors.insert(pair<string, unsigned short int >("rrc06",6));
    collectors.insert(pair<string, unsigned short int >("rrc09",7));
    collectors.insert(pair<string, unsigned short int >("rrc10",8));
    collectors.insert(pair<string, unsigned short int >("rrc11",9));
    collectors.insert(pair<string, unsigned short int >("rrc12",10));
    collectors.insert(pair<string, unsigned short int >("rrc13",11));
    collectors.insert(pair<string, unsigned short int >("rrc14",12));
    collectors.insert(pair<string, unsigned short int >("rrc15",13));
    collectors.insert(pair<string, unsigned short int >("rrc16",14));
    collectors.insert(pair<string, unsigned short int >("rrc17",15));
    collectors.insert(pair<string, unsigned short int >("rrc18",16));
    collectors.insert(pair<string, unsigned short int >("rrc19",17));
    collectors.insert(pair<string, unsigned short int >("rrc20",18));
    collectors.insert(pair<string, unsigned short int >("rrc21",19));
    Wrapper *w = new Wrapper(start, end, dumpDuration, collectors, mode,4);
    return 0;
}



