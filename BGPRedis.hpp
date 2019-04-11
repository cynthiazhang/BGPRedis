//
//  BGPRedis.hpp
//  BGPGeo
//
//  Created by zhangxinyi on 2019/3/19.
//  Copyright © 2019年 zhangxinyi. All rights reserved.
//

#ifndef BGPRedis_hpp
#define BGPRedis_hpp

#include <iostream>
#include <string>
#include <stdio.h>
#include <hiredis/hiredis.h>
#include "BGPGeopolitics.h"

class Redis
{
public:
    
    Redis(Threadsafe_queue<Event*> *queue);
    ~Redis();
    bool connect(std::string host, int port);
    std::string get(std::string key);
    std::string keys(std::string key);
    void set(std::string key, std::string value);
    void run();
    
private:
    Threadsafe_queue<Event *> *queue;
    redisContext* _connect;
    redisReply* _reply;
    
};

#endif /* BGPRedis_hpp */
