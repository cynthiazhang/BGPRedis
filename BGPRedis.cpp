//
//  BGPRedis.cpp
//  BGPGeo
//
//  Created by zhangxinyi on 2019/3/19.
//  Copyright © 2019年 zhangxinyi. All rights reserved.
//

#include "BGPRedis.hpp"

Redis:: Redis(Threadsafe_queue<Event*> *queue):queue(queue){}

Redis:: ~Redis(){
    this->_connect = NULL;
    this->_reply = NULL;
}

bool Redis::connect(std::string host, int port)
{
    this->_connect = redisConnect(host.c_str(), port);
    if(this->_connect != NULL && this->_connect->err){
        std::cout<<"Redis connection error:"<<this->_connect->errstr<<endl;
        return 0;
    }
    return 1;
}

std::string Redis::get(std::string key){
    this->_reply = (redisReply*)redisCommand(this->_connect, "GET %s", key.c_str());
    if(this->_reply->type == REDIS_REPLY_STRING){
        std::string str = this->_reply->str;
        std::cout<<str<<endl;
        freeReplyObject(this->_reply);
        return str;
    }
    else{
        freeReplyObject(this->_reply);
        string str= "REDIS DOES NOT REPLY A STRING";
        return str;
    }
}

void Redis::set(std::string key, std::string value){
    redisCommand(this->_connect, "SET %s %s", key.c_str(), value.c_str());
}

std::string Redis::keys(std::string key){
    this->_reply = (redisReply*)redisCommand(this->_connect, "KEYS \"*\"", key.c_str());
    std::string str = this->_reply->str;
    std::cout<<str<<endl;
    freeReplyObject(this->_reply);
    return str;
}


void Redis::run(){
    int i = 0;
    Event *popEvent;
    while(1){
        queue->wait_end_pop(popEvent);
        string str = popEvent->toJson();
        //std::cout<<str<<endl;
        string s = std::to_string(i);
        set(s,str);
        //get(s);
        i++;
        popEvent = NULL;
        delete popEvent;
    }
    return;
}


