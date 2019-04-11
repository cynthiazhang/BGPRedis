//
//  threadsafe_queue.h
//  BGPGeo
//
//  Created by zhangxinyi on 2019/3/20.
//  Copyright © 2019年 zhangxinyi. All rights reserved.
//

#ifndef threadsafe_queue_h
#define threadsafe_queue_h


#include <iostream>
#include <queue>
#include <mutex>
#include <memory>
#include <condition_variable>

template<typename T>
class Threadsafe_queue
{
private:
    mutable std::mutex mut;
    std::queue<std::shared_ptr<T>> data_queue;
    std::condition_variable data_cond;
public:
    Threadsafe_queue(){}
    void wait_end_pop(T& value)
    {
        std::unique_lock<std::mutex> lk(mut);
        data_cond.wait(lk,[this]{return !data_queue.empty();});
        value=std::move(*data_queue.front());
        data_queue.pop();
        return;
    }
    bool try_pop(T& value)
    {
        std::lock_guard<std::mutex> lk(mut);
        if(data_queue.empty())
            return false;
        value=std::move(*data_queue.front());
        data_queue.pop();
        return true;
    }
    
    std::shared_ptr<T> wait_end_pop()
    {
        std::unique_lock<std::mutex> lk(mut);
        data_cond.wait(lk,[this]{return !data_queue.empty();});
        std::shared_ptr<T> res=data_queue.front();
        data_queue.pop();
        return res;
    }
    
    std::shared_ptr<T> try_pop()
    {
        std::lock_guard<std::mutex> lk(mut);
        if(data_queue.empty())
            return std::shared_ptr<T>();
        std::shared_ptr<T> res=data_queue.front();
        data_queue.pop();
        return res;
    }
    
    void push(T new_value)
    {
        std::shared_ptr<T> data(std::make_shared<T>(std::move(new_value)));
        std::lock_guard<std::mutex> lk(mut);
        data_queue.push(data);
        data_cond.notify_one();
        return;
    }
    
    bool empty() const
    {
        std::lock_guard<std::mutex> lk(mut);
        return data_queue.empty();
    }
    
    int size() const{
        std::lock_guard<std::mutex> lk(mut);
        return data_queue.size();
    }
};



#endif /* threadsafe_queue_h */
