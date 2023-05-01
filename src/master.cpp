#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include "../include/locker.h"
#include "./buttonrpc-master/buttonrpc.hpp"
#include <bits/stdc++.h>

#include <log4cpp/Category.hh>
#include <log4cpp/PropertyConfigurator.hh>

#include "logRAII.h"

using namespace std;

#define MAP_TASK_TIMEOUT 3
#define REDUCE_TASK_TIMEOUT 5

log4cpp::Category& logger = log4cpp::Category::getInstance("master");

class Master{
public:
    Master(int mapNum = 8, int reduceNum = 8);  //带缺省值的有参构造，也可通过命令行传参指定，我偷懒少打两个数字直接放构造函数里
    int getMapNum(){                            
        return m_mapNum;
    }
    int getReduceNum(){
        return m_reduceNum;
    }
    string assignMapTask();                        //分配map任务的函数，RPC
    int assignReduceTask();                     //分配reduce任务的函数，RPC
    void setMapStat(string filename);           //设置特定map任务完成的函数，RPC
    bool isMapDone();                           //检验所有map任务是否完成，RPC
    void setReduceStat(int taskIndex);          //设置特定reduce任务完成的函数，RPC
    bool Done();                                //判断reduce任务是否已经完成
    bool getFinalStat(){                        //所有任务是否完成，实际上reduce完成就完成了，有点小重复
        return m_done;
    }
    void GetAllFile(char* file[], int index);   //从argv[]中获取待处理的文件名
private:
    static void* waitMapTask(void *arg);        //回收map的定时线程
    static void* waitReduceTask(void* arg);     //回收reduce的定时线程
    static void* waitTime(void* arg);           //用于定时的线程
    void waitMap(string filename);
    void waitReduce(int reduceIdx);
private:
    bool m_done;
    list<char *> m_allMapTaskList;                        //所有map任务的工作队列, that's fileName
    locker m_assign_lock;                       //保护共享数据的锁
    int m_fileNum;                                //从命令行读取到的文件总数
    int m_mapNum;
    int m_reduceNum;
    unordered_map<string, int> m_finishedMapTask; //存放所有完成的map任务对应的文件名, modified by worker through RPC
    unordered_map<int, int> m_finishedReduceTask; //存放所有完成的reduce任务对应的reduce编号
    vector<int> m_allReduceTaskList;                    //所有reduce任务的工作队列
    vector<string> m_pendingMapWork;              //正在处理的map任务，分配出去就加到这个队列，用于判断超时处理重发
    int m_curMapIndex;                            //当前处理第几个map任务
    int m_curReduceIndex;                         //当前处理第几个reduce任务
    vector<int> m_runningReduceWork;              //正在处理的reduce任务，分配出去就加到这个队列，用于判断超时处理重发
};


Master::Master(int mapNum, int reduceNum):m_done(false), m_mapNum(mapNum), m_reduceNum(reduceNum){
    
    logRAII logRAIIObj(__FUNCTION__);
    logger.info("Master construction: mapNum is %d, reduceNum is %d", mapNum, reduceNum);
    
    m_allMapTaskList.clear();
    m_finishedMapTask.clear();
    m_finishedReduceTask.clear();
    m_pendingMapWork.clear();
    m_runningReduceWork.clear();
    m_curMapIndex = 0;
    m_curReduceIndex = 0;
    if(m_mapNum <= 0 || m_reduceNum <= 0){
        throw std::exception();
    }
    for(int i = 0; i < reduceNum; i++){
        m_allReduceTaskList.emplace_back(i);
    }
}

void Master::GetAllFile(char* file[], int argc){

    logRAII logRAIIObj(__FUNCTION__);
    logger.info("get all file names from argv");

    for(int i = 1; i < argc; i++){
        m_allMapTaskList.emplace_back(file[i]);           //xpc: file[i-1], right?
        logger.info("the %dth file is: %s", i,file[i]);
    }
    m_fileNum = argc - 1;
}

//map的worker只需要拿到对应的文件名就可以进行map
string Master::assignMapTask(){
    logRAII logRAIIObj(__FUNCTION__);

    if(isMapDone()) return "empty";
    if(!m_allMapTaskList.empty()){
        m_assign_lock.lock();
        char* task = m_allMapTaskList.back(); //从工作队列取出一个待map的文件名
        m_allMapTaskList.pop_back();            
        m_assign_lock.unlock();
        waitMap(string(task));      //调用waitMap将取出的任务加入正在运行的map任务队列并等待计时线程
        return string(task);
    }
    //m_hashSet.erase(task);
    return "empty";
}

void* Master::waitMapTask(void* arg){

    logRAII logRAIIObj(__FUNCTION__);

    Master* map = (Master*)arg;
    // printf("wait maphash is %p\n", &map->master->m_finishedMapTask);
    void* status;
    pthread_t tid;
    char op = 'm';
    pthread_create(&tid, NULL, waitTime, &op);      //wait for worker's response
    pthread_join(tid, &status);                     //join方式回收实现超时后解除阻塞,
    map->m_assign_lock.lock();
    //若超时后在对应的hashmap中没有该map任务完成的记录，重新将该任务加入工作队列
    if(!map->m_finishedMapTask.count(map->m_pendingMapWork[map->m_curMapIndex])){
        logger.info("filename : %s is timeout, it'll be readded into the m_allMapTaskList", 
                        map->m_pendingMapWork[map->m_curMapIndex].c_str());

        const char* text = map->m_pendingMapWork[map->m_curMapIndex].c_str();//这钟方式加入list后不会变成空字符串
        map->m_allMapTaskList.push_back((char*)text);
        map->m_curMapIndex++;
        map->m_assign_lock.unlock();
        return NULL;
    }
    
    logger.info("map for filename : %s is finished at idx : %d\n", map->m_pendingMapWork[map->m_curMapIndex].c_str(), map->m_curMapIndex);
    
    map->m_curMapIndex++;
    map->m_assign_lock.unlock();
}

void Master::waitMap(string filename){

    logRAII logRAIIObj(__FUNCTION__);

    m_assign_lock.lock();
    m_pendingMapWork.push_back(string(filename));  //将分配出去的map任务加入正在运行的工作队列
    m_assign_lock.unlock();

    logger.info("task: %s has been added into m_pendingMapWork", filename.c_str());
    
    pthread_t tid;
    pthread_create(&tid, NULL, waitMapTask, this); //创建一个用于回收计时线程及处理超时逻辑的线程
    pthread_detach(tid);
}

//分map任务还是reduce任务进行不同时间计时的计时线程
void* Master::waitTime(void* arg){

    logRAII logRAIIObj(__FUNCTION__);

    char* op = (char*)arg;
    if(*op == 'm'){
        logger.info("map task timeout: %d", MAP_TASK_TIMEOUT);
        sleep(MAP_TASK_TIMEOUT);
    }else if(*op == 'r'){
        logger.info("reduce task timeout: %d", MAP_TASK_TIMEOUT);
        sleep(REDUCE_TASK_TIMEOUT);
    }
}

void* Master::waitReduceTask(void* arg){

    logRAII logRAIIObj(__FUNCTION__);

    Master* reduce = (Master*)arg;
    void* status;
    pthread_t tid;
    char op = 'r';
    pthread_create(&tid, NULL, waitTime, &op);
    pthread_join(tid, &status);
    reduce->m_assign_lock.lock();
    //若超时后在对应的hashmap中没有该reduce任务完成的记录，将该任务重新加入工作队列
    if(!reduce->m_finishedReduceTask.count(reduce->m_runningReduceWork[reduce->m_curReduceIndex])){
        reduce->m_allReduceTaskList.emplace_back(reduce->m_runningReduceWork[reduce->m_curReduceIndex]);
        logger.info(" reduce task: %d is timeout", reduce->m_runningReduceWork[reduce->m_curReduceIndex]);
        reduce->m_curReduceIndex++;
        reduce->m_assign_lock.unlock();
        return NULL;
    }
    logger.info("%d reduce is completed\n", reduce->m_runningReduceWork[reduce->m_curReduceIndex]);
    reduce->m_curReduceIndex++;
    reduce->m_assign_lock.unlock();
}

void Master::waitReduce(int reduceIdx){
    logRAII logRAIIObj(__FUNCTION__);
    logger.info("reduceIdx is: %d", reduceIdx);

    m_assign_lock.lock();
    m_runningReduceWork.push_back(reduceIdx); //将分配出去的reduce任务加入正在运行的工作队列
    m_assign_lock.unlock();
    pthread_t tid;
    pthread_create(&tid, NULL, waitReduceTask, this); //创建一个用于回收计时线程及处理超时逻辑的线程
    pthread_detach(tid);
}

void Master::setMapStat(string filename){
    logRAII logRAIIObj(__FUNCTION__);
    logger.info("set filename:%s to 1", filename.c_str());

    m_assign_lock.lock();
    m_finishedMapTask[filename] = 1;  //通过worker的RPC调用修改map任务的完成状态
    // printf("map task : %s is finished, maphash is %p\n", filename.c_str(), &m_finishedMapTask);
    m_assign_lock.unlock();
    return;
}

bool Master::isMapDone(){
    logRAII logRAIIObj(__FUNCTION__);

    m_assign_lock.lock();
    if(m_finishedMapTask.size() != m_fileNum){  //当统计map任务的hashmap大小达到文件数，map任务结束
        m_assign_lock.unlock();
        logger.info("Map undone");
        return false;
    }
    m_assign_lock.unlock();
    logger.info("Map done");
    return true;
}

int Master::assignReduceTask(){
    logRAII logRAIIObj(__FUNCTION__);
    logger.info("master assignReduceTask begin");
    
    if(Done()) return -1;
    if(!m_allReduceTaskList.empty()){
        m_assign_lock.lock();
        int reduceIdx = m_allReduceTaskList.back(); //取出reduce编号
        m_allReduceTaskList.pop_back();
        m_assign_lock.unlock();
        waitReduce(reduceIdx);    //调用waitReduce将取出的任务加入正在运行的reduce任务队列并等待计时线程
        return reduceIdx;
    }
    return -1;
}


void Master::setReduceStat(int taskIndex){
    logRAII logRAIIObj(__FUNCTION__);
    logger.info("set taskIndex: %d to 1", taskIndex);
    m_assign_lock.lock();
    m_finishedReduceTask[taskIndex] = 1;  //通过worker的RPC调用修改reduce任务的完成状态
    m_assign_lock.unlock();
    return;
}

bool Master::Done(){
    logRAII logRAIIObj(__FUNCTION__);
    m_assign_lock.lock();
    int len = m_finishedReduceTask.size(); //reduce的hashmap若是达到reduceNum，reduce任务及总任务完成,xpc: why?
    m_assign_lock.unlock();
    logger.info("reduce task has been done: %s", (len==m_reduceNum) ? "yes" : "no");
    if(len == m_reduceNum) printf("reduce task has been done");
    return len == m_reduceNum;
}

int main(int argc, char* argv[]){
    if(argc < 2){
        cout<<"missing parameter! The format is ./Master pg*.txt"<<endl;
        exit(-1);
    }

    // log
    log4cpp::PropertyConfigurator::configure("/home/xpc/cppProject/6.824/src/MapReduce/test/test_log4cpp/log.conf");
    logRAII logRAIIObj(__FUNCTION__);
    logger.info("master main begin");
    // alarm(10);
    buttonrpc server;
    server.as_server(5555);
    Master master(3, 4);
    master.GetAllFile(argv, argc);
    server.bind("getMapNum", &Master::getMapNum, &master);
    server.bind("getReduceNum", &Master::getReduceNum, &master);
    server.bind("assignMapTask", &Master::assignMapTask, &master);
    server.bind("setMapStat", &Master::setMapStat, &master);
    server.bind("isMapDone", &Master::isMapDone, &master);
    server.bind("assignReduceTask", &Master::assignReduceTask, &master);
    server.bind("setReduceStat", &Master::setReduceStat, &master);
    server.bind("Done", &Master::Done, &master);
    server.run();
    return 0;
}