#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include <string.h>
#include <signal.h>
#include <sys/time.h>
#include "../include/locker.h"
#include "./buttonrpc-master/buttonrpc.hpp"
#include <bits/stdc++.h>
#include <pthread.h>
#include <dlfcn.h>

#include <log4cpp/Category.hh>
#include <log4cpp/PropertyConfigurator.hh>

#include "logRAII.h"

#include <algorithm>

using namespace std;

log4cpp::Category& logger = log4cpp::Category::getInstance("worker");

#define MAX_REDUCE_NUM 15
//可能造成的bug，考虑write多次写，每次写1024用while读进buf
//c_str()返回的是一个临时指针，值传递没事，但若涉及地址出错

int disabledMapId = 0;   //用于人为让特定map任务超时的Id
int disabledReduceId = 0;   //用于人为让特定reduce任务超时的Id

//定义master分配给自己的map和reduce任务数，实际无所谓随意创建几个，我是为了更方便测试代码是否ok
int m_mapTaskNum;
int m_reduceTaskNum;

//定义实际处理map任务的数组，存放map任务号
//(map任务大于总文件数时，多线程分配ID不一定分配到正好增序的任务号，如10个map任务，总共8个文件，可能就是0,1,2,4,5,7,8,9)

class KeyValue{
public:
    string key;
    string value;
};

//定义的两个函数指针用于动态加载动态库里的map和reduce函数
typedef vector<KeyValue> (*MapFunc)(KeyValue kv);
typedef vector<string> (*ReduceFunc)(vector<KeyValue> kvs, int reduceTaskIdx);
MapFunc mapF;
ReduceFunc reduceF;

//给每个map线程分配的任务ID，用于写中间文件时的命名
int m_MapId = 0;            
pthread_mutex_t map_mutex;
pthread_cond_t cond;
int fileId = 0;

//对每个字符串求hash找到其对应要分配的reduce线程
int ihash(string str){
    int sum = 0;
    for(int i = 0; i < str.size(); i++){
        sum += (str[i] - '0');
    }
    return sum % m_reduceTaskNum;
}

//删除所有写入中间值的临时文件
void removeFiles(){

    logRAII logRAIIObj(__FUNCTION__);
    logger.info("remove all intermediate Files generated last time");


    string path;
    for(int i = 0; i < m_mapTaskNum; i++){
        for(int j = 0; j < m_reduceTaskNum; j++){
            path = "mr-" + to_string(i) + "-" + to_string(j);
            int ret = access(path.c_str(), F_OK);
            if(ret == 0) remove(path.c_str());
        }
    }
}

//取得  key:filename, value:content 的kv对作为map任务的输入
KeyValue getContentForFilePath(char* file){

    logRAII logRAIIObj(__FUNCTION__);
    logger.info("file is: %s", file);

    int fd = open(file, O_RDONLY);
    int length = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);
    char buf[length];
    bzero(buf, length);
    int len = read(fd, buf, length);
    if(len != length){
        perror("read");
        exit(-1);
    }
    KeyValue kv;
    kv.key = string(file);
    kv.value = string(buf);
    close(fd);
    logger.info("key:%s, value:%s", kv.key.c_str(),kv.value.c_str());
    return kv;
}

//将map任务产生的中间值写入临时文件
void writeKV(int fd, const KeyValue& kv){

    logRAII logRAIIObj(__FUNCTION__);

    string tmp = kv.key + ",1 ";
    int len = write(fd, tmp.c_str(), tmp.size());
    logger.info("intermediate:%s,size:%d,len:%d",tmp,tmp.size(),len);
    if(len == -1){
        perror("write");
        exit(-1);
    }
    close(fd);
}

//创建每个map任务对应的不同reduce号的中间文件并调用 -> writeKV 写入磁盘
void writeInDisk(const vector<KeyValue>& kvs, int mapTaskIdx){

    logRAII logRAIIObj(__FUNCTION__);
    logger.info("mapTaskId: %d", mapTaskIdx);

    for(const auto& v : kvs){
        int reduce_idx = ihash(v.key);
        string path;
        path = "mr-" + to_string(mapTaskIdx) + "-" + to_string(reduce_idx);
        logger.info("key:%s, reduce_idx(hash):%d, intermediate path:%s", v.key.c_str(), reduce_idx, path.c_str());
        int ret = access(path.c_str(), F_OK);
        if(ret == -1){
            int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0664);
            writeKV(fd, v);
        }else if(ret == 0){
            int fd = open(path.c_str(), O_WRONLY | O_APPEND);
            writeKV(fd, v);
        }   
    }
}

//以char类型的op为分割拆分字符串
vector<string> split(string text, char op){

    logRAII logRAIIObj(__FUNCTION__);
    logger.info("worker split1 begin");

    int n = text.size();
    vector<string> str;
    string tmp = "";
    for(int i = 0; i < n; i++){
        if(text[i] != op){
            tmp += text[i];
        }else{
            if(tmp.size() != 0) str.push_back(tmp);
            tmp = "";
        }
    }
    return str;
}

//以逗号为分割拆分字符串
string split(string text){

    logRAII logRAIIObj(__FUNCTION__);
    logger.info("worker split2 begin");


    string tmp = "";
    for(int i = 0; i < text.size(); i++){
        if(text[i] != ','){
            tmp += text[i];
        }else break;
    }
    return tmp;
}

//获取对应reduce编号的所有中间文件
vector<string> getAllInterMedfilesForReduceNum(string path, int op){

    logRAII logRAIIObj(__FUNCTION__);
    logger.info("path:%s, reduceTaskNum:%d", path.c_str(), op);


    DIR *dir = opendir(path.c_str());
    vector<string> ret;
    if (dir == NULL)
    {
        printf("[ERROR] %s is not a directory or not exist!", path.c_str());
        return ret;
    }
    struct dirent* entry;
    while ((entry=readdir(dir)) != NULL)
    {
        int len = strlen(entry->d_name);
        int oplen = to_string(op).size();
        if(len - oplen < 5) continue;
        string filename(entry->d_name);
        if(!(filename[0] == 'm' && filename[1] == 'r' && filename[len - oplen - 1] == '-')) continue;
        string cmp_str = filename.substr(len - oplen, oplen);
        if(cmp_str == to_string(op)){
            ret.push_back(entry->d_name);
            logger.info("fileName:%s",entry->d_name);
        }
    }
    closedir(dir);
    return ret;
}

//对于一个ReduceTask，获取所有相关文件并将value的list以string写入vector
//vector中每个元素的形式为"abc 11111";
vector<KeyValue> Myshuffle(int reduceTaskNum){
    logRAII logRAIIObj(__FUNCTION__);
    logger.info("reduceTaskNum:%d", reduceTaskNum);

    string path;
    vector<string> str;
    str.clear();
    vector<string> filename = getAllInterMedfilesForReduceNum(".", reduceTaskNum);
    
    unordered_map<string, string> hash;
    for(int i = 0; i < filename.size(); i++){
        path = filename[i];
        char text[path.size() + 1];
        strcpy(text, path.c_str());
        KeyValue kv = getContentForFilePath(text);
        string context = kv.value;
        vector<string> retStr = split(context, ' ');
        
   
        logger.info("fileName:%s,context:%s,splitted:%s", path.c_str(), context.c_str(), std::accumulate(retStr.begin(),retStr.end(),std::string(),[&](const auto& x, const auto& y){
            return x+y+" ";    
        }).c_str());
        

        str.insert(str.end(), retStr.begin(), retStr.end());
    }

    logger.info("all words:%s",std::accumulate(str.begin(),str.end(),std::string(),[&](const auto& x, const auto& y){
            return x+y+" ";    
        }).c_str());
    for(const auto& a : str){
        hash[split(a)] += "1";
    }
    
    vector<KeyValue> retKvs;
    KeyValue tmpKv;
    for(const auto& a : hash){
        tmpKv.key = a.first;
        tmpKv.value = a.second;
        logger.info("word:%s,fre:%s",a.first.c_str(),a.second.c_str());
        retKvs.push_back(tmpKv);
    }
    sort(retKvs.begin(), retKvs.end(), [](KeyValue& kv1, KeyValue& kv2){
        return kv1.key < kv2.key;
    });
    return retKvs;
}

//paralel field
void* mapWorker(void* arg){

    logRAII logRAIIObj(__FUNCTION__);
    logger.info("worker mapWorker begin");

//1、初始化client连接用于后续RPC;获取自己唯一的MapTaskID
    buttonrpc client;
    client.as_client("127.0.0.1", 5555);
    pthread_mutex_lock(&map_mutex);
    int mapTaskIdx = m_MapId++;
    pthread_mutex_unlock(&map_mutex);
    logger.info("current map task id: %d", mapTaskIdx);
    bool ret = false;
    while(1){
    //2、通过RPC从Master获取任务
    //client.set_timeout(10000);
        ret = client.call<bool>("isMapDone").val();
        if(ret){
            logger.info("master told me that map has been done, broacast condition variable to start reduce process");
            pthread_cond_broadcast(&cond);
            return NULL;
        }
        string taskTmp = client.call<string>("assignMapTask").val();   //通过RPC返回值取得任务，在map中即为文件名
        if(taskTmp == "empty") continue; 
        logger.info("mapTaskId is:%d, get the task: %s from master", mapTaskIdx, taskTmp.c_str());
        pthread_mutex_lock(&map_mutex);

        //------------------------自己写的测试超时重转发的部分---------------------
        //注：需要对应master所规定的map数量，因为是1，3，5被置为disabled，相当于第2，4，6个拿到任务的线程宕机
        //若只分配两个map的worker，即0工作，1宕机，我设的超时时间比较长且是一个任务拿完在拿一个任务，所有1的任务超时后都会给到0，
        if(disabledMapId == 1 || disabledMapId == 3 || disabledMapId == 5){
            disabledMapId++;
            pthread_mutex_unlock(&map_mutex);
            logger.info("current map task id:%d, task:%s, time out!", mapTaskIdx, taskTmp.c_str());
            while(1){
                sleep(2);
            }
        }else{
            disabledMapId++;   
        }
        pthread_mutex_unlock(&map_mutex);
        //------------------------自己写的测试超时重转发的部分---------------------

    //3、拆分任务，任务返回为文件path及map任务编号，将filename及content封装到kv的key及value中
        char task[taskTmp.size() + 1];
        strcpy(task, taskTmp.c_str());
        KeyValue kv = getContentForFilePath(task);

    //4、执行map函数，然后将中间值写入本地
        vector<KeyValue> kvs = mapF(kv);
        logger.info("after mapF, kvs:");
        std::for_each(kvs.begin(),kvs.end(),[&](const auto& kv){logger.info("\tkey:%s,val:%s",kv.key.c_str(),kv.value.c_str());});
        writeInDisk(kvs, mapTaskIdx);

    //5、发送RPC给master告知任务已完成
        logger.info("map task id: %d, has finished the task : %s", mapTaskIdx, taskTmp.c_str());
        client.call<void>("setMapStat", taskTmp);

    }
} 

//用于最后写入磁盘的函数，输出最终结果
void myWrite(int fd, vector<string>& str){

    logRAII logRAIIObj(__FUNCTION__);
    logger.info("worker myWrite begin");

    int len = 0;
    char buf[2];
    sprintf(buf,"\n");
    for(auto s : str){
        len = write(fd, s.c_str(), s.size());
        write(fd, buf, strlen(buf));
        if(len == -1){
            perror("write");
            exit(-1);
        }
    }
}

void* reduceWorker(void* arg){

    logRAII logRAIIObj(__FUNCTION__);

    //removeFiles();
    buttonrpc client;
    client.as_client("127.0.0.1", 5555);
    bool ret = false;
    while(1){
        //若工作完成直接退出reduce的worker线程
        ret = client.call<bool>("Done").val();
        if(ret){
            logger.info("master tole me reduce has been done");
            return NULL;
        }
        int reduceTaskIdx = client.call<int>("assignReduceTask").val();
        if(reduceTaskIdx == -1) continue;
        logger.info("get the reduce task:%d from master", reduceTaskIdx);
        pthread_mutex_lock(&map_mutex);

        //人为设置的crash线程，会导致超时，用于超时功能的测试
        if(disabledReduceId == 1 || disabledReduceId == 3 || disabledReduceId == 5){
            disabledReduceId++;
            pthread_mutex_unlock(&map_mutex);
            logger.info("the reduce task: %d time out!", reduceTaskIdx);
            while(1){
                sleep(2);
            }
        }else{
            disabledReduceId++;   
        }
        pthread_mutex_unlock(&map_mutex);

        //取得reduce任务，读取对应文件，shuffle后调用reduceFunc进行reduce处理
        vector<KeyValue> kvs = Myshuffle(reduceTaskIdx);
        logger.info("after shuffle: %s", std::accumulate(kvs.begin(),kvs.end(),std::string(),[](const auto& x, const auto& y){return x+y.key+":"+y.value+"\t";}).c_str());
        vector<string> ret = reduceF(kvs, reduceTaskIdx);
        logger.info("after reduceF: %s", std::accumulate(ret.begin(),ret.end(),std::string(),[](const auto& x, const auto& y){return x+y+" ";}).c_str());
        vector<string> str;
        for(int i = 0; i < kvs.size(); i++){
            str.push_back(kvs[i].key + " " + ret[i]);
        }
        string filename = "mr-out-" + to_string(reduceTaskIdx);
        int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0664);
        myWrite(fd, str);
        logger.info("final output file:%s,final content needed to write: %s",filename.c_str(), std::accumulate(str.begin(),str.end(),std::string(),[](const auto& x, const auto& y){return x+y+" ";}).c_str());
        close(fd);
        logger.info("the reduce task is: %d has been finished", pthread_self(), reduceTaskIdx);
        client.call<bool>("setReduceStat", reduceTaskIdx);  //最终文件写入磁盘并发起RPCcall修改reduce状态
    }
}

//删除最终输出文件，用于程序第二次执行时清除上次保存的结果
void removeOutputFiles(){
    logRAII logRAIIObj(__FUNCTION__);
    logger.info("remove all output files generated last time");
    string path;
    for(int i = 0; i < MAX_REDUCE_NUM; i++){
        path = "mr-out-" + to_string(i);
        int ret = access(path.c_str(), F_OK);
        if(ret == 0) remove(path.c_str());
    }
}

int main(){
    
    // log
    log4cpp::PropertyConfigurator::configure("/home/xpc/cppProject/6.824/src/MapReduce/test/test_log4cpp/log.conf");
    logRAII logRAIIObj(__FUNCTION__);
    logger.info("worker main begin");

    pthread_mutex_init(&map_mutex, NULL);
    pthread_cond_init(&cond, NULL);

    //运行时从动态库中加载map及reduce函数(根据实际需要的功能加载对应的Func)
    void* handle = dlopen("/home/xpc/cppProject/6.824/src/MapReduce/lib/libmrFunc.so", RTLD_LAZY);
    if (!handle) {
        cerr << "Cannot open library: " << dlerror() << '\n';
        exit(-1);
    }
    mapF = (MapFunc)dlsym(handle, "mapF");
    if (!mapF) {
        cerr << "Cannot load symbol 'hello': " << dlerror() <<'\n';
        dlclose(handle);
        exit(-1);
    }
    reduceF = (ReduceFunc)dlsym(handle, "reduceF");
    if (!mapF) {
        cerr << "Cannot load symbol 'hello': " << dlerror() <<'\n';
        dlclose(handle);
        exit(-1);
    }

    //作为RPC请求端
    buttonrpc work_client;
    work_client.as_client("127.0.0.1", 5555);
    work_client.set_timeout(5000);
    m_mapTaskNum = work_client.call<int>("getMapNum").val();
    m_reduceTaskNum = work_client.call<int>("getReduceNum").val();
    logger.info("ip:%s,port:%d,timeout:%d,mapTaskNum from master:%d, reduceTaskNum from master:%d",
                    "127.0.0.1", 5555, 5000, m_mapTaskNum, m_reduceTaskNum);

    removeFiles();          //若有，则清理上次输出的中间文件
    removeOutputFiles();    //清理上次输出的最终文件

    //创建多个map及reduce的worker线程
    pthread_t tidMap[m_mapTaskNum];
    pthread_t tidReduce[m_reduceTaskNum];
    logger.info("begin map process");
    for(int i = 0; i < m_mapTaskNum; i++){
        pthread_create(&tidMap[i], NULL, mapWorker, NULL);
        pthread_detach(tidMap[i]);
    }
    logger.info("wait for reduce process");
    pthread_mutex_lock(&map_mutex);
    pthread_cond_wait(&cond, &map_mutex);
    pthread_mutex_unlock(&map_mutex);
    logger.info("begin reduce process");
    for(int i = 0; i < m_reduceTaskNum; i++){
        pthread_create(&tidReduce[i], NULL, reduceWorker, NULL);
        pthread_detach(tidReduce[i]);
    }

    while(1){
        if(work_client.call<bool>("Done").val()){
            logger.info("all reduce task has been done");
            break;
        }
        sleep(1);
    }
    printf("worker has done his work!\n");
    //任务完成后清理中间文件，关闭打开的动态库，释放资源
    // removeFiles();
    dlclose(handle);
    pthread_mutex_destroy(&map_mutex);
    pthread_cond_destroy(&cond);
}