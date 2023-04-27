#include <log4cpp/Category.hh>
#include <log4cpp/PropertyConfigurator.hh>
#include "log4cpp/NDC.hh"
#include <thread>
// #define LOG(msg)\
//         do{\
//             log4cpp::Category& logger = log4cpp::Category::getInstance("sample");\
//             logger.info(msg);\
//         }while(0);

struct logRAII{
    logRAII(const std::string& func){
        log4cpp::NDC::push(func);
    }
    void log(const std::string& msg){
        log4cpp::Category& logger = log4cpp::Category::getInstance("sample");
        logger.info(msg);
    }
    ~logRAII(){
        log4cpp::NDC::pop();
    }
};

void test2()
{
    logRAII lR(__FUNCTION__);
   lR.log("enter test2");
   lR.log("enter test22");
   lR.log("enter test222");
}

void test()
{
    logRAII lR(__FUNCTION__);
    lR.log("enter test1");
    lR.log("enter test11");
    test2();
}

int main()
{
    // 加载配置文件
    log4cpp::PropertyConfigurator::configure("/home/xpc/cppProject/6.824/src/MapReduce/test/test_log4cpp/log.conf");
    // 输出日志
    logRAII lR(__FUNCTION__);
    lR.log("enter main!");

    std::thread threads[10];
    for (int i = 0; i < 10; i++) {
        threads[i] = std::thread(test);
    }

    // 等待所有线程结束
    for (int i = 0; i < 10; i++) {
        threads[i].join();
    }
    return 0;
}
