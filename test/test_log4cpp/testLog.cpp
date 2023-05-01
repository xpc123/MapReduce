#include <log4cpp/Category.hh>
#include <log4cpp/PropertyConfigurator.hh>
#include "log4cpp/NDC.hh"
#include <thread>

#include "../../include/logRAII.h"

log4cpp::Category& logger = log4cpp::Category::getInstance("worker");

void test2()
{
    logRAII lR(__FUNCTION__);
   logger.info("enter test2");
   logger.info("enter test22");
   logger.info("enter test222");
}

void test()
{
    logRAII lR(__FUNCTION__);
    logger.info("enter test1");
    logger.info("enter test11");
    test2();
}

int main()
{
    // 加载配置文件
    log4cpp::PropertyConfigurator::configure("/home/xpc/cppProject/6.824/src/MapReduce/test/test_log4cpp/log.conf");
    // 输出日志
    logRAII lR(__FUNCTION__);

    std::string xpc="xinpengcheng";
    logger.info("enter main:%s", xpc.c_str());

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
