#include <../../include/logRAII.h>


logRAII::logRAII(const std::string& category, const std::string& func): m_category(category)
{
    log4cpp::NDC::push(func);
}

void logRAII::log(const std::string& msg){
    log4cpp::Category& logger = log4cpp::Category::getInstance(m_category);
    logger.info(msg);
}

logRAII::~logRAII(){
    log4cpp::NDC::pop();
}
