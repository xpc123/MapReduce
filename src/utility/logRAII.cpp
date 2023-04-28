#include "logRAII.h"


logRAII::logRAII(const std::string& func)
{
    log4cpp::NDC::push(func);
}

logRAII::~logRAII(){
    log4cpp::NDC::pop();
}
