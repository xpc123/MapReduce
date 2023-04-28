#include <string>

#include <log4cpp/Category.hh>
#include <log4cpp/PropertyConfigurator.hh>
#include "log4cpp/NDC.hh"

class logRAII{
public:
    logRAII(const std::string& func);
    ~logRAII();
};