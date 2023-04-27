#include <string>

#include <log4cpp/Category.hh>
#include <log4cpp/PropertyConfigurator.hh>
#include "log4cpp/NDC.hh"

class logRAII{
public:
    logRAII(const std::string& category, const std::string& func);
    void log(const std::string& msg);
    ~logRAII();
private:
    std::string m_category;
};