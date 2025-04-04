#ifndef FILE_UTILS_H
#define FILE_UTILS_H

#include <semaphore.h>
#include <string>

using std::string;

class SemGuard {
    sem_t* sem;
    string makeName(const string& filename);
public:
    SemGuard(const string& name);
    ~SemGuard();
};

#endif
