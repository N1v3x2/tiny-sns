#include <algorithm>
#include <fcntl.h>
#include "file_utils.h"

#include <iostream>

string SemGuard::makeName(const string& filename) {
    string semName = filename;
    std::replace(semName.begin(), semName.end(), '/', '_');
    semName.insert(0, 1, '/');
    return semName;
}

SemGuard::SemGuard(const string& name) {
    sem = sem_open(makeName(name).c_str(), O_CREAT, 0644, 1);
    sem_wait(sem);
}

SemGuard::~SemGuard() {
    sem_post(sem);
    sem_close(sem);
}
