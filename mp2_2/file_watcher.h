#ifndef FILE_WATCHER_H
#define FILE_WATCHER_H

#include <unordered_map>
#include <string>
#include <functional>
#include <filesystem>

using std::unordered_map;
using std::string;
using std::function;
using std::filesystem::file_time_type;
using std::filesystem::path;

class FileWatcher {
private:
    unordered_map<string, int> fileOffset;
    unordered_map<string, file_time_type> lastWrite;
public:
    FileWatcher(path watchDir);
    void watch(const string& suffix, const function<int(int)>& callback);
};

#endif
