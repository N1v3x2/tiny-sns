#ifndef SYNCHRONIZER_UTILS_H
#define SYNCHRONIZER_UTILS_H

#include <stdarg.h>

#include <amqp.h>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "file_utils.h"

using namespace std::filesystem;

using std::ifstream;
using std::string;
using std::vector;

vector<string> getLinesFromFile(const path& filename);
void die_on_amqp_error(amqp_rpc_reply_t x, char const* context);
void die_on_error(int x, char const* context);
void die(const char* fmt, ...);

#endif
