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

/**
 * @brief Gets all lines from the file
 *
 * @param filename The file to get liens from
 */
vector<string> getLinesFromFile(const path& filename);

/**
 * @brief Exits the process on an AMQP error produced by the RabbitMQ server
 *
 * @param x The reply from the RabbitMQ server
 * @param context Error context
 */
void die_on_amqp_error(amqp_rpc_reply_t x, char const* context);

/**
 * @brief Exits the process due to an error code
 *
 * @param x An error code
 * @param context Error context
 */
void die_on_error(int x, char const* context);

/**
 * @brief Exits the process due to an error
 *
 * @param fmt Error context
 */
void die(const char* fmt, ...);

#endif
