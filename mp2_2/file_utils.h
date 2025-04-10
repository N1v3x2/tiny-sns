#ifndef FILE_UTILS_H
#define FILE_UTILS_H

#include <semaphore.h>
#include <string>

using std::string;

/**
 * @class SemGuard
 * @brief Provides RAII semantics for managing named semaphores
 *
 */
class SemGuard {
  private:
    sem_t* sem;
    /**
     * @brief Converts a filename into a unique semaphore name by replacing all
     * path separator characters with '_'
     *
     * @param filename The filename to convert
     * @return A unique semaphore name for the file
     */
    string makeName(const string& filename);

  public:
    /**
     * @brief Constructs a new `SemGuard` object to manage the lifecycle of a
     * named semaphore
     *
     * @param name The name of the file the semaphore is responsible for
     */
    SemGuard(const string& name);

    /**
     * @brief Unlocks the named semaphore and deallocates any associated memory
     */
    ~SemGuard();
};

#endif
