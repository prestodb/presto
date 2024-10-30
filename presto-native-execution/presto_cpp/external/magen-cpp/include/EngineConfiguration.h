/*
 * IBM Confidential
 *
 * OCO Source Materials
 *
 * (C) Copyright IBM Corp. 2020, 2022. All Rights Reserved.
 *
 * The source code for this program is not published or otherwise divested of its trade
 * secrets irrespective of what has been deposited with the U.S. Copyright Office.
 *
 * US Government Users Restricted Rights - Use, duplication or disclosure restricted by
 * GSA ADP Schedule Contract with IBM Corp.
 */

#pragma once
#include <iostream>

//For callback function for GSKit SHA256 hashing function
#define DIGEST_LENGTH  32

enum AllocScope {
    PROCESS = 0,   // For global object memory allocation at init() call, deallocate when library exits
    TRANSIENT = 1  // For any dynamic memory allcoation at runtime and release it immediatly once task is done
};

//For callback function registration
/**
 * @brief CallBack Function for memory allocation
 *
 * @param size_t - memory size to be allocated
 * @return void* - return the allocated memory
 **/
typedef void *(*MemAllocFn)(size_t, AllocScope);

/**
 * @brief CallBack Function for memory free
 *
 * @param void* - free the allocated memory
 **/
typedef void (*MemFreeFn)(void *);

/**
 * @brief CallBack Function for memory reallocation
 *
 * @param void*  - Pointer to allocated memory
 * @param size_t - memory size to be reallocated
 * @return void* - return the new allocated memory
 **/
typedef void *(*MemReallocFn)(void *, size_t,AllocScope); 

/**
 * @brief Set the callBack function pointer for memory allocation, reallocation and free
 *
 * @param MemAllocFn     - callback pointer for memory allocation
 * @param MemReallocFn   - callback pointer for memory reallocation
 * @param MemFreeFn      - callback pointer to free the allocated memory
 * @return status  - 1 - Succuss 0 - failure
 **/
int setMemoryFunctions(MemAllocFn pMemAllocFn, MemReallocFn pMemReallocFn, MemFreeFn pMemFreeFn);


/**
 * @brief CallBack Function for DB2 GSKit SHA256 hashing to replace the magen dependent openssl api call
 *  
 * @param inBytes - Input byte array value with null terminator
 * @param inLen  - size of the input
 * @param inSeed -  Seed value for hash function
 * @param outHashedValue - Before return fill the 32-byte SHA256 value for the given input parameters
 * @return status  - 1 - Succuss 0 - failure
 */
typedef int (*SHA256Hash)(unsigned char inBytes[], std::size_t inLen, const char* inSeed, unsigned char outHashedValue[DIGEST_LENGTH]);

/**
 * @brief set method to register the CallBack Function for DB2 GSKit SHA256 hashing
 * 
 * @param SHA256Hash -  Function type to register
 * @return status  - 1 - Succuss 0 - failure
 */
int setSHA256Callback(SHA256Hash sha256HashCallbackFn);

/*
* @param inDirPath  - Root folder path for library to read its resource files
* @return status  - true - Succuss, false - Failure
*/
//Read resources files related to formats
//Create object instances of all format types to improve the performance and avoids the additional memory alloc/free at runtime
bool initializeLibrary(const char* inDirPath);

/*
* Call this fucntion while unloading Magen library from Engine otherwise don't call it, 
* it allows to clean-up the application scope objects
* @return status  - true - Succuss, false - Failure
*/
bool destructLibrary();

/*   NOTE:
*          Order of the function calling
*
*    1. Register memory callback function, invoke setMemoryFunctions()
*    2. Register SHA256 callback function which will be consumed at runtime - setSHA256Callback()
*    3. Initialise the engine configuration, invoke initializeLibrary()
*    4. Call this destructLibrary() function only when unloading the Magen library on that system.
*
*/
