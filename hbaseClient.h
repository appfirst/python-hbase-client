/******************************************************************************
 *
 *  Copyright © 2011-2012 AppFirst, Inc.  All rights reserved.
 *
 *  This file and the related software and code (if any) may contain confidential
 *  and proprietary information of AppFirst, Inc. and are made available solely
 *  for confidential use by the intended recipient.  Unless expressly authorized
 *  by AppFirst, Inc. in a separate written agreement, no part of this file or
 *  any related software or code may be used, reproduced, modified, disclosed or
 *  distributed, in any form or by any means.
 *
 *****************************************************************************/

#include <syslog.h>

/*
 *  V0.5  Should get basics in place
 *  V0.7  Fixed data conversion issue and added scan with filters
 *  V0.8  Resolved deprecated functions; must use Hbase 0.96 API docs
 *        Added Flush & SetAutoFlush
 *  V0.9  Added a defualt auto flush to the Connect method
 *  V0.92 Fixed an error in Get; check for empty results
 *  V0.94 Fixed a memory leak in the log and error handlers
 *        This version has been load tested and performs as expected
 *  V0.96 Added a Delete method and an hbmaster param to Connect
 *        Added error checks for null params to the Java code
 *  V0.98 Fixed a bug in Scan that was returning 1 value only
 *        Pass Java exceptions as Python exceptions
 *  V1.0  Intial working version
 *        Added diagnostics & a version method
 *  V1.1  Added max heap control for the JVM
 */

#define HBASE_CLIENT_VERSION 1.1
#define MAX_TABLE_NAME 128
#define LOG_MAX_LINE 1024
#define MAX_PATH LOG_MAX_LINE
#define DEBUG 1
#define PYTHON_INTERFACE
#define JAVA_CLASS_PREFIX "-Djava.class.path="
#define JAVA_LIB_PREFIX "-Djava.library.path="
#define JAVA_MEM_PREFIX "-Xmx"

typedef unsigned long long ticks;

#if DEBUG == 1
#define log(args...) do { PyLog((const char *)args); } while (0);
#define err(args...) do { PyErr((const char *)args); } while (0);
#elif DEBUG == 2
#define log(args...) do { printf(args); } while (0);
#define err(args...) do { printf(args); } while (0);
#elif DEBUG == 3
#define log(args...) do { syslog(LOG_NOTICE, (const char *)args); } while (0);
#define err(args...) do { syslog(LOG_NOTICE, (const char *)args); } while (0);
#else
#define log(args...) do { ; } while (0);
#define err(args...) do { syslog(LOG_NOTICE, (const char *)args); } while (0);
#endif

typedef struct hbase_client
{
    JNIEnv *env;
    jobject hbClient;
    jmethodID connect;
    jmethodID disconnect;
    jmethodID put;
    jmethodID get;
    jmethodID scan;
    jmethodID data;
    jmethodID cell;
    jmethodID dataScan;
    jmethodID flush;
    jmethodID setAutoFlush;
    jmethodID delete;
    jmethodID throwToString;
} hbase_client_t;
