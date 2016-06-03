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

#include "Python.h"
#include <jni.h>
#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <stdarg.h>
#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <time.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/time.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "hbaseClient.h"

static int _diagnostics = 0;
static uint64_t _freq = 0;
static ticks _tbegin, _tend, _tduration;

static uint64_t
getClockfreq(void)
{
    uint64_t result = 0;
    int fd;

    fd = open ("/proc/cpuinfo", O_RDONLY);
    if (fd != -1) {
        // /proc filesystem "files" are 4096 bytes max
        char *buf;
        ssize_t n;

        buf = (char *)malloc(MAX_PATH);
        if (!buf) {
            close(fd);
            return result;
        }
        n = read (fd, buf, MAX_PATH);
        if (n > 0) {
            char *mhz = strstr (buf, "cpu MHz");

            if (mhz != NULL) {
                char *endp = buf + n;
                int seen_decpoint = 0;
                int ndigits = 0;

                /* Search for the beginning of the string.  */
                while (mhz < endp && (*mhz < '0' || *mhz > '9') && *mhz != '\n') {
                    ++mhz;
                }

                while (mhz < endp && *mhz != '\n') {
                    if (*mhz >= '0' && *mhz <= '9') {
                        result *= 10;
                        result += *mhz - '0';
                        if (seen_decpoint) {
                            ++ndigits;
                        }
                    } else if (*mhz == '.') {
                        seen_decpoint = 1;
                    }
                    ++mhz;
                }

                /* Compensate for missing digits at the end.  */
                while (ndigits++ < 6) {
                    result *= 10;
                }
            }
        }
        free(buf);
        close (fd);
    }

    return result;
}

static ticks getTicks(void)
{
    unsigned a, d;
    asm volatile("rdtsc" : "=a" (a), "=d" (d));
    return ((ticks)a) | (((ticks)d) << 32);
}

static PyObject *Logger = NULL;
static void
PyLog(const char *msg, ...)
{
    PyObject *arg, *result;
    va_list va;
    char buf[LOG_MAX_LINE];

    if (Logger != NULL) {
        va_start(va, msg);
        if (vsnprintf(buf, LOG_MAX_LINE, msg, va) < 0) {
            printf("ERROR: vsnprintf\n");
            return;
        }
        va_end(va);

        if ((arg = Py_BuildValue("(s)", buf)) == NULL) {
            printf("ERROR: Invalid param: %s:%d\n", __FUNCTION__, __LINE__);
            return;
        }

        result = PyEval_CallObject(Logger, arg);
        Py_DECREF(arg);
        if (result != NULL) {
            Py_DECREF(result);
        }
    } else {
        syslog(LOG_NOTICE, msg);
    }
}

static PyObject *Error = NULL;
static void
PyErr(const char *msg, ...)
{
    PyObject *arg, *result;
    va_list va;
    char buf[LOG_MAX_LINE];

    if (Error != NULL) {
        va_start(va, msg);
        if (vsnprintf(buf, LOG_MAX_LINE, msg, va) < 0) {
            printf("ERROR: vsnprintf\n");
            return;
        }
        va_end(va);

        if ((arg = Py_BuildValue("(s)", buf)) == NULL) {
            printf("ERROR: Invalid param: %s:%d\n", __FUNCTION__, __LINE__);
            return;
        }

        result = PyEval_CallObject(Error, arg);
        Py_DECREF(arg);
        if (result != NULL) {
            Py_DECREF(result);
        }
    } else {
        syslog(LOG_NOTICE, msg);
    }
}

/*
 * This is a utility function thqt gets a JNI connection.
 * Locates an existing JVM or starts a new one, if none exists.
 */
static JNIEnv *
get_jni_env(const char *memsize)
{
    int ret, num_vms;
    JNIEnv *env = NULL;
    JavaVM *vmBuf[1], *vm;

    ret = JNI_GetCreatedJavaVMs(&(vmBuf[0]), 1, &num_vms);
    if (ret != 0) {
        err("JNI_GetCreatedJavaVMs failed with error: %d %s:%d", ret,
            __FUNCTION__, __LINE__);
        return NULL;
    }

    
    if (num_vms == 0) {
        int len;
        JavaVMInitArgs args;
        JavaVMOption options[3];
        char *classpath, *cstring, *lstring, *mstring;

        args.version = JNI_VERSION_1_6;
        args.nOptions = 3;
        args.ignoreUnrecognized = 0;

        if ((classpath = getenv("CLASSPATH")) == NULL) {
            err("ERROR: %s:%d\n", __FUNCTION__, __LINE__);
            return NULL;
        }

        len = strlen(classpath) + strlen(JAVA_CLASS_PREFIX) + 2;
        if ((cstring = malloc(len)) == NULL) {
            err("ERROR: %s:%d\n", __FUNCTION__, __LINE__);
            return NULL;
        }

        strncpy(cstring, JAVA_CLASS_PREFIX, len);
        strncat(cstring, classpath, len);
        
        options[0].optionString = cstring;

        if ((classpath = getenv("LD_LIBRARY_PATH")) == NULL) {
            err("ERROR: %s:%d\n", __FUNCTION__, __LINE__);
            return NULL;
        }

        len = strlen(classpath) + strlen(JAVA_LIB_PREFIX) + 2;
        if ((lstring = malloc(len)) == NULL) {
            err("ERROR: %s:%d\n", __FUNCTION__, __LINE__);
            return NULL;
        }

        strncpy(lstring, JAVA_LIB_PREFIX, len);
        strncat(lstring, classpath, len);
        options[1].optionString = lstring;

        len = strlen(memsize) + strlen(JAVA_MEM_PREFIX) + 2;
        if ((mstring = malloc(len)) == NULL) {
            err("ERROR: %s:%d\n", __FUNCTION__, __LINE__);
            return NULL;
        }

        strncpy(mstring, JAVA_MEM_PREFIX, len);
        strncat(mstring, memsize, len);
        options[2].optionString = mstring;

        args.options = options;

        vm = vmBuf[0];
        ret = JNI_CreateJavaVM(&vm, (void**)&env, &args);
        if (ret < 0 || !env) {
            log("No current Java VMs started and can't start a new one\n");
            return NULL;
        }
        
        ret = (*vm)->AttachCurrentThread(vm, (void **)&env, (void *)NULL);
        if (ret != 0) {
            log("AttachCurrentThread failed with error: %d %s:%d", ret, __FUNCTION__, __LINE__);
            return NULL;
        }
        
        free(cstring);
        free(lstring);
        free(mstring);
    } else {
        //Attach this thread to the JVM
        vm = vmBuf[0];
        ret = (*vm)->AttachCurrentThread(vm, (void **)&env, (void *)NULL);
        if (ret != 0) {
            log("AttachCurrentThread failed with error: %d %s:%d", ret, __FUNCTION__, __LINE__);
            return NULL;
        }
    }
  return env;
}

static int
getJavaExcpetion(hbase_client_t *client, jthrowable exception)
{
    jstring jmsg;
    const char *msg;

    if ((client == NULL) || (exception == NULL)) {
        err("Bad param: %s:%d\n", __FUNCTION__, __LINE__);
        return -1;
    }

    jmsg = (jstring) (*client->env)->CallObjectMethod(client->env, exception,
                                                      client->throwToString);
    msg = (*client->env)->GetStringUTFChars(client->env, jmsg, 0);
    PyErr_SetString(PyExc_IOError, msg);
    (*client->env)->ReleaseStringUTFChars(client->env, jmsg, msg);
    (*client->env)->DeleteLocalRef(client->env, jmsg);
    return 0;
}


static int
releaseJavaStuff(hbase_client_t *client,
                 jbyteArray jtable, const char *table,
                 jbyteArray jrow, const char *rowkey,
                 jbyteArray jcolfam, const char *colfamily,
                 jbyteArray jcolumn, const char *column,
                 jbyteArray jvalue, const char *data,
                 jbyteArray jfilter, const char *filter)
{
    if (jtable && table) {
        (*client->env)->ReleaseByteArrayElements(client->env, jtable, (jbyte *)table, 0);
        (*client->env)->DeleteLocalRef(client->env, jtable);
    }

    if (jrow && rowkey) {
        (*client->env)->ReleaseByteArrayElements(client->env, jrow, (jbyte *)rowkey, 0);
        (*client->env)->DeleteLocalRef(client->env, jrow);
    }

    if (jcolfam && colfamily) {
        (*client->env)->ReleaseByteArrayElements(client->env, jcolfam, (jbyte *)colfamily, 0);
        (*client->env)->DeleteLocalRef(client->env, jcolfam);
    }

    if (jcolumn && column) {
        (*client->env)->ReleaseByteArrayElements(client->env, jcolumn, (jbyte *)column, 0);
        (*client->env)->DeleteLocalRef(client->env, jcolumn);
    }

    if (jvalue && data) {
        (*client->env)->ReleaseByteArrayElements(client->env, jvalue, (jbyte *)data, 0);
        (*client->env)->DeleteLocalRef(client->env, jvalue);
    }
    
    if (jfilter && filter) {
        (*client->env)->ReleaseByteArrayElements(client->env, jfilter, (jbyte *)filter, 0);
        (*client->env)->DeleteLocalRef(client->env, jfilter);
    }
    return 0;
}

/*
 * These is the API for the Hbase client.
 * These functions use Python Ctypes.
 */
unsigned long
Connect(const char *zknode, const char *hbmaster,
        const char *table, int autoFlush, const char *memsize)
{
    hbase_client_t *client;
    JNIEnv *env = NULL;
    jclass hbase_client_class, throwable;
    jstring zvalue, hbmvalue, jtable;
    jthrowable exception;

    log("pid %d Connecting to Zookeeper at %s\n", getpid(), zknode);

    // Check here as this can be called direct from Python
    if (!memsize) {
        err("ERROR: No memory parameter: %s:%d\n", __FUNCTION__, __LINE__);
        return (unsigned long)0;
    }

    if ((env = get_jni_env(memsize)) == NULL) {
        err("ERROR: can't get a JNI environment: %s:%d\n", __FUNCTION__, __LINE__);
        return (unsigned long)0;
    }

    if ((client = malloc(sizeof(hbase_client_t))) == NULL) {
        err("ERROR: could not malloc for hbase_client: %s:%d\n", __FUNCTION__, __LINE__);
        return (unsigned long)0;
    }

    // Init the clock freq
    _freq = getClockfreq();

    client->env = env;

    // Get methods for hbaseClient
    hbase_client_class = (*env)->FindClass(env, "hbaseClient");
    
    client->connect = (*env)->GetMethodID(env, hbase_client_class, "<init>",
                                          "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Z)V");

    client->disconnect = (*env)->GetMethodID(env, hbase_client_class, "Disconnect","()V");

    client->put = (*env)->GetMethodID(env, hbase_client_class, "Put",
                                      "([B[B[B[B[B)I");
    client->get = (*env)->GetMethodID(env, hbase_client_class, "Get",
                                      "([B[B[B[B)I");
    client->scan = (*env)->GetMethodID(env, hbase_client_class, "Scan",
                                       "([B[B[B[B[B[B)I");
    client->data = (*env)->GetMethodID(env, hbase_client_class, "DataValue",
                                       "()[B");
    client->cell = (*env)->GetMethodID(env, hbase_client_class, "DataCell",
                                       "()[[B");
    client->dataScan = (*env)->GetMethodID(env, hbase_client_class, "DataScan",
                                           "()[B");    
    client->flush = (*env)->GetMethodID(env, hbase_client_class, "Flush",
                                           "([B)I");
    client->setAutoFlush = (*env)->GetMethodID(env, hbase_client_class, "SetAutoFlush",
                                           "([BZ)Z");
    client->delete = (*env)->GetMethodID(env, hbase_client_class, "Delete",
                                           "([B[B[B[B)I");
    // Get methods for Java exception handling
    throwable = (*client->env)->FindClass(client->env, "java/lang/Throwable");

    client->throwToString = (*client->env)->GetMethodID(client->env, throwable,
                                                        "toString", "()Ljava/lang/String;");
    // Build params for hbaseClient constructor
    zvalue = (*env)->NewStringUTF(env, zknode);
    hbmvalue = (*env)->NewStringUTF(env, hbmaster);

    if (table != NULL) {
        jtable = (*env)->NewStringUTF(env, table);
    } else {
        jtable = NULL;
    }

    client->hbClient = (*env)->NewObject(client->env, hbase_client_class, client->connect,
                                         zvalue, hbmvalue, jtable, (jboolean)autoFlush);
    if ((*client->env)->ExceptionCheck(client->env)) {
        exception = (*client->env)->ExceptionOccurred(client->env);
        (*client->env)->ExceptionClear(client->env);
        getJavaExcpetion(client, exception);
        return (unsigned long)-1;
    }

    printf("Hbase Client Version %f\nAutoFlush is set to %d\nMax heap size is %s\n",
           HBASE_CLIENT_VERSION, autoFlush, memsize);
    return (unsigned long)client;
}

int
Disconnect(hbase_client_t *client)
{
    jthrowable exception;

    (*client->env)->CallVoidMethod(client->env, client->hbClient, client->disconnect, NULL);
    if ((*client->env)->ExceptionCheck(client->env)) {
        exception = (*client->env)->ExceptionOccurred(client->env);
        (*client->env)->ExceptionClear(client->env);
        getJavaExcpetion(client, exception);
        return -1;
    }

    (*client->env)->DeleteLocalRef(client->env, client->hbClient);
    free(client);
    return 0;
}

int
Put(hbase_client_t *client,
    const char *table, size_t table_len,
    const char *rowkey, size_t row_len,
    const char *colfamily, size_t cf_len,
    const char *column, size_t col_len,
    const char *data, size_t data_len)
{
    int res = 0;
    jbyteArray jtable, jrow, jcolfam, jcolumn, jvalue;
    jthrowable exception;

    log("Put(%d) client %lu\n", getpid(), client);
    if ((rowkey == NULL) || (row_len <= 0) ||
        (data == NULL) || (data_len <= 0)) {
        err("ERROR: bad params %s:%d\n", __FUNCTION__, __LINE__);
        return -1;
    }
    
    jrow = (*client->env)->NewByteArray(client->env, (jsize)row_len);
    (*client->env)->SetByteArrayRegion(client->env, jrow, (jsize)0, (jsize)row_len, (const jbyte *)rowkey);

    jvalue = (*client->env)->NewByteArray(client->env, (jsize)data_len);
    (*client->env)->SetByteArrayRegion(client->env, jvalue, (jsize)0, (jsize)data_len, (const jbyte *)data);

    if ((table != NULL) && (table_len > 0)) {
        jtable = (*client->env)->NewByteArray(client->env, (jsize)table_len);
        (*client->env)->SetByteArrayRegion(client->env, jtable, (jsize)0, (jsize)table_len, (const jbyte *)table);
    } else {
        jtable = NULL;
    }

    if ((colfamily != NULL) && (cf_len > 0)) {
        jcolfam = (*client->env)->NewByteArray(client->env, (jsize)cf_len);
        (*client->env)->SetByteArrayRegion(client->env, jcolfam, (jsize)0, (jsize)cf_len, (const jbyte *)colfamily);
    } else {
        jcolfam = NULL;
    }

    if ((column != NULL) && (col_len > 0)) {
        jcolumn = (*client->env)->NewByteArray(client->env, (jsize)col_len);
        (*client->env)->SetByteArrayRegion(client->env, jcolumn, (jsize)0, (jsize)col_len, (const jbyte *)column);
    } else {
        jcolumn = NULL;
    }
    
    res = (*client->env)->CallIntMethod(client->env,
                                        client->hbClient,
                                        client->put,
                                        jtable, jrow, jcolfam,
                                        jcolumn, jvalue);
    if ((*client->env)->ExceptionCheck(client->env)) {
        exception = (*client->env)->ExceptionOccurred(client->env);
        (*client->env)->ExceptionClear(client->env);
        getJavaExcpetion(client, exception);
        res = -1;
    }

    releaseJavaStuff(client, jtable, table,
                     jrow, rowkey, jcolfam, colfamily,
                     jcolumn, column, jvalue, data,
                     NULL, NULL);
    return res;
}

int
Get(hbase_client_t *client,
    const char *table, size_t table_len,
    const char *rowkey, size_t row_len,
    const char *colfamily, size_t cf_len,
    const char *column, size_t col_len)
{
    int res;
    jbyteArray jtable, jrow, jcolfam, jcolumn;
    jthrowable exception;

    log("Get(%d) client %lu\n", getpid(), client);

    if ((rowkey == NULL) || (row_len <= 0)) {
        err("ERROR: bad params %s:%d\n", __FUNCTION__, __LINE__);
        return -1;
    }
    
    jrow = (*client->env)->NewByteArray(client->env, (jsize)row_len);
    (*client->env)->SetByteArrayRegion(client->env, jrow, (jsize)0, (jsize)row_len, (const jbyte *)rowkey);

    if ((table != NULL) && (table_len > 0)) {
        jtable = (*client->env)->NewByteArray(client->env, (jsize)table_len);
        (*client->env)->SetByteArrayRegion(client->env, jtable, (jsize)0, (jsize)table_len, (const jbyte *)table);
    } else {
        jtable = NULL;
    }

    if ((colfamily != NULL) && (cf_len > 0)) {
        jcolfam = (*client->env)->NewByteArray(client->env, (jsize)cf_len);
        (*client->env)->SetByteArrayRegion(client->env, jcolfam, (jsize)0, (jsize)cf_len, (const jbyte *)colfamily);
    } else {
        jcolfam = NULL;
    }

    if ((column != NULL) && (col_len > 0)) {
        jcolumn = (*client->env)->NewByteArray(client->env, (jsize)col_len);
        (*client->env)->SetByteArrayRegion(client->env, jcolumn, (jsize)0, (jsize)col_len, (const jbyte *)column);
    } else {
        jcolumn = NULL;
    }
    
    res = (*client->env)->CallIntMethod(client->env,
                                        client->hbClient,
                                        client->get,
                                        jtable, jrow, jcolfam, jcolumn);
    if ((*client->env)->ExceptionCheck(client->env)) {
        exception = (*client->env)->ExceptionOccurred(client->env);
        (*client->env)->ExceptionClear(client->env);
        getJavaExcpetion(client, exception);
        res = -1;
    }

    releaseJavaStuff(client, jtable, table,
                     jrow, rowkey, jcolfam, colfamily,
                     jcolumn, column, NULL, NULL,
                     NULL, NULL);
    return res;
}

int
Scan(hbase_client_t *client,
     const char *table, size_t table_len,
     const char *start_row, size_t start_row_len,
     const char *end_row, size_t end_row_len,
     const char *colfamily, size_t colfam_len,
     const char *column, size_t column_len,
     const char *filter, size_t filter_len)
{
    int res = 0;
    jbyteArray jtable = NULL, jstartrow = NULL, jendrow = NULL;
    jbyteArray jcolfamily = NULL, jcolumn = NULL, jfilter = NULL;
    jthrowable exception;

    log("Scan(%d) client %lu\n", getpid(), client);
    
    if ((start_row != NULL) && (start_row_len > 0)) {
        jstartrow = (*client->env)->NewByteArray(client->env, (jsize)start_row_len);
        (*client->env)->SetByteArrayRegion(client->env, jstartrow, (jsize)0, (jsize)start_row_len, (const jbyte *)start_row);
    }

    if ((end_row != NULL) && (end_row_len > 0)) {
        jendrow = (*client->env)->NewByteArray(client->env, (jsize)end_row_len);
        (*client->env)->SetByteArrayRegion(client->env, jendrow, (jsize)0, (jsize)end_row_len, (const jbyte *)end_row);
    }

    if ((table != NULL) && (table_len > 0)) {
        jtable = (*client->env)->NewByteArray(client->env, (jsize)table_len);
        (*client->env)->SetByteArrayRegion(client->env, jtable, (jsize)0, (jsize)table_len, (const jbyte *)table);
    }

    if ((colfamily != NULL) && (colfam_len > 0)) {
        jcolfamily = (*client->env)->NewByteArray(client->env, (jsize)colfam_len);
        (*client->env)->SetByteArrayRegion(client->env, jcolfamily, (jsize)0, (jsize)colfam_len, (const jbyte *)colfamily);
    }

    if ((column != NULL) && (column_len > 0)) {
        jcolumn = (*client->env)->NewByteArray(client->env, (jsize)column_len);
        (*client->env)->SetByteArrayRegion(client->env, jcolumn, (jsize)0, (jsize)column_len, (const jbyte *)column);
    }

    if ((filter != NULL) && (filter_len > 0)) {
        jfilter = (*client->env)->NewByteArray(client->env, (jsize)filter_len);
        (*client->env)->SetByteArrayRegion(client->env, jfilter, (jsize)0, (jsize)filter_len, (const jbyte *)filter);
    }

    res = (*client->env)->CallIntMethod(client->env,
                                        client->hbClient,
                                        client->scan,
                                        jtable, jstartrow,
                                        jendrow, jcolfamily,
                                        jcolumn, jfilter);
    if ((*client->env)->ExceptionCheck(client->env)) {
        exception = (*client->env)->ExceptionOccurred(client->env);
        (*client->env)->ExceptionClear(client->env);
        getJavaExcpetion(client, exception);
        res = -1;
    }

    releaseJavaStuff(client, jtable, table,
                     jstartrow, start_row, jendrow, end_row,
                     jcolumn, column, jfilter, filter, NULL, NULL);
    return res;
}

int
Flush(hbase_client_t *client, const char *table, size_t table_len)
{
    int res = 0;
    jbyteArray jtable;
    jthrowable exception;

    log("Flush(%d) client %lu\n", getpid(), client);

    if ((table != NULL) && (table_len > 0)) {
        jtable = (*client->env)->NewByteArray(client->env, (jsize)table_len);
        (*client->env)->SetByteArrayRegion(client->env, jtable, (jsize)0, (jsize)table_len, (const jbyte *)table);
    } else {
        jtable = NULL;
    }

    res = (*client->env)->CallIntMethod(client->env,
                                        client->hbClient,
                                        client->flush,
                                        jtable);
    if ((*client->env)->ExceptionCheck(client->env)) {
        exception = (*client->env)->ExceptionOccurred(client->env);
        (*client->env)->ExceptionClear(client->env);
        getJavaExcpetion(client, exception);
        res = -1;
    }

    releaseJavaStuff(client, jtable, table,
                     NULL, NULL, NULL, NULL,
                     NULL, NULL, NULL, NULL,
                     NULL, NULL);
    return res;
}

int
setAutoFlush(hbase_client_t *client, const char *table,
             size_t table_len, jboolean flush)
{
    jboolean jret = 0;
    jbyteArray jtable;
    jthrowable exception;

    log("setAutoFlush(%d) client %lu\n", getpid(), client);

    if ((table != NULL) && (table_len > 0)) {
        jtable = (*client->env)->NewByteArray(client->env, (jsize)table_len);
        (*client->env)->SetByteArrayRegion(client->env, jtable, (jsize)0, (jsize)table_len, (const jbyte *)table);
    } else {
        jtable = NULL;
    }

    jret = (*client->env)->CallIntMethod(client->env,
                                         client->hbClient,
                                         client->setAutoFlush,
                                         jtable, flush);
    if ((*client->env)->ExceptionCheck(client->env)) {
        exception = (*client->env)->ExceptionOccurred(client->env);
        (*client->env)->ExceptionClear(client->env);
        getJavaExcpetion(client, exception);
        jret = (jboolean)-1;
    }

    releaseJavaStuff(client, jtable, table,
                     NULL, NULL, NULL, NULL,
                     NULL, NULL, NULL, NULL,
                     NULL, NULL);
    return (int)jret;
}

int
Delete(hbase_client_t *client,
       const char *table, size_t table_len,
       const char *row, size_t row_len,
       const char *colfamily, size_t cf_len,
       const char *column, size_t col_len)
{
    int res;
    jbyteArray jtable, jrow, jcolfam, jcolumn;
    jthrowable exception;

    if ((row == NULL) || (row_len <= 0)) {
        err("ERROR: bad params %s:%d\n", __FUNCTION__, __LINE__);
        return -1;
    }
    
    jrow = (*client->env)->NewByteArray(client->env, (jsize)row_len);
    (*client->env)->SetByteArrayRegion(client->env, jrow, (jsize)0, (jsize)row_len, (const jbyte *)row);

    if ((table != NULL) && (table_len > 0)) {
        jtable = (*client->env)->NewByteArray(client->env, (jsize)table_len);
        (*client->env)->SetByteArrayRegion(client->env, jtable, (jsize)0, (jsize)table_len, (const jbyte *)table);
    } else {
        jtable = NULL;
    }

    if ((colfamily != NULL) && (cf_len > 0)) {
        jcolfam = (*client->env)->NewByteArray(client->env, (jsize)cf_len);
        (*client->env)->SetByteArrayRegion(client->env, jcolfam, (jsize)0, (jsize)cf_len, (const jbyte *)colfamily);
    } else {
        jcolfam = NULL;
    }

    if ((column != NULL) && (col_len > 0)) {
        jcolumn = (*client->env)->NewByteArray(client->env, (jsize)col_len);
        (*client->env)->SetByteArrayRegion(client->env, jcolumn, (jsize)0, (jsize)col_len, (const jbyte *)column);
    } else {
        jcolumn = NULL;
    }
    
    res = (*client->env)->CallIntMethod(client->env,
                                        client->hbClient,
                                        client->delete,
                                        jtable, jrow, jcolfam, jcolumn);
    if ((*client->env)->ExceptionCheck(client->env)) {
        exception = (*client->env)->ExceptionOccurred(client->env);
        (*client->env)->ExceptionClear(client->env);
        getJavaExcpetion(client, exception);
        res = -1;
    }

    releaseJavaStuff(client, NULL, NULL,
                     jrow, row, jcolfam, colfamily,
                     jcolumn, column, NULL, NULL,
                     NULL, NULL);
    return res;
}

/*
 * This is the Python module interface
 */
#ifdef PYTHON_INTERFACE
/* int Put(connect, table, row, colfamily, column, data) */
static PyObject *
hbase_Put(PyObject *self, PyObject *args)
{
    int res;
    unsigned long kclient;
    hbase_client_t *client;
    size_t table_len, row_len, colfamily_len, column_len, data_len;
    const char *table, *row, *colfamily, *column, *data;

    if (_diagnostics && (_freq != 0)) {
        _tbegin = getTicks();
    }

    row = NULL;
    data = NULL;
    table = NULL;
    column = NULL;
    colfamily = NULL;

    if (!PyArg_ParseTuple(args,"ket#et#et#et#et#", &kclient,
                          NULL, &table, &table_len,
                          NULL, &row, &row_len,
                          NULL, &colfamily, &colfamily_len,
                          NULL, &column, &column_len,
                          NULL, &data, &data_len)) {
        PyErr_SetString(PyExc_SyntaxError, "Invalid Parameter");
        return NULL;
    }

    client = (hbase_client_t *)kclient;
    res = Put(client, table, table_len,
              row, row_len,
              colfamily, colfamily_len,
              column, column_len,
              data, data_len);

    if (_diagnostics && (_freq != 0)) {
        _tend = getTicks();
        _tduration = (((_tend - _tbegin)) * (uint64_t)(1000000)) / _freq;
    }

    if (res == -1) {
        return NULL;
    }

    return Py_BuildValue("i", res);
}

/* void * Get(connect, table, row, colfamily, column) */
static PyObject *
hbase_Get(PyObject *self, PyObject *args)
{
    int res;
    unsigned long kclient;
    hbase_client_t *client;
    size_t table_len, row_len, colfamily_len, column_len;
    const char *table, *row, *colfamily, *column;

    if (_diagnostics && (_freq != 0)) {
        _tbegin = getTicks();
    }

    row = NULL;
    table = NULL;
    column = NULL;
    colfamily = NULL;
    
    if (!PyArg_ParseTuple(args,"ket#et#et#et#", &kclient,
                          NULL, &table, &table_len,
                          NULL, &row, &row_len,
                          NULL, &colfamily, &colfamily_len,
                          NULL, &column, &column_len)) {
        PyErr_SetString(PyExc_SyntaxError, "Invalid Parameter");
        return NULL;
    }

    client = (hbase_client_t *)kclient;
    res = Get(client,
              table, table_len,
              row, row_len,
              colfamily, colfamily_len,
              column, column_len);
    if (res == -1) {
        return NULL;
    }

    if (_diagnostics && (_freq != 0)) {
        _tend = getTicks();
        _tduration = (((_tend - _tbegin)) * (uint64_t)(1000000)) / _freq;
    }

    return Py_BuildValue("i", res);
}

/* int Scan(connect, table, start_row, end_row, colfamily, column, filter) */
static PyObject *
hbase_Scan(PyObject *self, PyObject *args)
{
    int res;
    unsigned long kclient;
    hbase_client_t *client;
    size_t table_len, start_row_len, end_row_len;
    size_t colfam_len, column_len, filter_len;
    const char *table, *start_row, *end_row;
    const char *colfamily, *column, *filter;
    PyObject *pirate;

    if (_diagnostics && (_freq != 0)) {
        _tbegin = getTicks();
    }

    table = NULL;
    end_row = NULL;
    start_row = NULL;
    colfamily = NULL;
    column = NULL;
    filter = NULL;

    if (!PyArg_ParseTuple(args,"ket#et#et#et#et#et#",
                          &kclient,
                          NULL, &table, &table_len,
                          NULL, &start_row, &start_row_len,
                          NULL, &end_row, &end_row_len,
                          NULL, &colfamily, &colfam_len,
                          NULL, &column, &column_len,
                          NULL, &filter, &filter_len)) {
        PyErr_SetString(PyExc_SyntaxError, "Invalid Parameter");
        return NULL;
    }

    client = (hbase_client_t *)kclient;
    res = Scan(client,
               table, table_len,
               start_row, start_row_len,
               end_row, end_row_len,
               colfamily, colfam_len,
               column, column_len,
               filter, filter_len);
    if (res == -1) {
        return NULL;
    }

    pirate = Py_BuildValue("i", res);

    if (_diagnostics && (_freq != 0)) {
        _tend = getTicks();
        _tduration = (((_tend - _tbegin)) * (uint64_t)(1000000)) / _freq;
    }

    return pirate;
}

/* char * Data(connect) */
static PyObject *
hbase_Data(PyObject *self, PyObject *args)
{
    size_t vlen;
    unsigned long kclient;
    hbase_client_t *client;
    jbyteArray jvalue;
    PyObject *pycell;
    const char *value;
    jthrowable exception;

    if (_diagnostics && (_freq != 0)) {
        _tbegin = getTicks();
    }

    if (!PyArg_ParseTuple(args,"k", &kclient)) {
        PyErr_SetString(PyExc_SyntaxError, "Invalid Parameter");
        return NULL;
    }

    client = (hbase_client_t *)kclient;
    if (client == NULL) {
        PyErr_SetString(PyExc_SyntaxError, "Invalid Parameter");
        return  NULL;
    }

    log("Read Data: client %p\n", client);

    jvalue = (jbyteArray)(*client->env)->CallObjectMethod(client->env,
                                                          client->hbClient,
                                                          client->data,
                                                          NULL);
    if ((*client->env)->ExceptionCheck(client->env)) {
        exception = (*client->env)->ExceptionOccurred(client->env);
        (*client->env)->ExceptionClear(client->env);
        getJavaExcpetion(client, exception);
        return NULL;
    }

    if (jvalue == NULL) {
        return Py_BuildValue("s", NULL);
    }

    vlen = (*client->env)->GetArrayLength(client->env, jvalue);
    value = (const char *)(*client->env)->GetByteArrayElements(client->env, jvalue, (jboolean *)NULL);

    pycell = PyBytes_FromStringAndSize(value, (Py_ssize_t)vlen);
    (*client->env)->ReleaseByteArrayElements(client->env, jvalue, (jbyte *)value, JNI_ABORT);
    (*client->env)->DeleteLocalRef(client->env, jvalue);

    if (_diagnostics && (_freq != 0)) {
        _tend = getTicks();
        _tduration = (((_tend - _tbegin)) * (uint64_t)(1000000)) / _freq;
    }

    return pycell;
}

/* char * Cell(connect) */
static PyObject *
hbase_Cell(PyObject *self, PyObject *args)
{
    int i;
    jint count;
    size_t row_len, cf_len, col_len, value_len;
    unsigned long kclient;
    jobjectArray jcell;
    hbase_client_t *client;
    PyObject *pycell, *pyrow, *pycol, *pycolfam, *pyvalue;
    const char *row = NULL;
    jstring row_element = NULL;
    const char *colfam = NULL;
    jstring colfam_element = NULL;
    const char *column = NULL;
    jstring column_element = NULL;
    const char *value = NULL;
    jstring value_element = NULL;
    jthrowable exception;

    if (_diagnostics && (_freq != 0)) {
        _tbegin = getTicks();
    }

    if (!PyArg_ParseTuple(args,"k", &kclient)) {
        PyErr_SetString(PyExc_SyntaxError, "Invalid Parameter");
        return NULL;
    }

    client = (hbase_client_t *)kclient;
    if (client == NULL) {
        PyErr_SetString(PyExc_SyntaxError, "Invalid Parameter");
        return NULL;
    }

    log("Read Cell: client %p\n", client);
    row_len = cf_len = col_len = value_len = 0;
    
    jcell = (jobjectArray)(*client->env)->CallObjectMethod(client->env,
                                                           client->hbClient,
                                                           client->cell,
                                                           NULL);
    if ((*client->env)->ExceptionCheck(client->env)) {
        exception = (*client->env)->ExceptionOccurred(client->env);
        (*client->env)->ExceptionClear(client->env);
        getJavaExcpetion(client, exception);
        return NULL;
    }

    if (jcell == NULL) {
        return Py_BuildValue("s", NULL);
    }
    
    count = (*client->env)->GetArrayLength(client->env, jcell);
    for (i=0; i < count; i++) {
        size_t len;
        jbyteArray element;
        const char *str;
        
        element = (jbyteArray)(*client->env)->GetObjectArrayElement(client->env, jcell, i);
        if((*client->env)->ExceptionOccurred(client->env)) {
            (*client->env)->ExceptionClear(client->env);
            return NULL;
        }
        
        len = (*client->env)->GetArrayLength(client->env, element);
        str = (const char *)(*client->env)->GetByteArrayElements(client->env, element, (jboolean *)NULL);
        
        switch(i) {
        case 0:
            row = str;
            row_len = len;
            row_element = element;
        case 1:
            colfam = str;
            cf_len = len;
            colfam_element = element;
        case 2:
            column = str;
            col_len = len;
            column_element = element;
        case 3:
            value = str;
            value_len = len;
            value_element = element;
        }

    }

    pyrow = PyBytes_FromStringAndSize(row, (Py_ssize_t)row_len);
    pycol = PyBytes_FromStringAndSize(column, (Py_ssize_t)col_len);
    pycolfam = PyBytes_FromStringAndSize(colfam, (Py_ssize_t)cf_len);
    pyvalue = PyBytes_FromStringAndSize(value, (Py_ssize_t)value_len);
    pycell = Py_BuildValue("{s:O,s:O,s:O,s:O}",
                           "row", pyrow,
                           "colfam", pycolfam,
                           "column", pycol,
                           "value", pyvalue);

    (*client->env)->ReleaseByteArrayElements(client->env, row_element, (jbyte *)row, JNI_ABORT);
    (*client->env)->ReleaseByteArrayElements(client->env, colfam_element, (jbyte *)colfam, JNI_ABORT);
    (*client->env)->ReleaseByteArrayElements(client->env, column_element, (jbyte *)column, JNI_ABORT);
    (*client->env)->ReleaseByteArrayElements(client->env, value_element, (jbyte *)value, JNI_ABORT);
    
    (*client->env)->DeleteLocalRef(client->env, jcell);

    if (_diagnostics && (_freq != 0)) {
        _tend = getTicks();
        _tduration = (((_tend - _tbegin)) * (uint64_t)(1000000)) / _freq;
    }

    return pycell;
}

/* char * DataScan(connect) */
static PyObject *
hbase_DataScan(PyObject *self, PyObject *args)
{
    size_t vlen;
    unsigned long kclient;
    hbase_client_t *client;
    jbyteArray jvalue;
    PyObject *pycell;
    const char *value;
    jthrowable exception;

    if (_diagnostics && (_freq != 0)) {
        _tbegin = getTicks();
    }

    if (!PyArg_ParseTuple(args,"k", &kclient)) {
        PyErr_SetString(PyExc_SyntaxError, "Invalid Parameter");
        return NULL;
    }
    
    client = (hbase_client_t *)kclient;
    if (client == NULL) {
        PyErr_SetString(PyExc_SyntaxError, "Invalid Parameter");
        return NULL;
    }

    log("DataScan(%d) client %lu\n", getpid(), kclient);

    jvalue = (jbyteArray)(*client->env)->CallObjectMethod(client->env,
                                                          client->hbClient,
                                                          client->dataScan,
                                                          NULL);
    if ((*client->env)->ExceptionCheck(client->env)) {
        exception = (*client->env)->ExceptionOccurred(client->env);
        (*client->env)->ExceptionClear(client->env);
        getJavaExcpetion(client, exception);
        return NULL;
    }

    if (jvalue == NULL) {
        return Py_BuildValue("s", NULL);
    }

    vlen = (*client->env)->GetArrayLength(client->env, jvalue);
    value = (const char *)(*client->env)->GetByteArrayElements(client->env, jvalue, (jboolean *)NULL);

    pycell = PyBytes_FromStringAndSize(value, (Py_ssize_t)vlen);
    (*client->env)->ReleaseByteArrayElements(client->env, jvalue, (jbyte *)value, JNI_ABORT);
    (*client->env)->DeleteLocalRef(client->env, jvalue);

    if (_diagnostics && (_freq != 0)) {
        _tend = getTicks();
        _tduration = (((_tend - _tbegin)) * (uint64_t)(1000000)) / _freq;
    }

    return pycell;
}

/* unsigned long Connect(char *zookeeper, char *hbase_master, char *table, int autoflush) */
static PyObject *
hbase_Connect(PyObject *self, PyObject *args)
{
    int autoFlush;
    unsigned long client;
    char *zknode, *hbmaster, *table, *memsize;

    if (!PyArg_ParseTuple(args,"sszis", &zknode, &hbmaster, &table, &autoFlush, &memsize)) {
        PyErr_SetString(PyExc_SyntaxError, "Invalid Parameter");
        return NULL;
    }
    
    client = Connect(zknode, hbmaster, table, autoFlush, memsize);
    if (client == (unsigned long)-1) {
        return NULL;
    }

    return Py_BuildValue("k", (unsigned long)client);
}

/* int Disconnect(connect) */
static PyObject *
hbase_Disconnect(PyObject *self, PyObject *args)
{
    int res;
    unsigned long kclient;
    hbase_client_t *client;

    if (!PyArg_ParseTuple(args,"k", &kclient)) {
        PyErr_SetString(PyExc_SyntaxError, "Invalid Parameter");
        return NULL;
    }
    
    client = (hbase_client_t *)kclient;
    res = Disconnect(client);
    if (res == -1) {
        return NULL;
    }

    return Py_BuildValue("i", 0);
}

/* int Flush(connect, char *table) */
static PyObject *
hbase_Flush(PyObject *self, PyObject *args)
{
    int res;
    unsigned long kclient;
    hbase_client_t *client;
    size_t table_len;
    const char *table;
    PyObject *pirate;

    if (_diagnostics && (_freq != 0)) {
        _tbegin = getTicks();
    }

    table = NULL;

    if (!PyArg_ParseTuple(args,"ket#", &kclient,
                          NULL, &table, &table_len)) {
        PyErr_SetString(PyExc_SyntaxError, "Invalid Parameter");
        return NULL;
    }
    
    client = (hbase_client_t *)kclient;
    res = Flush(client, table, table_len);
    if (res == -1) {
        return NULL;
    }

    pirate = Py_BuildValue("i", res);

    if (_diagnostics && (_freq != 0)) {
        _tend = getTicks();
        _tduration = (((_tend - _tbegin)) * (uint64_t)(1000000)) / _freq;
    }

    return pirate;
}

/* int SetAutoFlush(connect, char *table, int flush) */
static PyObject *
hbase_SetAutoFlush(PyObject *self, PyObject *args)
{
    int flush, res;
    unsigned long kclient;
    hbase_client_t *client;
    size_t table_len;
    const char *table;

    table = NULL;

    if (!PyArg_ParseTuple(args,"ket#i", &kclient,
                          NULL, &table, &table_len, &flush)) {
        PyErr_SetString(PyExc_SyntaxError, "Invalid Parameter");
        return NULL;
    }
    
    client = (hbase_client_t *)kclient;
    res = setAutoFlush(client, table, table_len, flush);
    if (res == -1) {
        return NULL;
    }

    return Py_BuildValue("i", res);
}

/* int Delete(connect, char *table, char *row, char *colfamily, char *column) */
static PyObject *
hbase_Delete(PyObject *self, PyObject *args)
{
    int res;
    unsigned long kclient;
    hbase_client_t *client;
    PyObject *pirate;
    size_t table_len, row_len, colfamily_len, column_len;
    const char *table, *row, *colfamily, *column;

    if (_diagnostics && (_freq != 0)) {
        _tbegin = getTicks();
    }

    row = NULL;
    table = NULL;
    column = NULL;
    colfamily = NULL;
    
    if (!PyArg_ParseTuple(args,"ket#et#et#et#", &kclient,
                          NULL, &table, &table_len,
                          NULL, &row, &row_len,
                          NULL, &colfamily, &colfamily_len,
                          NULL, &column, &column_len)) {
        PyErr_SetString(PyExc_SyntaxError, "Invalid Parameter");
        return NULL;
    }
    
    log("Delete(%d) client %lu\n", getpid(), kclient);
    client = (hbase_client_t *)kclient;
    if (client == NULL) {
        PyErr_SetString(PyExc_SyntaxError, "Invalid Parameter");
        return NULL;
    }

    res = Delete(client, table, table_len, row, row_len,
                 colfamily, colfamily_len, column, column_len);
    if (res == -1) {
        return NULL;
    }

    pirate = Py_BuildValue("i", res);

    if (_diagnostics && (_freq != 0)) {
        _tend = getTicks();
        _tduration = (((_tend - _tbegin)) * (uint64_t)(1000000)) / _freq;
    }

    return pirate;
}

/* int WaitPuts(connect) */
static PyObject *
hbase_WaitPuts(PyObject *self, PyObject *args)
{
    return Py_BuildValue("i", 0);
}

/* int WaitGets(connect) */
static PyObject *
hbase_WaitGets(PyObject *self, PyObject *args)
{
    return Py_BuildValue("i", 0);
}

/* unsigned long Logger(void *function) */
static PyObject *
hbase_Logger(PyObject *self, PyObject *args)
{
    PyObject *temp;

    if (PyArg_ParseTuple(args, "O:Log", &temp)) {
        if (!PyCallable_Check(temp)) {
            PyErr_SetString(PyExc_TypeError, "parameter must be callable");
            return NULL;
        }

        Py_XINCREF(temp);
        Py_XDECREF(Logger); // Removes any previous definition
        Logger = temp;
        Py_INCREF(Logger);
        log("Logger is %p\n", Logger);
    }
    return Py_BuildValue("k", Logger);
}

/* unsigned long Error(void *function) */
static PyObject *
hbase_Error(PyObject *self, PyObject *args)
{
    PyObject *temp;

    if (PyArg_ParseTuple(args, "O:Err", &temp)) {
        if (!PyCallable_Check(temp)) {
            PyErr_SetString(PyExc_TypeError, "parameter must be callable");
            return NULL;
        }

        Py_XINCREF(temp);
        Py_XDECREF(Error); // Removes any previous definition

        Error = temp;
        Py_INCREF(Error);
        log("Error logs to %p\n", Error);
    }
    return Py_BuildValue("k", Error);
}

/* int JumpStart(void) */
static PyObject *
hbase_JumpStart(PyObject *self, PyObject *args)
{
    char *memsize;
    JNIEnv *env;

    if (!PyArg_ParseTuple(args,"s", &memsize)) {
        PyErr_SetString(PyExc_SyntaxError, "Invalid Parameter");
        return NULL;
    }

    env = get_jni_env(memsize);
    log("Jump Started\n");
    return Py_BuildValue("k", (unsigned long)env);
}


static PyObject *
hbase_DiagSet(PyObject *self, PyObject *args)
{
    int kop;

    if (!PyArg_ParseTuple(args,"i", &kop)) {
        PyErr_SetString(PyExc_SyntaxError, "Invalid Parameter");
        return NULL;
    }

    _diagnostics = (int)kop;
    log("Diagnostics set to %d\n", _diagnostics);

    return Py_BuildValue("i", _diagnostics);
}

static PyObject *
hbase_DiagGet(PyObject *self, PyObject *args)
{
    return Py_BuildValue("k", _tduration);
}

static PyObject *
hbase_Version(PyObject *self, PyObject *args)
{
    return Py_BuildValue("f", (float)HBASE_CLIENT_VERSION);
}

/* Module method table */
static PyMethodDef HbaseMethods[] = {
  {"Get",  hbase_Get, METH_VARARGS, "Start a read operation"},
  {"Put",  hbase_Put, METH_VARARGS, "Start a write operation"},
  {"Scan",  hbase_Scan, METH_VARARGS, "Start a scan operation"},
  {"Data",  hbase_Data, METH_VARARGS, "Read the data value from a previous get request"},
  {"Cell",  hbase_Cell, METH_VARARGS, "Read the cell data from a previous get request"},
  {"DataScan",  hbase_DataScan, METH_VARARGS, "Read the data value from a previous scan request"},
  {"Connect",  hbase_Connect, METH_VARARGS, "Connect to an Hbase cluster"},
  {"Disconnect",  hbase_Disconnect, METH_VARARGS, "Disconnect from an Hbase cluster & clean up"},
  {"Flush",  hbase_Flush, METH_VARARGS, "Flush all Hbase client operations"},
  {"SetAutoFlush",  hbase_SetAutoFlush, METH_VARARGS, "Set Auto Flush state; true or false"},
  {"Delete",  hbase_Delete, METH_VARARGS, "Mark cells for deletion"},  
  {"WaitPuts",  hbase_WaitPuts, METH_VARARGS, "Wait for completion of all writes"},
  {"WaitGets",  hbase_WaitGets, METH_VARARGS, "Wait for completion of all reads"},
  {"Logger",  hbase_Logger, METH_VARARGS, "Define the Python function to call to log"},
  {"Error",  hbase_Error, METH_VARARGS, "Define the Python function to call to report errors"},
  {"JumpStart",  hbase_JumpStart, METH_VARARGS, "Force the VM to start before any requests are made; for responser & streamer"},
  {"DiagSet",  hbase_DiagSet, METH_VARARGS, "Enable or disable diagnostics mode"},
  {"DiagGet",  hbase_DiagGet, METH_VARARGS, "Return the most recent duration measurement"},
  {"Version",  hbase_Version, METH_VARARGS, "Return the version numnber of this Hbase Client"},
  { NULL, NULL, 0, NULL}
};

#if PYTHON == 2
void
inithbase(void)
{
  /* Create the module and add the functions */
  Py_InitModule("hbase", HbaseMethods);
}
#else
/* Module structure */
static struct PyModuleDef hbasemodule = {
  PyModuleDef_HEAD_INIT,
  "hbase",           /* name of module */
  "Hbase Client Interface Module",  /* Doc string (may be NULL) */
  -1,                 /* Size of per-interpreter state or -1 */
  HbaseMethods       /* Method table */
};

/* Module initialization function */
PyMODINIT_FUNC
PyInit_hbase(void) {
  return PyModule_Create(&hbasemodule);
}
#endif // PYTHON
#endif // PYTHON_INTERFACE
