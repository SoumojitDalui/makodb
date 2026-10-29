#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "ReplayDB.h"
#include "ThreadPool.h"

import std;

// @unsafe: uses memcpy and pointer arithmetic
// Single timestamp system: extracts timestamp and latency tracker from buffer
CommitInfo get_latest_commit_info(char *buffer, size_t len) {
    CommitInfo info;
    
    // Single timestamp format: last 2 uint32_t values are [timestamp, latency_tracker]
    int pos = len - sizeof(uint32_t) * 2;
    
    memcpy(&info.timestamp, buffer + pos, sizeof(uint32_t));
    pos += sizeof(uint32_t);
    memcpy(&info.latency_tracker, buffer + pos, sizeof(uint32_t));
    
    return info;
}

// @unsafe: uses memcpy, pointer arithmetic, and raw pointer management
size_t treplay_in_same_thread_opt_mbta_v2(size_t par_id, char *buffer, size_t len, abstract_db* db, int nshards) {
    //printf("replay a log, par_id:%d, len:%d\n", par_id, len);
    len -= sizeof(uint32_t) * 2; // eliminate last timestamp and latency_tracker
    size_t ULL_LEN = sizeof (uint32_t);

    std::vector<WrappedLogV2*> _intermediateLogs;

    size_t idx=0;
    int c_txn=0, c_kv=0;
    while (idx < len) {
        // 1. get current shard cid
        c_txn+=1;
        uint32_t cid=0;
        memcpy ((char *) &cid, buffer + idx, ULL_LEN);
        idx += sizeof(uint32_t);

        // 2. get count of K-V
        unsigned short int count=0;
        memcpy ((char *) &count, buffer + idx, sizeof(unsigned short int));
        idx += sizeof(unsigned short int) ;
        c_kv+=count;

        // 3. get len of K-V
        unsigned int _len=0;
        memcpy ((char *) &_len, buffer + idx, sizeof(unsigned int));
        idx += sizeof(unsigned int) ;

        // 4. wrap K-V
        auto on_log = new WrappedLogV2() ;
        on_log->cid= cid;
        on_log->count = count;
        on_log->len = _len;
        on_log->pairs = (char *)buffer + idx;

        // 5. skip K-V pairs
        idx += _len ;

        _intermediateLogs.emplace_back (on_log);
    }

    size_t thread_put = 0;
    auto iter = _intermediateLogs.begin();
    for (;iter!=_intermediateLogs.end();iter++) {
        thread_put += getFileContentNew_OneLogOptimized_mbta_v2((*iter)->pairs,
                                                                (*iter)->cid,
                                                                (*iter)->count,
                                                                (*iter)->len,
                                                                db) ;
    }
    //Warning("%d K-V pairs replayed, par_id:%d, total-len: %d, c_txn:%d, c_kv:%d", thread_put, par_id, len, c_txn, c_kv);
    return thread_put;
}

// @unsafe: uses memcpy and pointer arithmetic
bool for_each_log_write(const char *buffer, size_t len, const LogWriteFn &fn) {
    // The entry ends with the latest commit id and a latency tracker.
    if (len < sizeof(uint32_t) * 2) return false;
    len -= sizeof(uint32_t) * 2;

    size_t idx = 0;
    while (idx < len) {
        // A transaction: commit id, count of writes, their length in bytes.
        uint32_t cid = 0;
        unsigned short int count = 0;
        unsigned int kv_len = 0;
        if (len - idx < sizeof(cid) + sizeof(count) + sizeof(kv_len)) return false;
        memcpy(&cid, buffer + idx, sizeof(cid));
        idx += sizeof(cid);
        memcpy(&count, buffer + idx, sizeof(count));
        idx += sizeof(count);
        memcpy(&kv_len, buffer + idx, sizeof(kv_len));
        idx += sizeof(kv_len);
        if (kv_len > len - idx) return false;

        // Each write: key length and key, value length and value, table id
        // (the top bit marks a delete).
        const char *pairs = buffer + idx;
        size_t off = 0;
        for (unsigned short int i = 0; i < count; i++) {
            unsigned short int k_len = 0, v_len = 0, table_id = 0;
            if (kv_len - off < sizeof(k_len)) return false;
            memcpy(&k_len, pairs + off, sizeof(k_len));
            off += sizeof(k_len);
            if (kv_len - off < k_len) return false;
            std::string_view key(pairs + off, k_len);
            off += k_len;

            if (kv_len - off < sizeof(v_len)) return false;
            memcpy(&v_len, pairs + off, sizeof(v_len));
            off += sizeof(v_len);
            if (kv_len - off < v_len) return false;
            std::string_view value(pairs + off, v_len);
            off += v_len;

            if (kv_len - off < sizeof(table_id)) return false;
            memcpy(&table_id, pairs + off, sizeof(table_id));
            off += sizeof(table_id);

            const bool removed = (table_id & (1 << 15)) != 0;
            fn(cid, static_cast<unsigned short>(table_id & ~(1 << 15)), removed, key, value);
        }
        idx += kv_len;
    }
    return true;
}
