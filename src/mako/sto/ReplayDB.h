
#ifndef SILO_ONS_BENCHMARKS_STO_REPLAYDB_H_
#define SILO_ONS_BENCHMARKS_STO_REPLAYDB_H_
#include <functional>
#include <iostream>
#include <string_view>
#include <vector>
#include "storage/abstract_db.h"

// Single timestamp system commit info
struct CommitInfo {
    uint32_t timestamp; // timestamp*10+term
    uint32_t latency_tracker;
};

/**
 * @brief: decode buffer and then replay records
 *
 */
size_t treplay_in_same_thread_opt_mbta_v2(size_t par_id, char *buffer, size_t len, abstract_db* db, int nshards);

/**
 * @brief Get the latest commit info from buffer (single timestamp system)
 *
 */
CommitInfo get_latest_commit_info(char *buffer, size_t len);

/**
 * @brief Calls fn for every write in one log entry (the buffer a thread
 * pushes to Paxos and persists; Transaction::serialize_util writes it), in
 * order: the transaction's commit id (timestamp*10 + term), the table id
 * without the delete flag, whether the write deletes the key, and the key
 * and value bytes. Reads the buffer only and applies nothing. Returns false,
 * after the writes before it, if the entry is cut short.
 */
using LogWriteFn = std::function<void(uint32_t cid, unsigned short table_id, bool removed,
                                      std::string_view key, std::string_view value)>;
bool for_each_log_write(const char *buffer, size_t len, const LogWriteFn &fn);

#endif