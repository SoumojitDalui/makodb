#pragma once

// OrderedIndexKvStore — the adapter that binds the cluster KvStore port
// onto Mako's unified FullOrderedIndex (abstract_ordered_index). It
// lives on the mako side, NOT in src/cluster/, so the cluster metadata
// component stays free of storage-engine headers. Production wiring
// (shard 0 bootstrap) constructs the __mako_config__ mbta index, wraps
// it in an OrderedIndexKvStore, and hands that to ConfigManager.
//
// ConfigManager uses only the non-txn OrderedIndex point ops, and on
// that surface values are raw bytes in both directions (no
// mako::Encode) — see abstract_ordered_index.h. So the adapter is a
// straight std::string <-> lcdf::Str shim.
//
// Threads. Each non-txn op runs a one-op STO transaction, which needs
// per-thread engine state (an STO thread id and a Masstree threadinfo).
// Mako workers and RPC helpers get that from abstract_db::thread_init;
// the threads that reach this store do not: the ConfigWatcher poll
// thread and the ConfigKvService handler thread. Without it their first
// access dereferences a null threadinfo. So the store takes an optional
// hook and runs it once on every thread it has not seen. The bootstrap
// thread, which is already initialized when it seeds the store, is
// marked ready instead (mark_current_thread_ready), because running
// thread_init on a live thread would hand it a second STO thread id.
// Tests over a fake index pass no hook.

import cluster;   // config/sharding metadata module (was #include "cluster/...")
#include "storage/abstract_ordered_index.h"

#include <string>
#include <rusty/option.hpp>   // KvStore::get returns rusty::Option<std::string>

namespace janus {

// @safe - thin shim from the KvStore port to a FullOrderedIndex.
class OrderedIndexKvStore : public KvStore {
public:
    // Plain function pointer + context so this header needs no
    // storage-engine (abstract_db) include.
    using ThreadInitHook = void (*)(void* ctx);

    // Non-owning: the index outlives this adapter (owned by the shard).
    explicit OrderedIndexKvStore(::FullOrderedIndex* index,
                                 ThreadInitHook init_thread = nullptr,
                                 void* init_ctx = nullptr)
        : index_(index), init_thread_(init_thread), init_ctx_(init_ctx) {}

    // Record that the calling thread already has engine state, so the
    // hook never re-initializes it.
    static void mark_current_thread_ready() { thread_ready() = true; }

    // @unsafe - FullOrderedIndex point read
    rusty::Option<std::string> get(const std::string& key) override {
        if (index_ == nullptr) return rusty::None;
        ensure_thread_ready();
        std::string out;
        if (index_->get(lcdf::Str(key.data(), key.size()), out,
                        std::string::npos)) {
            return rusty::Some(std::move(out));
        }
        return rusty::None;
    }

    // @unsafe - FullOrderedIndex point put
    void put(const std::string& key, const std::string& value) override {
        if (index_ != nullptr) {
            ensure_thread_ready();
            index_->put(lcdf::Str(key.data(), key.size()), value);
        }
    }

    // @unsafe - FullOrderedIndex point remove
    void remove(const std::string& key) override {
        if (index_ != nullptr) {
            ensure_thread_ready();
            index_->remove(lcdf::Str(key.data(), key.size()));
        }
    }

private:
    // One flag per thread, shared by every store: engine state is per
    // thread, not per store.
    static bool& thread_ready() {
        thread_local bool ready = false;
        return ready;
    }

    void ensure_thread_ready() {
        if (init_thread_ == nullptr || thread_ready()) return;
        init_thread_(init_ctx_);
        thread_ready() = true;
    }

    ::FullOrderedIndex* index_;
    ThreadInitHook init_thread_;
    void* init_ctx_;
};

}  // namespace janus
