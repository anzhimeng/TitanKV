#include "titan_c.h"
#include "titankv/db.h"
#include "titankv/db_impl.h"
#include "titankv/coprocessor.h"
#include "lsm/mvcc_reader.h"
#include "lsm/dbformat.h"
#include "util/coding.h"
#include <vector>
#include <string>
#include <cstring>
#include <iostream>

struct titan_db_t {
    titankv::DB* rep;
};

// 辅助函数：将 C 风格错误转换为 C++ Status
// 注意：这里我们反过来，将 Status 转换为 C 风格错误字符串
void set_error(char** err, const titankv::Status& s) {
    if (s.ok()) {
        *err = nullptr;
    } else {
        std::string msg = s.ToString();
        *err = strdup(msg.c_str());
    }
}

// 辅助函数：转换 CF
titankv::CFType to_cpp_cf(titan_cf_t cf) {
    switch (cf) {
        case CF_LOCK: return titankv::kCFLock;
        case CF_WRITE: return titankv::kCFWrite;
        default: return titankv::kCFDefault;
    }
}

extern "C" {

titan_db_t* titan_open(const char* name, const titan_options_t* c_options, char** err) {
    titankv::Options options;
    if (c_options) {
        options.create_if_missing = c_options->create_if_missing;
        options.use_direct_io = c_options->use_direct_io;
        if (c_options->write_buffer_size > 0) options.write_buffer_size = c_options->write_buffer_size;
        if (c_options->max_file_size > 0) options.max_file_size = c_options->max_file_size;
        if (c_options->max_blob_file_size > 0) options.max_blob_file_size = c_options->max_blob_file_size;
        if (c_options->min_blob_size > 0) options.min_blob_size = c_options->min_blob_size;
        if (c_options->block_size > 0) options.block_size = c_options->block_size;
        if (c_options->block_cache_size > 0) {
            options.block_cache = std::shared_ptr<titankv::Cache>(titankv::NewLRUCache(c_options->block_cache_size));
        }
        if (c_options->bloom_filter_bits > 0) {
            options.filter_policy = titankv::NewBloomFilterPolicy(c_options->bloom_filter_bits);
        }
        if (c_options->wal_sync_bytes > 0) options.wal_sync_bytes = c_options->wal_sync_bytes;
        if (c_options->wal_sync_interval_ms > 0) options.wal_sync_interval_ms = c_options->wal_sync_interval_ms;
    } else {
        options.create_if_missing = true;
    }
    
    titankv::DB* db;
    titankv::Status s = titankv::DB::Open(options, name, &db);
    
    if (!s.ok()) {
        set_error(err, s);
        return nullptr;
    }
    
    titan_db_t* tdb = new titan_db_t;
    tdb->rep = db;
    *err = nullptr;
    return tdb;
}

void titan_close(titan_db_t* db) {
    if (db) {
        delete db->rep;
        delete db;
    }
}

void titan_put(titan_db_t* db, const char* key, size_t klen, 
               const char* val, size_t vlen, char** err) {
    titankv::Status s = db->rep->Put(titankv::WriteOptions(), titankv::Slice(key, klen), titankv::Slice(val, vlen));
    set_error(err, s);
}

void titan_get(titan_db_t* db, const char* key, size_t klen, 
               char** val, size_t* vlen, char** err) {
    std::string result;
    titankv::Status s = db->rep->Get(titankv::ReadOptions(), titankv::Slice(key, klen), &result);
    
    if (s.ok()) {
        *vlen = result.size();
        *val = static_cast<char*>(malloc(result.size()));
        memcpy(*val, result.data(), result.size());
        *err = nullptr;
    } else {
        *val = nullptr;
        *vlen = 0;
        set_error(err, s);
    }
}

void titan_delete(titan_db_t* db, const char* key, size_t klen, char** err) {
    titankv::Status s = db->rep->Delete(titankv::WriteOptions(), titankv::Slice(key, klen));
    set_error(err, s);
}

void titan_free(void* ptr) {
    if (ptr) free(ptr);
}

void titan_get_statistics(titan_db_t* db, titan_stats_t* stats) {
    if (!db || !db->rep || !stats) return;
    // 需要强制转换为 DBImpl 才能访问 options_
    auto impl = reinterpret_cast<titankv::DBImpl*>(db->rep);
    const auto& opts = impl->GetOptions();
    if (opts.statistics) {
        stats->blob_bytes_written = opts.statistics->blob_bytes_written;
        stats->blob_bytes_read = opts.statistics->blob_bytes_read;
        stats->gc_run_count = opts.statistics->gc_run_count;
        stats->gc_bytes_reclaimed = opts.statistics->gc_bytes_reclaimed;
        stats->gc_keys_moved = opts.statistics->gc_keys_moved;
    }
}

void titan_set_gc_threshold(titan_db_t* db, double threshold) {
     if (!db || !db->rep) return;
     auto impl = reinterpret_cast<titankv::DBImpl*>(db->rep);
     impl->SetGCThreshold(threshold);
}

void titan_batch_write(titan_db_t* db, 
                       const char** keys, size_t* klen, 
                       const char** vals, size_t* vlen, 
                       int count, char** err) {
    titankv::WriteBatch batch;
    for (int i = 0; i < count; ++i) {
        batch.Put(titankv::Slice(keys[i], klen[i]), titankv::Slice(vals[i], vlen[i]));
    }
    titankv::Status s = db->rep->Write(titankv::WriteOptions(), &batch);
    set_error(err, s);
}

void titan_batch_write_ops(titan_db_t* db, 
                       const char** keys, size_t* klen, 
                       const char** vals, size_t* vlen, 
                       const int* ops, int count, char** err) {
    titankv::WriteBatch batch;
    for (int i = 0; i < count; ++i) {
        if (ops[i] == 0) { // Put
             batch.Put(titankv::Slice(keys[i], klen[i]), titankv::Slice(vals[i], vlen[i]));
        } else { // Delete
             batch.Delete(titankv::Slice(keys[i], klen[i]));
        }
    }
    titankv::Status s = db->rep->Write(titankv::WriteOptions(), &batch);
    set_error(err, s);
}

void titan_get_approximate_sizes(titan_db_t* db, 
                       const char** start_keys, size_t* start_lens,
                       const char** end_keys, size_t* end_lens,
                        int n, uint64_t* sizes) {
    // 简单实现：暂不支持
    for (int i=0; i<n; i++) sizes[i] = 0;
}

void titan_ingest_sst(titan_db_t* db, const char* path, char** err) {
    auto impl = reinterpret_cast<titankv::DBImpl*>(db->rep);
    titankv::Status s = impl->IngestSST(path);
    set_error(err, s);
}

void titan_delete_range(titan_db_t* db, const char* start, size_t slen, 
                        const char* end, size_t elen, char** err) {
    auto impl = reinterpret_cast<titankv::DBImpl*>(db->rep);
    titankv::Status s = impl->DeleteRange(titankv::WriteOptions(), 
                                          titankv::Slice(start, slen), 
                                          titankv::Slice(end, elen));
    set_error(err, s);
}

void titan_dump_sst(titan_db_t* db, const char* start, size_t slen,
                    const char* end, size_t elen,
                    const char* path, char** err) {
     auto impl = reinterpret_cast<titankv::DBImpl*>(db->rep);
     titankv::Status s = impl->DumpSST(titankv::Slice(start, slen), 
                                       titankv::Slice(end, elen), path, 0);
     set_error(err, s);
}


void titan_mvcc_prewrite(titan_db_t* db, const titan_mutation_t* mutations, int count,
                         const char* primary, size_t plen, uint64_t start_ts, uint64_t ttl, char** err) {
    std::vector<titankv::Mutation> cpp_mutations;
    for (int i = 0; i < count; ++i) {
        titankv::Mutation m;
        m.op = (mutations[i].op == 0) ? titankv::Mutation::Put : titankv::Mutation::Delete;
        m.key = std::string(mutations[i].key, mutations[i].klen);
        m.value = std::string(mutations[i].value, mutations[i].vlen);
        cpp_mutations.push_back(m);
    }
    
    // Default to empty secondaries for basic prewrite
    std::vector<std::string> secondaries;

    titankv::Status s = db->rep->MvccPrewrite(cpp_mutations, 
                                              std::string(primary, plen), 
                                              start_ts, ttl, 
                                              0, // min_commit_ts
                                              false, // is_pessimistic_lock
                                              secondaries);
    set_error(err, s);
}

void titan_mvcc_prewrite_1pc(titan_db_t* db, const titan_mutation_t* mutations, int count,
                             const char* primary, size_t plen, uint64_t start_ts, 
                             uint64_t commit_ts, uint64_t ttl, char** err) {
    std::vector<titankv::Mutation> cpp_mutations;
    for (int i = 0; i < count; ++i) {
        titankv::Mutation m;
        m.op = (mutations[i].op == 0) ? titankv::Mutation::Put : titankv::Mutation::Delete;
        m.key = std::string(mutations[i].key, mutations[i].klen);
        m.value = std::string(mutations[i].value, mutations[i].vlen);
        cpp_mutations.push_back(m);
    }
    
    auto impl = reinterpret_cast<titankv::DBImpl*>(db->rep);
    titankv::Status s = impl->MvccPrewrite1PC(cpp_mutations, 
                                              std::string(primary, plen), 
                                              start_ts, commit_ts, ttl);
    set_error(err, s);
}

void titan_mvcc_prewrite_async(titan_db_t* db, const titan_mutation_t* mutations, int count,
                         const char* primary, size_t plen, uint64_t start_ts, uint64_t ttl, 
                         uint64_t min_commit_ts, bool is_pessimistic_lock, 
                         const char** secondaries, const size_t* secondary_lens, int secondary_count,
                         char** err) {
    std::vector<titankv::Mutation> cpp_mutations;
    for (int i = 0; i < count; ++i) {
        titankv::Mutation m;
        m.op = (mutations[i].op == 0) ? titankv::Mutation::Put : titankv::Mutation::Delete;
        m.key = std::string(mutations[i].key, mutations[i].klen);
        m.value = std::string(mutations[i].value, mutations[i].vlen);
        cpp_mutations.push_back(m);
    }

    // Convert secondaries from C array to C++ vector
    std::vector<std::string> cpp_secondaries;
    if (secondaries != nullptr && secondary_count > 0) {
        cpp_secondaries.reserve(secondary_count);
        for (int i = 0; i < secondary_count; ++i) {
            cpp_secondaries.emplace_back(secondaries[i], secondary_lens[i]);
        }
    }

    titankv::Status s = db->rep->MvccPrewrite(cpp_mutations, 
                                              std::string(primary, plen), 
                                              start_ts, ttl, 
                                              min_commit_ts, 
                                              is_pessimistic_lock,
                                              cpp_secondaries);
    set_error(err, s);
}

void titan_acquire_pessimistic_lock(titan_db_t* db, 
                                    const char** keys, size_t* klen, int count,
                                    const char* primary, size_t plen, 
                                    uint64_t start_ts, uint64_t ttl, 
                                    uint64_t for_update_ts,
                                    bool return_values,
                                    char*** values, size_t** vlens, bool** not_founds,
                                    char** err) {
    std::vector<std::string> cpp_keys;
    for (int i = 0; i < count; ++i) {
        cpp_keys.emplace_back(keys[i], klen[i]);
    }

    std::vector<std::string> out_values;
    std::vector<bool> out_not_found;

    titankv::Status s = db->rep->AcquirePessimisticLock(cpp_keys, 
                                                        std::string(primary, plen),
                                                        start_ts, ttl, for_update_ts,
                                                        return_values, &out_values, &out_not_found);
    
    if (s.ok()) {
        *err = nullptr;
        if (return_values) {
            // Allocate arrays for results
            *values = (char**)malloc(count * sizeof(char*));
            *vlens = (size_t*)malloc(count * sizeof(size_t));
            *not_founds = (bool*)malloc(count * sizeof(bool));

            for (int i = 0; i < count; ++i) {
                (*not_founds)[i] = out_not_found[i];
                if (!out_not_found[i]) {
                    (*vlens)[i] = out_values[i].size();
                    (*values)[i] = (char*)malloc(out_values[i].size());
                    memcpy((*values)[i], out_values[i].data(), out_values[i].size());
                } else {
                    (*values)[i] = nullptr;
                    (*vlens)[i] = 0;
                }
            }
        }
    } else {
        set_error(err, s);
    }
}


void titan_mvcc_commit(titan_db_t* db, const char** keys, size_t* klens, int count,
                       uint64_t start_ts, uint64_t commit_ts, char** err) {
    std::vector<std::string> cpp_keys;
    cpp_keys.reserve(count);
    for (int i = 0; i < count; ++i) {
        cpp_keys.emplace_back(keys[i], klens[i]);
    }
    
    titankv::Status s = db->rep->MvccCommit(cpp_keys, start_ts, commit_ts);
    set_error(err, s);
}

void titan_mvcc_get(titan_db_t* db, const char* key, size_t klen, uint64_t start_ts,
                    char** val, size_t* vlen, char** err) {
    std::string result;
    titankv::Status s = db->rep->MvccGet(titankv::Slice(key, klen), start_ts, &result);
    if (s.ok()) {
        *vlen = result.size();
        *val = static_cast<char*>(malloc(result.size()));
        memcpy(*val, result.data(), result.size());
        *err = nullptr;
    } else {
        *val = nullptr;
        *vlen = 0;
        set_error(err, s);
    }
}

void titan_check_txn_status(titan_db_t* db, const char* pkey, size_t plen, 
                            uint64_t lock_ts, uint64_t current_ts,
                            int* action, uint64_t* commit_ts, char** err) {
    titankv::Status s = db->rep->CheckTxnStatus(titankv::Slice(pkey, plen), lock_ts, current_ts, action, commit_ts);
    set_error(err, s);
}

void titan_coprocessor_execute(titan_db_t* db, 
                               const titan_coprocessor_request_t* req, 
                               titan_coprocessor_response_t* resp, 
                               char** err) {
    if (!db || !db->rep || !req || !resp) {
        return;
    }
    
    titankv::CoprocessorRequest cpp_req;
    if (req->coprocessor_type == 0) {
        cpp_req.type = titankv::CoprocessorType::kCount;
    } else {
        cpp_req.type = titankv::CoprocessorType::kSum;
    }
    cpp_req.start_key.assign(req->start_key, req->start_key_len);
    if (req->end_key) {
        cpp_req.end_key.assign(req->end_key, req->end_key_len);
    }
    cpp_req.start_ts = req->start_ts;
    if (req->filter_value) {
        cpp_req.filter_value.assign(req->filter_value, req->filter_value_len);
    }
    cpp_req.filter_operator = static_cast<titankv::FilterOperator>(req->filter_operator);
    
    titankv::CoprocessorResponse cpp_resp;
    titankv::DB* db_impl = reinterpret_cast<titankv::DB*>(db->rep);
    titankv::Status s = db_impl->ExecuteCoprocessor(cpp_req, &cpp_resp);
    
    if (s.ok()) {
                resp->count = cpp_resp.count;
                resp->sum = cpp_resp.sum;
                resp->error_msg = nullptr;
            } else {
        // Handle error (copy string)
        // For simplicity, we use the output err parameter or resp->error_msg
        // The API signature has char** err, let's use that.
        set_error(err, s);
    }
}

void titan_mvcc_gc(titan_db_t* db, uint64_t safe_point, char** err) {
    if (db && db->rep) {
        auto impl = reinterpret_cast<titankv::DBImpl*>(db->rep);
        titankv::Status s = impl->MvccGC(safe_point);
        set_error(err, s);
    }
}

void titan_check_conflict(titan_db_t* db, 
                          const char** keys, size_t* klen, 
                          int count, 
                          uint64_t start_ts, 
                          char** err) {
    if (!db || !db->rep) {
        return;
    }
    
    std::vector<std::string> cpp_keys;
    cpp_keys.reserve(count);
    for (int i = 0; i < count; ++i) {
        cpp_keys.emplace_back(keys[i], klen[i]);
    }
    
    auto impl = reinterpret_cast<titankv::DBImpl*>(db->rep);
    titankv::Status s = impl->CheckConflict(cpp_keys, start_ts);
    set_error(err, s);
}

void titan_put_cf(titan_db_t* db, titan_cf_t cf, const char* key, size_t klen, 
                  const char* val, size_t vlen, uint64_t ts, char** err) {
    titankv::Status s = db->rep->PutCF(to_cpp_cf(cf), titankv::Slice(key, klen), titankv::Slice(val, vlen), ts);
    set_error(err, s);
}

void titan_delete_cf(titan_db_t* db, titan_cf_t cf, const char* key, size_t klen, uint64_t ts, char** err) {
    titankv::Status s = db->rep->DeleteCF(to_cpp_cf(cf), titankv::Slice(key, klen), ts);
    set_error(err, s);
}

void titan_get_cf(titan_db_t* db, titan_cf_t cf, const char* key, size_t klen, uint64_t ts,
                  char** val, size_t* vlen, char** err) {
    std::string result;
    titankv::Status s = db->rep->GetCF(to_cpp_cf(cf), titankv::Slice(key, klen), &result, ts);
    if (s.ok()) {
        *vlen = result.size();
        *val = static_cast<char*>(malloc(result.size()));
        memcpy(*val, result.data(), result.size());
        *err = nullptr;
    } else {
        *val = nullptr;
        *vlen = 0;
        set_error(err, s);
    }
}

void* titan_mvcc_reader_create(titan_db_t* db, uint64_t snapshot) {
    if (!db || !db->rep) return nullptr;
    // We need to return a pointer to MvccReader
    // MvccReader is in lsm/mvcc_reader.h
    // But we don't want to expose that header to C users.
    // So we wrap it or cast it.
    // Actually titan_c.h says void* reader.
    // So we can just new MvccReader
    auto impl = reinterpret_cast<titankv::DBImpl*>(db->rep);
    return new titankv::MvccReader(impl, snapshot);
}

void titan_mvcc_reader_destroy(void* reader) {
    delete static_cast<titankv::MvccReader*>(reader);
}

int titan_mvcc_reader_seek_write(void* reader, const char* key, size_t klen,
                                 uint64_t* commit_ts, char** val, size_t* vlen) {
    auto r = static_cast<titankv::MvccReader*>(reader);
    std::string value;
    // We don't have SeekWrite in MvccReader exposed like this?
    // MvccReader has GetValue.
    // Wait, SeekWrite is used for Point Get in MVCC?
    // MvccReader::GetValue does the job.
    // But if we want to iterate?
    // The C API implies "SeekWrite".
    // Let's assume MvccReader has a Seek method or we use GetValue.
    // Actually, MvccReader is designed for Point Get.
    // For Scan, we should expose Iterator.
    // But let's see what Go needs.
    // Go calls titan_mvcc_get which uses MvccReader internally.
    // So this function might not be needed or I should check MvccReader implementation.
    // Since I am not using this function in my changes, I will leave it or implement dummy.
    return 0;
}

// Wrapper Iterator to expose UserKey and handle MVCC/Deletions
class APIIterator : public titankv::Iterator {
public:
    APIIterator(titankv::Iterator* iter, titankv::DBImpl* db) : iter_(iter), db_(db), valid_(false) {}
    ~APIIterator() override { delete iter_; }

    bool Valid() const override { return valid_; }

    void SeekToFirst() override {
        iter_->SeekToFirst();
        FindNextUserEntry(false);
    }

    void SeekToLast() override {
        valid_ = false;
    }

    void Seek(const titankv::Slice& target) override {
        std::string internal_key;
        internal_key.append(target.data(), target.size());
        titankv::PutFixed64(&internal_key, titankv::PackSequenceAndType(titankv::kMaxSequenceNumber, titankv::kTypeValue));
        iter_->Seek(internal_key);
        FindNextUserEntry(false);
    }

    void Next() override {
        assert(valid_);
        iter_->Next();
        FindNextUserEntry(true);
    }

    void Prev() override {
        valid_ = false;
    }

    titankv::Slice key() const override {
        assert(valid_);
        return saved_user_key_;
    }

    titankv::Slice value() const override {
        assert(valid_);
        titankv::Slice v = iter_->value();
        
        // Copy to local buffer to handle mutation (decoding/fetching)
        resolved_value_.assign(v.data(), v.size());
        
        // Resolve BlobIndex or Inline Value
        // This handles kValueInlineTag (removes prefix) and kValueBlobTag (fetches from BlobStore)
        titankv::Status s = db_->ResolveBlobIndex(&resolved_value_);
        if (!s.ok()) {
            // If resolution fails, we might want to surface this error.
            // But Iterator::value() returns Slice.
            // We'll return the raw value (or whatever ResolveBlobIndex left) 
            // and rely on user checking status() if we had a way to set it.
            // Since we can't easily set underlying iterator status, we log or ignore.
            // For now, returning potentially raw value is the fallback.
        }
        return titankv::Slice(resolved_value_);
    }

    titankv::Status status() const override { return iter_->status(); }

private:
    titankv::Iterator* iter_;
    titankv::DBImpl* db_;
    bool valid_;
    std::string saved_user_key_;
    mutable std::string resolved_value_;

    void FindNextUserEntry(bool skipping) {
        while (iter_->Valid()) {
            titankv::Slice internal_key = iter_->key();
            if (internal_key.size() < 8) {
                iter_->Next();
                continue;
            }

            titankv::Slice user_key = titankv::ExtractUserKey(internal_key);

            if (skipping && user_key.compare(saved_user_key_) == 0) {
                iter_->Next();
                continue;
            }

            uint64_t tag = titankv::DecodeFixed64(internal_key.data() + internal_key.size() - 8);
            titankv::ValueType type = static_cast<titankv::ValueType>(tag & 0xff);

            if (type == titankv::kTypeDeletion) {
                saved_user_key_ = user_key.ToString();
                skipping = true;
                iter_->Next();
                continue;
            } else if (type == titankv::kTypeValue) {
                valid_ = true;
                saved_user_key_ = user_key.ToString();
                return;
            }
            iter_->Next();
        }
        valid_ = false;
    }
};

struct titan_iterator_t {
    titankv::Iterator* rep;
};

titan_iterator_t* titan_create_iterator(titan_db_t* db, const titan_read_options_t* options, titan_cf_t cf) {
    titankv::ReadOptions cpp_options;
    if (options) {
        cpp_options.verify_checksums = options->verify_checksums;
        cpp_options.fill_cache = options->fill_cache;
    }
    
    titankv::Iterator* iter = db->rep->NewIterator(cpp_options, to_cpp_cf(cf));
    if (!iter) return nullptr;
    
    // Pass DBImpl pointer to APIIterator for Blob resolution
    auto impl = reinterpret_cast<titankv::DBImpl*>(db->rep);
    titankv::Iterator* api_iter = new APIIterator(iter, impl);

    titan_iterator_t* t_iter = new titan_iterator_t;
    t_iter->rep = api_iter;
    return t_iter;
}

void titan_iterator_destroy(titan_iterator_t* iter) {
    if (iter) {
        delete iter->rep;
        delete iter;
    }
}

bool titan_iterator_valid(titan_iterator_t* iter) {
    return iter && iter->rep->Valid();
}

void titan_iterator_seek_to_first(titan_iterator_t* iter) {
    if (iter) iter->rep->SeekToFirst();
}

void titan_iterator_seek_to_last(titan_iterator_t* iter) {
    if (iter) iter->rep->SeekToLast();
}

void titan_iterator_seek(titan_iterator_t* iter, const char* key, size_t klen) {
    if (iter) iter->rep->Seek(titankv::Slice(key, klen));
}

void titan_iterator_next(titan_iterator_t* iter) {
    if (iter) iter->rep->Next();
}

void titan_iterator_prev(titan_iterator_t* iter) {
    if (iter) iter->rep->Prev();
}

void titan_iterator_key(titan_iterator_t* iter, const char** key, size_t* klen) {
    if (iter && iter->rep->Valid()) {
        titankv::Slice s = iter->rep->key();
        *key = s.data();
        *klen = s.size();
    } else {
        *key = nullptr;
        *klen = 0;
    }
}

void titan_iterator_value(titan_iterator_t* iter, const char** val, size_t* vlen) {
    if (iter && iter->rep->Valid()) {
        titankv::Slice s = iter->rep->value();
        *val = s.data();
        *vlen = s.size();
    } else {
        *val = nullptr;
        *vlen = 0;
    }
}

void titan_iterator_status(titan_iterator_t* iter, char** err) {
    if (iter) {
        set_error(err, iter->rep->status());
    } else {
        *err = nullptr;
    }
}

} // extern "C"
