#include <cstring>
#include <string>
#include <atomic>
#include <mutex>
#include <iostream>
#include <fstream>
#include <vector>
#include <memory>
#include <boost/thread/shared_mutex.hpp>
#include <glog/logging.h>
#include <shared_mutex>
#include <functional>


namespace ox {

    using std::cout;
    using std::cerr;
    using std::atomic;
    using std::endl;
    using std::ostream;
    using std::ofstream;
    using std::vector;
    using std::memcpy;
    using std::string;
    using std::unique_ptr;
    using boost::shared_mutex;
    using std::function;

    static constexpr uint32_t ALIGNMENT = 4096;
    static constexpr uint32_t MIN_PAGE_SIZE = 4096;

    // Disk-based hash table
    // ---------------------
    // 
    //
    // File Structure
    // page 0:                  super-page, all kinds of meta data
    // next page:               backup super-page
    //                          when update super-page, first copy old super-page to backup super-page
    // next (def 512) pages:    allocation bitmap
    //                          currently is not used, all bits are 0
    // next (def 2048) pages:    hash table index
    class HashTable {
        // SuperPage is an on-disk data structure
        // It's only allocated for disk writing
        struct __attribute__((__packed__)) SuperPage {
            static constexpr uint64_t MAGIC = 0x59AD59AD59AD59AD;
            static constexpr uint32_t VERSION = 1;
            uint64_t magic;
            uint32_t version;
            // config
            uint32_t page_size;
            uint32_t max_pages;     // need this to allocate bitmap
            uint32_t max_buckets;

            // global counters
            uint32_t next_page;
            uint32_t next_bucket;
        } super;
    public:
        // Size configuration
        struct Config {
            static constexpr uint32_t DEFAULT_PAGE_SIZE = 32 * 1024;
            uint32_t    page_size;
            static constexpr uint32_t DEFAULT_MAX_PAGES = 128 * 1024 * 1024;
            uint32_t    max_pages;
            static constexpr uint32_t DEFAULT_MAX_BUCKETS = 16 * 1024 * 1024;
            uint32_t    max_buckets;

            // Typical entry size:
            //      Timestamp 4-bytes
            //      ID  8-bytes
            //      Feature 391-bytes
            //      ......
            //      Page Size 16K = 32 objects
            //
            // Typical setting:
            //      Object size:            512 bytes
            //      Max database size:      1 billion objects
            //      Max uniq data size:     512G
            //      Page size:              16K, or 32 objects
            //      Max pages for obj:      32 million
            //      Max bitmap size:        4MB, or 256 pages
            //      Max Buckets:            10 millions
            //      Max buckets size:       4MB or 256 pages
            //

            // constraints
            // # buckets pages have to reside in memory
            // so max memory consumption is max_buckets * page_size, which by default is 
            //
            Config ()
                : page_size(DEFAULT_PAGE_SIZE),
                max_pages(DEFAULT_MAX_PAGES),
                max_buckets(DEFAULT_MAX_BUCKETS)
            {
                CHECK(page_size >= MIN_PAGE_SIZE);
            }
            Config (SuperPage const &sp)
                : page_size(sp.page_size),
                max_pages(sp.max_pages),
                max_buckets(sp.max_buckets) {
            }
        };

    private:
        // disk layout
        // 1 super page
        // 1 super page
        // bitmap pages
        // index pages
        // bucket pages
        struct Geometry: public Config {
            uint32_t bitmap_pages;
            uint32_t index_pages;
            uint32_t bitmap_offset;
            uint32_t index_offset;
            uint32_t bucket_offset;
            Geometry () {}
            Geometry (Config const &conf)
                : Config(conf),
                bitmap_pages(conf.max_pages / 8 / conf.page_size),
                index_pages(conf.max_buckets * sizeof(int32_t) / conf.page_size),
                bitmap_offset(2),
                index_offset(bitmap_offset + bitmap_pages),
                bucket_offset(index_offset + index_pages)
            {
                CHECK(0 == (conf.page_size & (conf.page_size -1)));
                CHECK(0 == conf.max_pages % (8 * conf.page_size));
                CHECK(0 == conf.max_buckets * sizeof(int32_t) % conf.page_size);
            }

            off64_t page_offset (uint32_t page) {
                return off64_t(page_size) * page;
            }
        } geom;


        // Bucket Page Structure
        // uint32_t next_page, 0 if no next page
        //
        //
        // Bucket structure
        //
        // Always in memory:    head page
        //
        // On disk:

        // Page is not synchronized
        struct Page {
            struct __attribute__((__packed__)) PageData {
                uint32_t next;
                uint32_t used;  // used bytes
                char data[1];
                static constexpr uint32_t head_size () {
                    return sizeof(PageData) - 1;
                }
            } *data;
            uint32_t size;  // total size
            uint32_t free;

        public:
            Page (): data(nullptr) {
                static_assert(PageData::head_size() < MIN_PAGE_SIZE, "");
            }

            ~Page () {
                if (data) {
                    ::free(data);
                }
            }
            uint32_t next_page () const {
                CHECK(data);
                return data->next;
            }

            bool empty () const {
                return data == nullptr;
            }

            void realloc (uint32_t nx, uint32_t sz) {
                CHECK(sz > 0);
                CHECK(sz >= MIN_PAGE_SIZE);
                if (data && (sz > size)) {
                    ::free(data);
                    data = nullptr;
                }
                if (data == nullptr) {
                    data = reinterpret_cast<PageData *>(aligned_alloc(ALIGNMENT, sz));
                    CHECK(data);
                }
                data->next = nx;
                data->used = 0;
                size = sz;
                free = size - PageData::head_size();
            }

            void read (int fd, off64_t off, uint32_t sz) {
                if (data == nullptr) {
                    realloc(0, sz);
                }
                ssize_t r = ::pread(fd, data, sz, off);
                CHECK(r == sz);
            }

            void write (int fd, off64_t off) {
                CHECK(data);
                ssize_t r = ::pwrite(fd, data, size, off);
                CHECK(r == size);
            }

            void scan (function<char const *(char const *)> callback) {
                CHECK(data);
                char const *off = data->data;
                char const *end = off + data->used;
                while (off < end) {
                    off = callback(off);
                }
            }

            bool append (uint32_t size, function<char *(char *)> callback) {
                CHECK(data);
                if (size > free) return false;
                callback(data->data + data->used);
                data->used += size;
                free -= size;
                return true;
            }
        };

        struct Bucket {
            uint32_t head;      // 0 means empty, otherwise the head page of the bucket
            Page page;          // content of the head page
            //string buf;       // buffer of the head page
            bool dirty;
            std::shared_timed_mutex mutex;

            Bucket (uint32_t h): head(h), dirty(false) {
                // created with empty buf
            }

            ~Bucket () {
            }
        };

        int fd;
        // global synchronization objects
        std::shared_timed_mutex buzy;
        std::mutex extending;
        //
        atomic<uint32_t> next_page;
        atomic<uint32_t> next_bucket;
        // entry Bucket * is only allocated once and never freed before destruction
        // buckets has size of max_buckets, with unused entries being nullptr
        vector<Bucket *> buckets;

        uint32_t alloc_page () {
            return next_page.fetch_add(1);
        }

    public:
        // create hash table
        static void create (string const &path, Config const &conf); 

        HashTable (string const &path, bool readonly = false);

        ~HashTable ();

        void flush ();

        void extend (uint32_t buckets);

        void report (ostream &);

        // append to a bucket, can be concurrent
        void append (uint32_t bid, uint32_t size, function<char *(char *)> callback);

        // scan a bucket, can be concurrent
        void scan (uint32_t bid, function<char const *(char const *buf)> callback);
    };

    class Journal {
        static constexpr uint32_t MAX_RECORD_SIZE = (1 << 16)-1;
        struct __attribute__ ((__packed__)) Head {
            static constexpr uint64_t MAGIC = 0x59AE59AE59AE59AE;
            static constexpr uint32_t VERSION = 1;
            static constexpr uint32_t HEAD_SIZE = 512;
            static constexpr uint32_t PAD_SIZE = 512 - 32;
            uint64_t magic;
            uint32_t version;
            uint32_t serial;
            uint64_t file_size;
            uint64_t last_index;
            char pad[PAD_SIZE];
        };

        string path;            // path to journal file
        ofstream str;            // use buffer of stream
        Head head;
        std::mutex mutex;

        struct __attribute__ ((__packed__)) RecordHead {
            static constexpr uint16_t MAGIC = 0x59AF;
            uint16_t magic;
            uint16_t size;
            uint32_t serial;
            //uint16_t meta_size;
        };

        /*
        struct __attribute__ ((__packed__)) IndexHead {
            static constexpr uint16_t MAGIC = 0x59B0;
            uint16_t magic;
            uint16_t size;
            uint32_t serial;
            //uint64_t previous;
            //uint16_t meta_size;
        };
        */

    public:
        static void create (string const &path); 
        static void replay (string const &path);
        Journal (string const &path_, bool trunc = true);
        ~Journal ();
        void append (uint32_t size, char const *buf);
        void sync ();
        //void replay ();
    };

    class DB {
        Journal journal;
        HashTable hash;
    public:
    };
#if 0
    class DB {
        struct Record {
            string key;
            string meta;
            Object object;
        };
        vector<Record *> records;
        Index *index;
        mutable shared_mutex mutex;
        Matcher matcher;
        SearchRequest defaults;
        int default_K;
        float default_R;
    public:
        DB (Config const &config, bool ro) 
            : index(nullptr),
            matcher(config),
            default_K(config.get<int>("donkey.defaults.K", 1)),
            default_R(config.get<float>("donkey.defaults.R", donkey::default_R()))
        {
            if (default_K <= 0) throw ConfigError("invalid defaults.K");
            if (!isnormal(default_R)) throw ConfigError("invalid defaults.R");

#ifdef AAALGO_DONKEY_TEXT
            string algo = config.get<string>("donkey.index.algorithm", "inverted");
#else
            string algo = config.get<string>("donkey.index.algorithm", "kgraph");
#endif
            if (algo == "linear") {
                index = create_linear_index(config);
            }
            else if (algo == "lsh") {
                index = create_lsh_index(config);
            }
            else if (algo == "kgraph") {
                index = create_kgraph_index(config);
            }
#ifdef AAALGO_DONKEY_TEXT
            else if (algo == "inverted") {
                index = create_inverted_index(config);
            }
#endif
            else throw ConfigError("unknown index algorithm");
            BOOST_VERIFY(index);
        }

        ~DB () {
            clear();
            delete index;
        }

        void insert (string const &key, string const &meta, Object *object) {
            Record *rec = new Record;
            rec->key = key;
            rec->meta = meta;
            object->swap(rec->object);
            unique_lock<shared_mutex> lock(mutex);
            size_t id = records.size();
            records.push_back(rec);
            rec->object.enumerate([this, id](unsigned tag, Feature const *ft) {
                index->insert(id, tag, ft);
            });
        }

        void search (Object const &object, SearchRequest const &params, SearchResponse *response) const {
            unordered_map<unsigned, Candidate> candidates;
            {
                Timer timer(&response->filter_time);
                shared_lock<shared_mutex> lock(mutex);
                object.enumerate([this, &params, &candidates](unsigned qtag, Feature const *ft) {
                    vector<Index::Match> matches;
                    index->search(*ft, params, &matches);
                    for (auto const &m: matches) {
                        auto &c = candidates[m.object];
                        Hint hint;
                        hint.dtag = m.tag;
                        hint.qtag = qtag;
                        hint.value = m.distance;
                        c.hints.push_back(hint);
                    }
                });
            }
            {
                Timer timer(&response->rank_time);
                response->hits.clear();
                float R = params.R;
                if (!std::isnormal(R)) {
                    R = default_R;
                }

                int K = params.K;
                if (K <= 0) {
                    K = default_K;
                }

                for (auto &pair: candidates) {
                    unsigned id = pair.first;
                    Candidate &cand = pair.second;
                    cand.object = &records[id]->object;
                    string details;
                    float score = matcher.apply(object, cand, &details);
                    bool good = false;
                    if (Matcher::POLARITY >= 0) {
                        good = score >= R;
                    }
                    else {
                        good = score <= R;
                    }
                    if (good) {
                        Hit hit;
                        hit.key = records[id]->key;
                        hit.meta = records[id]->meta;
                        hit.score = score;
                        hit.details.swap(details);
                        response->hits.push_back(hit);
                    }
                }
                if (Matcher::POLARITY < 0) {
                    sort(response->hits.begin(),
                         response->hits.end(),
                         [](Hit const &h1, Hit const &h2) { return h1.score < h2.score;});
                }
                else {
                    sort(response->hits.begin(),
                         response->hits.end(),
                         [](Hit const &h1, Hit const &h2) { return h1.score > h2.score;});
                }
                if (response->hits.size() > K) {
                    response->hits.resize(K);
                }
            }
        }

        void clear () {
            unique_lock<shared_mutex> lock(mutex);
            index->clear();
            for (auto record: records) {
                delete record;
            }
            records.clear();
        }

        void reindex () {
            unique_lock<shared_mutex> lock(mutex);
            // TODO: this can be improved
            // The index building part doesn't requires read-lock only
            index->rebuild();
        }

        void snapshot_index (string const &path) {
            unique_lock<shared_mutex> lock(mutex);
            if (records.size()) {
                index->snapshot(path);
            }
        }

        void recover_index (string const &path) {
            unique_lock<shared_mutex> lock(mutex);
            if (records.size()) {
                index->recover(path);
            }
        }
    };

    struct ExtractResponse {
        double time;
        Object object;
    };

    class Service {
    public:
        virtual ~Service () = default;
        virtual void ping (PingResponse *response) = 0;
        virtual void insert (InsertRequest const &request, InsertResponse *response) = 0;
        virtual void search (SearchRequest const &request, SearchResponse *response) = 0;
        virtual void misc (MiscRequest const &request, MiscResponse *response) = 0;
        virtual void extract (ExtractRequest const &request, ExtractResponse *response) {
            throw Error("unimplemented");
        };
    };

    class Server: public Service {
        class dir_checker {
        public:
            dir_checker (string const &dir) {
                int v = ::mkdir(dir.c_str(), 0777);
                if (v && (errno != EEXIST)) {
                    LOG(error) << "Root director " << dir << " does not exist and cannot be created.";
                    BOOST_VERIFY(0);
                }
            }
        };
        bool readonly;
        bool log_object;
        string root;
        dir_checker __dir_checker;
        Journal journal;
        vector<DB *> dbs;
        Extractor xtor;

        void check_dbid (uint16_t dbid) const {
            BOOST_VERIFY(dbid < dbs.size());
        }

        void loadObject (ObjectRequest const &request, Object *object) const; 

    public:
        Server (Config const &config, bool ro = false)
            : readonly(ro),
            log_object(config.get<int>("donkey.server.log_object", 0)),
            root(config.get<string>("donkey.root")),
            __dir_checker(root),
            journal(root + "/journal", ro),
            dbs(config.get<size_t>("donkey.max_dbs", DEFAULT_MAX_DBS), nullptr),
            xtor(config)
        {
            // create empty dbs
            for (auto &db: dbs) {
                db = new DB(config, readonly);
            }
            // recover journal 
            journal.recover([this](uint16_t dbid, string const &key, string const &meta, Object *object){
                try {
                    dbs[dbid]->insert(key, meta, object);
                }
                catch (...) {
                }
            });
            // reindex all dbs
            for (unsigned i = 0; i < dbs.size(); ++i) {
                dbs[i]->recover_index(format("%s/%d.index", root, i));
            }
        }

        ~Server () { // close all dbs
            for (DB *db: dbs) {
                delete db;
            }
        }

        // embedded mode
        void ping (PingResponse *resp) {
            resp->last_start_time = 0;
            resp->first_start_time = 0;
            resp->restart_count = 0;
        }

        void extract (ExtractRequest const &request, ExtractResponse *response) {
            Timer timer1(&response->time);
            loadObject(request, &response->object);
        }

        void insert (InsertRequest const &request, InsertResponse *response) {
            if (readonly) throw PermissionError("readonly journal");
            Timer timer(&response->time);
            if (log_object) log_object_request(request, "INSERT");
            check_dbid(request.db);
            Object object;
            {
                Timer timer1(&response->load_time);
                loadObject(request, &object);
            }
            {
                Timer timer2(&response->journal_time);
                journal.append(request.db, request.key, request.meta, object);
            }
            {
                Timer timer3(&response->index_time);
                // must come after journal, as db insert could change object content
                dbs[request.db]->insert(request.key, request.meta, &object);
            }
        }

        void search (SearchRequest const &request, SearchResponse *response) {
            Timer timer(&response->time);
            if (log_object) log_object_request(request, "SEARCH");
            check_dbid(request.db);
            Object object;
            {
                Timer timer1(&response->load_time);
                loadObject(request, &object);
            }
            dbs[request.db]->search(object, request, response);
        }

        void misc (MiscRequest const &request, MiscResponse *response) {
            LOG(info) << "misc operation: " << request.method;
            if (request.method == "reindex") {
                check_dbid(request.db);
                dbs[request.db]->reindex();
            }
            else if (request.method == "clear") {
                if (readonly) throw PermissionError("readonly journal");
                check_dbid(request.db);
                dbs[request.db]->clear();
            }
            else if (request.method == "sync") {
                if (readonly) throw PermissionError("readonly journal");
                journal.sync();
                for (unsigned i = 0; i < dbs.size(); ++i) {
                    dbs[i]->snapshot_index(format("%s/%d.index", root, i));
                }
            }
            response->code = 0;
        }
    };

    // true: reload
    // false: exit
    bool run_server (Config const &, Service *);

    class NetworkAddress {
        string h;   // empty for nothing 
        int p;      // -1 for nothing
    public:
        NetworkAddress (string const &);
        string host () const {
            if (h.empty()) throw InternalError("no host");
            return h;
        }
        int port () const {
            if (p <= 0) throw InternalError("no port");
            return p;
        }
        string host (string const &def) {
            if (h.size()) return h;
            return def;
        }
        unsigned short port (unsigned short def) const {
            if (p > 0) return p;
            return def;
        }
    };

    Service *make_client (Config const &);
    Service *make_client (string const &address);

#endif


}

