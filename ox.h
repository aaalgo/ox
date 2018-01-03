#include <cstring>
#include <string>
#include <atomic>
#include <mutex>
#include <iostream>
#include <vector>
#include <memory>
#include <boost/thread/shared_mutex.hpp>
#include <glog/logging.h>
#include <shared_mutex>
#include <functional>


namespace ox {

    using std::atomic;
    using std::endl;
    using std::ostream;
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
            uint32_t max_pages;
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
            uint32_t head;  // 0 means empty, otherwise the head page of the bucket
            Page page;      // content of the head page
            //string buf;     // buffer of the head page
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
}

