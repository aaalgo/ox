#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "ox.h"
#include "fnhack.h"

namespace ox {
    using std::ios;
    using boost::shared_lock;
    using boost::upgrade_lock;

    void HashTable::create (string const &path, Config const &conf) {
        Geometry geom(conf);
        CHECK(sizeof(SuperPage) <= geom.page_size);
        int fd = ::open(path.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0755);
        CHECK(fd >= 0);
        {
            string page(geom.page_size, 0);
            SuperPage sp;
            sp.magic = SuperPage::MAGIC;
            sp.version = SuperPage::VERSION;
            sp.page_size = geom.page_size;
            sp.max_pages = geom.max_pages;
            sp.max_buckets = geom.max_buckets;
            sp.next_page = geom.bucket_offset;
            sp.next_bucket = 0;
            memcpy(&page[0], &sp, sizeof(sp));
            // write twice
            for (int i = 0; i < 2; ++i) {
                ssize_t r = ::write(fd, &page[0], page.size());
                CHECK(r == page.size()) << strerror(errno);
            }
        }
        {
            string page(geom.page_size, 0);
            for (auto i = 0; i < geom.bitmap_pages; ++i) {
                ssize_t r = ::write(fd, &page[0], page.size());
                CHECK(r == page.size());
            }
            for (auto i = 0; i < geom.index_pages; ++i) {
                ssize_t r = ::write(fd, &page[0], page.size());
                CHECK(r == page.size());
            }
        }
        close(fd);
    }

    HashTable::HashTable (string const &path, bool readonly)
        : fd(::open(path.c_str(), readonly ? O_RDONLY : O_RDWR))
    {
        std::unique_lock<std::shared_timed_mutex> lock(buzy);
        CHECK(fd >= 0);
        ssize_t r = ::pread(fd, &super, sizeof(super), 0);
        CHECK(r == sizeof(super));
        CHECK(super.magic == SuperPage::MAGIC);
        CHECK(super.version == SuperPage::VERSION);
        // check super page
        geom = Geometry(Config(super));

        {   // check two super pages
            string sp1(geom.page_size, 0);
            string sp2(geom.page_size, 0);
            r = ::pread(fd, &sp1[0], sp1.size(), 0);
            CHECK(r == sp1.size());
            r = ::pread(fd, &sp2[0], sp2.size(), sp1.size());
            CHECK(r == sp2.size());
            CHECK(sp1 == sp2) << "File corruption, super pages do not agree.";
        }

        buckets.resize(geom.max_buckets);

        vector<uint32_t> index(super.next_bucket);
        if (index.size() > 0) {
            r = ::pread(fd, &index[0], index.size() * sizeof(index[0]), geom.index_offset * geom.page_size);
            CHECK(r == index.size() * sizeof(index[0]));
        }
        for (unsigned i = 0; i < index.size(); ++i) {
            buckets[i] = new Bucket(index[i]);
            CHECK(buckets[i]);
        }

        next_bucket = super.next_bucket;
        next_page = super.next_page;
    }

    HashTable::~HashTable () {
        std::unique_lock<std::shared_timed_mutex> lock(buzy);
        ::close(fd);
        for (unsigned i = 0; i < next_bucket; ++i) {
            delete buckets[i];
        }
    }

    void HashTable::extend (uint32_t n) {
        std::shared_lock<std::shared_timed_mutex> lock1(buzy);
        std::unique_lock<std::mutex> lock2(extending);
        CHECK(n > next_bucket) << "Extend must grow hash table";
        for (uint32_t i = next_bucket; i < n; ++i) {
            buckets[i] = new Bucket(0);
            CHECK(buckets[i]);
        }
        next_bucket = n;
    }

    void HashTable::report (ostream &os) {
        std::shared_lock<std::shared_timed_mutex> lock1(buzy);
        os << "page_size: " << super.page_size << endl;
        os << "max_pages: " << super.max_pages << endl;
        os << "max_buckets: " << super.max_buckets << endl;
        os << "next_bucket/sp: " << super.next_bucket << endl;
        os << "next_bucket: " << next_bucket << endl;
        os << "next_page/sp: " << super.next_page << endl;
        os << "next_page: " << next_page << endl;
        os << "file_size: " << next_page * super.page_size << endl;
    }

    void HashTable::append (uint32_t bid, uint32_t size, function<char *(char *)> callback) {
        CHECK(size + Page::PageData::head_size() < geom.page_size);
        CHECK(bid < next_bucket);
        std::shared_lock<std::shared_timed_mutex> lock1(buzy);
        // by this time
        Bucket *bucket = buckets[bid];
        std::unique_lock<std::shared_timed_mutex> lock2(bucket->mutex);
        // check size
        if (bucket->head == 0) {
            // empty bucket, first page
            bucket->page.realloc(bucket->head, geom.page_size);
            bucket->head = alloc_page();
        }
        else if (bucket->page.empty()) {
            bucket->page.read(fd, geom.page_offset(bucket->head), geom.page_size);
        }
        for (unsigned i = 0; i < 2; ++i) {
            if (bucket->page.append(size, callback)) break;
            bucket->page.write(fd, geom.page_offset(bucket->head));
            bucket->page.realloc(bucket->head, geom.page_size);
            bucket->head = alloc_page();
            // try again
        }
        bucket->dirty = true;
    }

    void HashTable::scan (uint32_t bid, function<char const *(char const *)> callback) {
        CHECK(bid < next_bucket);
        std::shared_lock<std::shared_timed_mutex> lock1(buzy);
        Bucket *bucket = buckets[bid];
        CHECK(bucket);
        // bucket is empty, shortcut
        if (bucket->head == 0) return;

        uint32_t next_page = 0;
        {
            // only need shared lock for in memory part
            std::shared_lock<std::shared_timed_mutex> lock2(bucket->mutex);
            // check read
            if (bucket->page.empty()) {
                bucket->page.read(fd, geom.page_offset(bucket->head), geom.page_size);
            }
            bucket->page.scan(callback);
            next_page = bucket->page.next_page();
        }
        // disk is append only, so disk reading do not need to be synchronized
        Page page;
        while (next_page > 0) {
            page.read(fd, geom.page_offset(next_page), geom.page_size);
            page.scan(callback);
            next_page = page.next_page();
        }
    }

    void HashTable::flush () {
        std::shared_lock<std::shared_timed_mutex> lock1(buzy);
        std::unique_lock<std::mutex> lock2(extending);
        super.next_page = next_page;
        super.next_bucket = next_bucket;
        ssize_t r = ::pwrite(fd, &super, sizeof(super), geom.page_offset(1));
        CHECK(r == sizeof(super));
        vector<uint32_t> index(next_bucket);
        for (uint32_t i = 0; i < next_bucket; ++i) {
            Bucket *bucket = buckets[i];
            index[i] = bucket->head;
            if (bucket->dirty) {
                std::shared_lock<std::shared_timed_mutex> lock2(bucket->mutex);
                bucket->page.write(fd, geom.page_offset(bucket->head));
            }
        }
        // write index pages
        r = ::pwrite(fd, (char const *)&index[0], sizeof(index[0]) * index.size(), geom.page_offset(geom.index_offset));
        CHECK(r == sizeof(index[0]) * index.size());
        // write other super-page
        super.next_page = next_page;
        super.next_bucket = next_bucket;
        r = ::pwrite(fd, &super, sizeof(super), geom.page_offset(0));
        CHECK(r == sizeof(super));
    }

    void Journal::create (string const &path) {
        Head head;
        CHECK(sizeof(head) == Head::HEAD_SIZE);
        head.magic = Head::MAGIC;
        head.version = Head::VERSION;
        head.serial = 0;
        head.file_size = sizeof(head);
        head.last_index = 0;
        std::fill(head.pad, head.pad + Head::PAD_SIZE, 0);
        ofstream os(path.c_str(), ios::binary);
        os.write(reinterpret_cast<char const *>(&head), sizeof(head));
        CHECK(os);
    }

    Journal::Journal (string const &path_, bool trunc)
              : path(path_)
    {
        uint64_t sz = 0;
        {   // read journal
            std::ifstream is(path.c_str(), ios::binary);
            CHECK(is);
            is.seekg(0, ios::end);
            sz = is.tellg();
            CHECK(sz >= Head::HEAD_SIZE);
            is.seekg(0, ios::beg);
            is.read(reinterpret_cast<char *>(&head), sizeof(head));
            CHECK(is);
        }
        CHECK(head.magic == Head::MAGIC);
        CHECK(head.version == Head::VERSION);
        CHECK(sz >= head.file_size);
        if (sz > head.file_size) {
            CHECK(trunc) << "journal not clean, open with trunc = true";
            LOG(WARNING) << "truncating journal to " << head.file_size << " bytes.";
            int fd = ::open(path.c_str(), O_WRONLY);
            CHECK(fd >= 0);
            int r = ::ftruncate(fd, head.file_size);
            CHECK(r == 0);
            close(fd);
        }
        str.open(path.c_str(), ios::binary);
        str.seekp(0, ios::end);
    }

    Journal::~Journal () {
        if (str.is_open()) {
            sync();
        }
    }

    void Journal::append (uint32_t size, char const *buf) {
        CHECK(size <= MAX_RECORD_SIZE);
        std::lock_guard<std::mutex> lock(mutex); 
        CHECK(str.is_open());
        RecordHead rhead;
        rhead.magic = RecordHead::MAGIC;
        rhead.size = size;
        rhead.serial = head.serial++;
        str.write(reinterpret_cast<char const *>(&rhead), sizeof(rhead));
        str.write(buf, size);
        head.file_size += sizeof(rhead) + size;
    }

    void Journal::sync () {
        CHECK(str.is_open());
        std::lock_guard<std::mutex> lock(mutex); 
        str.seekp(0, ios::beg);
        str.write(reinterpret_cast<char const *>(&head), sizeof(head));
        str.seekp(0, ios::end);
        str.flush();
        ::fsync(fileno_hack(str));
    }
}

