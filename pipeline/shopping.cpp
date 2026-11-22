#include "input.hpp"
#include <atomic>
#include <bit>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <memory>
#include <mutex>
#include <numeric>
#include <thread>

extern "C" int attribute(size_t,const uint32_t*,const int64_t*,const int64_t*,size_t,
                         const uint32_t*,const int64_t*,const uint32_t*,uint32_t,
                         int64_t,size_t,int64_t*);
static constexpr size_t BANKS = 128;
static int workers = 1;

template<typename F> void parallel(size_t count, F job) {
    std::atomic<size_t> next{0};
    std::exception_ptr failure;
    std::mutex lock;
    std::vector<std::thread> threads;
    for (int w = 0; w < workers; ++w) {
        threads.emplace_back([&,w] {
            try {
                for (size_t i; (i = next.fetch_add(1, std::memory_order_relaxed)) < count;)
                    job(w,i);
            } catch (...) {
                std::lock_guard guard(lock);
                failure = std::current_exception();
                next.store(count);
            }
        });
    }
    for (auto& thread : threads) thread.join();
    if (failure) std::rethrow_exception(failure);
}

// Fixed-size blocks avoid capacity doubling for tens of millions of records.
template<typename T> struct Blocks {
    static constexpr size_t BLOCK = 1024;
    std::vector<std::unique_ptr<T[]>> blocks;
    size_t size = 0;
    void add(T value) {
        if (size % BLOCK == 0) blocks.emplace_back(std::make_unique_for_overwrite<T[]>(BLOCK));
        blocks.back()[size++ % BLOCK] = value;
    }
    template<typename F> void each(F f) {
        for (size_t i = 0; i < blocks.size(); ++i)
            for (size_t j = 0, n = std::min(BLOCK,size-i*BLOCK); j < n; ++j) f(blocks[i][j]);
    }
    void clear() { blocks.clear(); size = 0; }
};

struct RepeatFilter {
    // Collisions only select more records for exact deduplication.
    std::vector<uint64_t> seen, repeated;
    uint64_t mask;
    explicit RepeatFilter(size_t bytes) {
        size_t bits = std::bit_ceil(std::clamp<size_t>(bytes / 12, 64, size_t(1) << 30));
        mask = bits-1; seen.resize(bits/64); repeated.resize(bits/64);
    }
    void visit(Text key) {
        uint64_t h = hash_text(key) & mask, bit = uint64_t(1) << (h & 63);
        auto word = std::atomic_ref<uint64_t>(seen[h >> 6]);
        if (word.fetch_or(bit,std::memory_order_relaxed) & bit)
            std::atomic_ref<uint64_t>(repeated[h >> 6]).fetch_or(bit,std::memory_order_relaxed);
    }
    bool possible_duplicate(uint64_t hash) const {
        auto h = hash & mask;
        return (repeated[h >> 6] >> (h & 63)) & 1;
    }
    void scan(Chunk chunk) {
        for (auto p = chunk.first; p < chunk.end;) {
            auto nl = static_cast<const char*>(memchr(p,'\n',chunk.end-p));
            auto end = nl ? nl : chunk.end;
            auto comma = static_cast<const char*>(memchr(p,',',end-p));
            if (comma && comma != p) {
                Text id(p, comma-p);
                if (id.size() >= 2 && id.front() == '"' && id.back() == '"')
                    id = id.substr(1,id.size()-2);
                visit(id);
            }
            p = nl ? nl+1 : end;
        }
    }
};

struct Event { int64_t time; uint32_t person, purchase; };
static_assert(sizeof(Event)==16);
struct Purchase { uint32_t price, category; };
struct Click { int64_t time; Text id; uint32_t person, campaign; };
struct Candidate { Text id; Event event; int64_t ingest; uint64_t hash; };

struct Reader {
    Dictionary users;
    Calendar calendar;
    Blocks<Event> events;
    std::vector<Purchase> purchases;
    Blocks<Click> clicks;
    std::array<Blocks<Candidate>,BANKS> candidates;
    std::vector<uint32_t> canonical;
};

struct People {
    Dictionary ids;
    std::vector<uint32_t> parent;
    uint32_t root(uint32_t id) {
        while (id < parent.size() && parent[id] != id) {
            parent[id] = parent[parent[id]];
            id = parent[id];
        }
        return id;
    }
    void join(Text a, Text b) {
        if (a.empty() || b.empty()) return;
        uint32_t x = ids.intern(a), y = ids.intern(b);
        while (parent.size() < ids.names.size()) parent.push_back(parent.size());
        x = root(x); y = root(y);
        if (x == y) return;
        if (ids.names[y] < ids.names[x]) std::swap(x,y);
        parent[y] = x;
    }
};

struct Catalogs {
    Dictionary products, categories, campaigns;
    std::vector<uint32_t> product_category;
    std::vector<Text> channels;
    uint32_t unknown;
    Catalogs(Input& product_input, Input& campaign_input) {
        for (auto chunk : product_input.chunks) parse_chunk<2>(chunk,[&](auto& f){
            products.intern(f[0]); product_category.push_back(categories.intern(f[1]));
        });
        unknown = categories.intern("unknown");
        for (auto chunk : campaign_input.chunks) parse_chunk<2>(chunk,[&](auto& f){
            campaigns.intern(f[0]); channels.push_back(f[1]);
        });
    }
};

void read_events(Reader& r, int worker, Chunk chunk, const Catalogs& catalogs,
                 const RepeatFilter& repeats, int64_t start, int64_t end) {
    parse_chunk<7>(chunk,[&](auto& f) {
        if (f[0].empty()) return;
        uint64_t hash = hash_text(f[0]);
        bool check = repeats.possible_duplicate(hash);
        bool purchase = f[4] == "purchase";
        int64_t time = r.calendar.timestamp(f[2]);
        int64_t ingest = r.calendar.ingest(f[3]);
        Event event{time,NONE,NONE};
        bool valid = !f[1].empty() && !f[5].empty() && time >= start && time < end &&
                     ingest != INVALID && ingest - time <= DAY_MS &&
                     (purchase || f[4] == "cart" || f[4] == "view");
        if (valid && purchase) {
            int64_t price = unsigned_number(f[6]);
            valid = price >= 1 && price <= 10000000;
            if (valid) {
                uint32_t product = catalogs.products.lookup(f[5]);
                uint32_t cat = product == NONE ? catalogs.unknown : catalogs.product_category[product];
                event.purchase = uint32_t(r.purchases.size()*8 + worker);
                r.purchases.push_back({uint32_t(price),cat});
            }
        }
        if (valid) event.person = r.users.intern(f[1]) * 8 + worker;
        if (check) r.candidates[hash % BANKS].add({f[0],event,ingest,hash});
        else if (valid) r.events.add(event);
    });
}

void dedup(size_t bank, Reader& dest, std::vector<Reader>& readers) {
    size_t count = 0;
    for (auto& r : readers) count += r.candidates[bank].size;
    std::vector<Candidate*> table(std::bit_ceil(std::max<size_t>(2,count*2)), nullptr);
    for (auto& r : readers) {
        r.candidates[bank].each([&](Candidate& row) {
            size_t pos = (row.hash / BANKS) & (table.size()-1);
            while (table[pos] && table[pos]->id != row.id) pos = (pos+1) & (table.size()-1);
            if (!table[pos] || table[pos]->ingest > row.ingest) table[pos] = &row;
        });
    }
    for (auto row : table) if (row && row->event.person != NONE) dest.events.add(row->event);
    for (auto& r : readers) r.candidates[bank].clear();
}

struct Shards {
    std::array<Blocks<Event>,BANKS> events;
    std::array<Blocks<Click>,BANKS> clicks;
};

template<typename T>
std::pair<std::vector<T>,std::vector<size_t>>
group_by_person(std::array<Blocks<T>,BANKS> Shards::*member, std::vector<Shards>& sources,
                size_t bank, size_t person_count) {
    size_t groups = (person_count + BANKS - 1) / BANKS;
    std::vector<size_t> offsets(groups+1);
    for (auto& source : sources)
        (source.*member)[bank].each([&](const T& row){ ++offsets[row.person/BANKS+1]; });
    std::partial_sum(offsets.begin(),offsets.end(),offsets.begin());
    auto cursor = offsets;
    std::vector<T> rows(offsets.back());
    for (auto& source : sources) {
        auto& input = (source.*member)[bank];
        input.each([&](const T& row){ rows[cursor[row.person/BANKS]++] = row; });
        input.clear();
    }
    return {std::move(rows),std::move(offsets)};
}

struct UserRow {
    int64_t revenue = 0;
    uint32_t person = 0;
    int32_t day = 0;
    uint32_t sessions = 0, events = 0, category = NONE, reserved = 0;
};
static_assert(sizeof(UserRow)==32);
#include "output.hpp"
inline int64_t utc_day(int64_t ms) { return ms/DAY_MS - (ms%DAY_MS<0); }

struct Daily {
    const Dictionary& cats;
    std::vector<int64_t> amounts;
    std::vector<uint32_t> touched;
    std::vector<UserRow> output;
    UserRow row;
    explicit Daily(const Dictionary& c) : cats(c), amounts(c.names.size()) {}
    void finish() {
        if (!row.events) return;
        for (auto cat : touched) {
            auto best = row.category;
            if (best == NONE || amounts[cat] > amounts[best] ||
                (amounts[cat] == amounts[best] && cats.names[cat] < cats.names[best]))
                row.category = cat;
        }
        for (auto cat : touched) amounts[cat] = 0;
        touched.clear();
        output.push_back(row);
        row = UserRow{};
    }
    void session(uint32_t person, int64_t time) {
        int32_t day = utc_day(time);
        if (row.events && (day != row.day || person != row.person)) finish();
        row.day = day; row.person = person; ++row.sessions;
    }
    void event(Purchase purchase) {
        ++row.events;
        if (purchase.price) {
            row.revenue += purchase.price;
            if (amounts[purchase.category] == 0) touched.push_back(purchase.category);
            amounts[purchase.category] += purchase.price;
        }
    }
};

void aggregate(size_t bank, std::vector<Shards>& shards, const Dictionary& people,
               const Catalogs& catalogs, int64_t first_day, size_t days,
               std::vector<int64_t>& totals, const std::vector<Reader>& readers, const fs::path& out) {
    auto [events, eoff] = group_by_person(&Shards::events,shards,bank,people.names.size());
    auto [clicks, coff] = group_by_person(&Shards::clicks,shards,bank,people.names.size());
    Daily daily(catalogs.categories);
    std::vector<int64_t> st, revenue, ct;
    std::vector<uint32_t> sp, cp, campaign;
    for (size_t group = 0; group+1 < eoff.size(); ++group) {
        if (eoff[group] == eoff[group+1]) continue;
        uint32_t person = group*BANKS + bank;
        auto first = events.begin()+eoff[group], last = events.begin()+eoff[group+1];
        std::sort(first,last,[](const Event& a,const Event& b){return a.time < b.time;});
        int64_t previous = 0;
        for (auto i = first; i != last; ++i) {
            if (i == first || i->time - previous > 1800000) {
                st.push_back(i->time); sp.push_back(person); revenue.push_back(0);
                daily.session(person,i->time);
            }
            previous = i->time;
            Purchase purchase{};
            if (i->purchase != NONE) purchase = readers[i->purchase%8].purchases[i->purchase/8];
            revenue.back() += purchase.price;
            daily.event(purchase);
        }
        daily.finish();
        auto begin = clicks.begin()+coff[group], end = clicks.begin()+coff[group+1];
        std::sort(begin,end,[](const Click& a,const Click& b) {
            return a.time != b.time ? a.time < b.time : a.id > b.id;
        });
        for (auto i = begin; i != end; ++i) {
            if (i != begin && i->time == (i-1)->time && i->id == (i-1)->id) continue;
            cp.push_back(person); ct.push_back(i->time); campaign.push_back(i->campaign);
        }
    }
    int code = attribute(st.size(),sp.data(),st.data(),revenue.data(),ct.size(),cp.data(),
                         ct.data(),campaign.data(),catalogs.campaigns.names.size(),
                         first_day,days,totals.data());
    if (code) throw std::runtime_error("attribution failed");
    if (bank==0 || !daily.output.empty())
        write_users(out/"user_daily"/("part-"+std::to_string(bank)+".parquet"),
                    daily.output,people,catalogs.categories);
}

int main(int argc,char** argv) try {
    if (argc != 6) throw std::runtime_error("expected data, output, start_ms, end_ms, workers");
    fs::path data=argv[1], out=argv[2];
    int64_t start=std::stoll(argv[3]), end=std::stoll(argv[4]);
    workers=std::clamp(std::stoi(argv[5]),1,8);
    fs::create_directories(out/"user_daily");
    fs::create_directories(out/"campaign_daily");
    auto began=std::chrono::steady_clock::now();
    auto stage=[&](const char* name){
        std::cerr<<name<<" elapsed_seconds="<<std::chrono::duration<double>(std::chrono::steady_clock::now()-began).count()<<'\n';
    };
    Input products(data/"products.csv"), campaigns(data/"campaigns.csv"), links(data/"links");
    Input events(data/"events"), clicks(data/"clicks");
    Catalogs catalogs(products,campaigns);
    People people;
    std::vector<Reader> readers(workers);
    size_t bytes=0;
    for (auto& file:events.mappings) bytes+=file.size;
    {
        RepeatFilter repeats(bytes);
        parallel(events.chunks.size(),[&](int,size_t i){repeats.scan(events.chunks[i]);});
        repeats.seen.clear(); repeats.seen.shrink_to_fit();
        stage("indexed event IDs");
        parallel(events.chunks.size(),[&](int w,size_t i){
            read_events(readers[w],w,events.chunks[i],catalogs,repeats,start,end);
        });
    }
    parallel(BANKS,[&](int w,size_t bank){dedup(bank,readers[w],readers);});
    stage("cleaned events");
    parallel(clicks.chunks.size(),[&](int w,size_t i){
        Reader& r=readers[w];
        parse_chunk<4>(clicks.chunks[i],[&](auto& f){
            if (f[0].empty() || f[1].empty()) return;
            int64_t t=r.calendar.timestamp(f[3]);
            if (t<start-7*DAY_MS || t>=end) return;
            uint32_t c=catalogs.campaigns.lookup(f[2]);
            if (c!=NONE) r.clicks.add({t,f[0],uint32_t(r.users.intern(f[1])*8+w),c});
        });
    });
    for (auto chunk:links.chunks) parse_chunk<2>(chunk,[&](auto& f){people.join(f[0],f[1]);});
    for (auto& r:readers) {
        r.canonical.reserve(r.users.names.size());
        for (Text name:r.users.names) r.canonical.push_back(people.root(people.ids.intern(name)));
        r.users=Dictionary{};
    }
    stage("resolved users and clicks");
    std::vector<Shards> shards(workers);
    auto route=[&](auto row,auto& dest){
        row.person=readers[row.person%8].canonical[row.person/8];
        dest[row.person%BANKS].add(row);
    };
    parallel(workers,[&](int,size_t i){
        auto& r=readers[i]; auto& dest=shards[i];
        r.events.each([&](Event e){route(e,dest.events);}); r.events.clear();
        r.clicks.each([&](Click c){route(c,dest.clicks);}); r.clicks.clear();
    });
    int64_t first_day=utc_day(start);
    size_t days=utc_day(end-1)-first_day+1;
    size_t metrics=(catalogs.campaigns.names.size()+1)*days*4;
    std::vector<std::vector<int64_t>> totals(workers,std::vector<int64_t>(metrics));
    parallel(BANKS,[&](int w,size_t bank){
        aggregate(bank,shards,people.ids,catalogs,first_day,days,totals[w],readers,out);
    });
    for(int w=1;w<workers;++w)
        for(size_t k=0;k<metrics;++k) totals[0][k]+=totals[w][k];
    write_campaigns(out/"campaign_daily"/"part-0.parquet",catalogs,first_day,days,totals[0]);
    std::cout<<"pipeline_seconds="
             <<std::chrono::duration<double>(std::chrono::steady_clock::now()-began).count()<<'\n';
    return 0;
} catch(const std::exception& e) { std::cerr<<e.what()<<'\n'; return 1; }
