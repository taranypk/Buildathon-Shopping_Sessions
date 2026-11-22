#pragma once
#include <algorithm>
#include <array>
#include <charconv>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace fs = std::filesystem;
using Text = std::string_view;
constexpr int64_t DAY_MS = 86400000;
constexpr int64_t INVALID = std::numeric_limits<int64_t>::min();
constexpr uint32_t NONE = UINT32_MAX;

struct Mapping {
    const char* data = nullptr;
    size_t size = 0;
    explicit Mapping(const fs::path& file) {
        int fd = open(file.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("cannot open " + file.string());
        struct stat st{};
        if (fstat(fd, &st) != 0) { close(fd); throw std::runtime_error("cannot stat input"); }
        size = st.st_size;
        if (size) {
            void* ptr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
            if (ptr == MAP_FAILED) { close(fd); throw std::runtime_error("cannot map input"); }
            data = static_cast<const char*>(ptr);
        }
        close(fd);
    }
    Mapping(Mapping&& from) noexcept : data(std::exchange(from.data, nullptr)), size(from.size) {}
    ~Mapping() { if (data) munmap(const_cast<char*>(data), size); }
};

struct Chunk { const char* first; const char* end; };

struct Input {
    std::vector<Mapping> mappings;
    std::vector<Chunk> chunks;
    explicit Input(const fs::path& path) {
        if (fs::is_directory(path)) {
            for (auto& entry : fs::directory_iterator(path))
                if (entry.is_regular_file() && entry.path().extension() == ".csv")
                    mappings.emplace_back(entry.path());
        } else {
            mappings.emplace_back(path);
        }
        for (auto& file : mappings) {
            if (!file.size) continue;
            auto header = static_cast<const char*>(memchr(file.data, '\n', file.size));
            if (!header) continue;
            const char* at = header + 1;
            const char* end = file.data + file.size;
            while (at < end) {
                constexpr ptrdiff_t chunk_size = 1 << 20;
                const char* stop = end;
                if (end - at > chunk_size) {
                    auto nl = static_cast<const char*>(memchr(at + chunk_size, '\n', end - at - chunk_size));
                    if (nl) stop = nl + 1;
                }
                chunks.push_back({at, stop});
                at = stop;
            }
        }
    }
};

template<size_t N, typename F>
void parse_chunk(Chunk chunk, F&& handle) {
    const char* begin = chunk.first;
    while (begin < chunk.end) {
        auto nl = static_cast<const char*>(memchr(begin, '\n', chunk.end - begin));
        const char* end = nl ? nl : chunk.end;
        std::array<Text,N> fields;
        const char* p = begin;
        bool valid = true;
        for (size_t i = 0; i < N; ++i) {
            auto comma = static_cast<const char*>(memchr(p, ',', end - p));
            if ((comma != nullptr) != (i + 1 < N)) { valid = false; break; }
            const char* stop = comma ? comma : end;
            const char* s = p;
            size_t n = stop - s;
            if (n && *s == '"') {
                if (n < 2 || s[n - 1] != '"') { valid = false; break; }
                ++s; n -= 2;
            }
            if (memchr(s, '"', n)) { valid = false; break; }
            fields[i] = {s,n};
            if (comma) p = comma + 1;
        }
        if (valid) handle(fields);
        begin = nl ? nl + 1 : end;
    }
}

inline uint64_t hash_text(Text s) { return std::hash<Text>{}(s); }

// The input stores own the bytes. Hash slots only index strings, and every
// match is checked against the original UTF-8 bytes.
struct Dictionary {
    std::vector<Text> names;
    std::vector<uint32_t> slots = std::vector<uint32_t>(16);
    void grow() {
        slots.assign(slots.size() * 2, 0);
        for (uint32_t i = 0; i < names.size(); ++i) {
            size_t pos = hash_text(names[i]) & (slots.size() - 1);
            while (slots[pos]) pos = (pos + 1) & (slots.size() - 1);
            slots[pos] = i + 1;
        }
    }
    uint32_t intern(Text s) {
        if ((names.size() + 1) * 10 > slots.size() * 7) grow();
        size_t pos = hash_text(s) & (slots.size() - 1);
        while (slots[pos]) {
            uint32_t i = slots[pos] - 1;
            if (names[i] == s) return i;
            pos = (pos + 1) & (slots.size() - 1);
        }
        uint32_t id = names.size();
        if (id == NONE) throw std::runtime_error("too many distinct IDs");
        names.push_back(s); slots[pos] = id + 1;
        return id;
    }
    uint32_t lookup(Text s) const {
        size_t pos = hash_text(s) & (slots.size() - 1);
        while (slots[pos]) {
            uint32_t i = slots[pos] - 1;
            if (names[i] == s) return i;
            pos = (pos + 1) & (slots.size() - 1);
        }
        return NONE;
    }
};

inline int digits2(const char* p) {
    unsigned a = unsigned(p[0] - '0'), b = unsigned(p[1] - '0');
    return a < 10 && b < 10 ? int(a * 10 + b) : -1;
}

inline int64_t unsigned_number(Text s) {
    if (s.empty() || s.front() < '0' || s.front() > '9') return INVALID;
    int64_t n;
    auto result = std::from_chars(s.data(), s.data() + s.size(), n);
    return result.ec == std::errc{} && result.ptr == s.data() + s.size() ? n : INVALID;
}

struct Calendar {
    struct Entry { int key = -1; int64_t midnight = 0; };
    std::array<Entry,64> cache{};
    int64_t midnight(int y, int m, int d) {
        int key = (y * 16 + m) * 32 + d;
        auto& entry = cache[(key ^ (key >> 5)) & 63];
        if (entry.key == key) return entry.midnight;
        static constexpr int lengths[] = {31,28,31,30,31,30,31,31,30,31,30,31};
        if (m < 1 || m > 12 || d < 1) return INVALID;
        bool leap = (y % 4 == 0) && (y % 100 != 0 || y % 400 == 0);
        if (d > lengths[m-1] + (m == 2 && leap)) return INVALID;
        // Proleptic Gregorian days relative to 1970-01-01.
        int year = y - (m <= 2), era = (year >= 0 ? year : year - 399) / 400;
        int yoe = year - era * 400;
        int doy = (153 * (m > 2 ? m - 3 : m + 9) + 2) / 5 + d - 1;
        int days = era * 146097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719468;
        entry = {key, int64_t(days) * DAY_MS};
        return entry.midnight;
    }
    int64_t timestamp(Text s) {
        if ((s.size() != 20 && s.size() != 25) || s[4] != '-') return unsigned_number(s);
        const char* p = s.data();
        if (p[7] != '-' || p[10] != 'T' || p[13] != ':' || p[16] != ':') return INVALID;
        int century = digits2(p), yr = digits2(p+2), month = digits2(p+5), day = digits2(p+8);
        int hour = digits2(p+11), minute = digits2(p+14), second = digits2(p+17);
        if (century < 0 || yr < 0 || month < 1 || month > 12 || day < 1 || day > 31 ||
            unsigned(hour) > 23 || unsigned(minute) > 59 || unsigned(second) > 59) return INVALID;
        int64_t base = midnight(century*100 + yr, month, day);
        if (base == INVALID) return INVALID;
        int zone = 0;
        if (s.size() == 20) {
            if (p[19] != 'Z') return INVALID;
        } else {
            if ((p[19] != '+' && p[19] != '-') || p[22] != ':') return INVALID;
            int h = digits2(p+20), m = digits2(p+23);
            if (unsigned(h) > 23 || unsigned(m) > 59) return INVALID;
            zone = (p[19] == '+' ? 1 : -1) * (h*60 + m);
        }
        return base + ((hour*60 + minute - zone)*60 + second)*1000;
    }
};
