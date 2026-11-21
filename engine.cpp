#include "output.hpp"
#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

using namespace std;
namespace fs = std::filesystem;
using S = string_view;
using I = int64_t;
constexpr I DAY = 86400000, WEEK = 7 * DAY, INVALID = numeric_limits<I>::min();
constexpr int B = 256;
int workers;
template <class F> void parallel(size_t n, F fn) {
    atomic<size_t> next{0};
    vector<thread> pool;
    for (size_t i = 0; i < min<size_t>(workers, n); ++i)
        pool.emplace_back([&] {
            for (;;) {
                auto j = next++;
                if (j >= n)
                    break;
                fn(j);
            }
        });
    for (auto &t : pool)
        t.join();
}
struct Mapping {
    char *p = nullptr;
    size_t n = 0;
    explicit Mapping(const fs::path &path) {
        int fd = open(path.c_str(), O_RDONLY);
        struct stat st{};
        if (fd < 0 || fstat(fd, &st))
            throw runtime_error("cannot open " + path.string());
        n = st.st_size;
        if (n)
            p = static_cast<char *>(mmap(nullptr, n, PROT_READ, MAP_PRIVATE, fd, 0));
        close(fd);
        if (p == MAP_FAILED)
            throw runtime_error("mmap failed");
    }
    ~Mapping() {
        if (n)
            munmap(p, n);
    }
};
struct Chunk {
    const char *p, *end;
};
struct Inputs {
    vector<unique_ptr<Mapping>> files;
    vector<Chunk> chunks;
    void add(fs::path path) {
        auto m = make_unique<Mapping>(path);
        if (!m->n)
            return;
        const char *end = m->p + m->n;
        auto newline = [&](const char *p) {
            auto q = static_cast<const char *>(memchr(p, '\n', end - p));
            return q ? q + 1 : end;
        };
        auto p = newline(m->p);
        while (p < end) {
            auto q = end - p > 2 * 1024 * 1024 ? newline(p + 2 * 1024 * 1024) : end;
            chunks.push_back({p, q});
            p = q;
        }
        files.push_back(move(m));
    }
    void folder(fs::path path) {
        for (auto &f : fs::directory_iterator(path))
            if (f.path().extension() == ".csv")
                add(f.path());
    }
};
template <class F> void lines(Chunk c, F fn) {
    for (auto p = c.p; p < c.end;) {
        auto e = static_cast<const char *>(memchr(p, '\n', c.end - p));
        if (!e)
            e = c.end;
        fn(S(p, e - p));
        if (e == c.end)
            break;
        p = e + 1;
    }
}
bool unquote(S &s) {
    if (s.size() >= 2 && s.front() == '"' && s.back() == '"') {
        s.remove_prefix(1);
        s.remove_suffix(1);
    }
    return s.find('"') == S::npos;
}
template <size_t N> bool fields(S s, array<S, N> &a) {
    for (size_t i = 0; i < N; i++) {
        size_t pos = s.find(',');
        if ((pos == S::npos) != (i + 1 == N))
            return false;
        a[i] = s.substr(0, pos);
        if (!unquote(a[i]))
            return false;
        if (pos != S::npos)
            s.remove_prefix(pos + 1);
    }
    return !a[0].empty();
}
int digits(S s, int pos, int n) {
    int v = 0;
    for (int i = pos; i < pos + n; i++) {
        unsigned d = unsigned(s[i] - '0');
        if (d > 9)
            return -10000;
        v = v * 10 + d;
    }
    return v;
}
I number(S s) {
    if (s.empty())
        return INVALID;
    I v;
    auto r = from_chars(s.data(), s.data() + s.size(), v);
    return s.front() >= '0' && s.front() <= '9' && r.ec == errc() && r.ptr == s.data() + s.size()
               ? v
               : INVALID;
}
I timestamp(S s) {
    if ((s.size() != 20 && s.size() != 25) || s[4] != '-')
        return number(s);
    if (s[7] != '-' || s[10] != 'T' || s[13] != ':' || s[16] != ':')
        return INVALID;
    int y = digits(s, 0, 4), m = digits(s, 5, 2), d = digits(s, 8, 2);
    int h = digits(s, 11, 2), mi = digits(s, 14, 2), sec = digits(s, 17, 2);
    static const int months[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (y < 1 || m < 1 || m > 12 || d < 1 ||
        d > months[m - 1] + (m == 2 && y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) || h < 0 ||
        h > 23 || mi < 0 || mi > 59 || sec < 0 || sec > 59)
        return INVALID;
    int offset = 0;
    if (s.size() == 20) {
        if (s[19] != 'Z')
            return INVALID;
    } else {
        int oh = digits(s, 20, 2), om = digits(s, 23, 2);
        if ((s[19] != '+' && s[19] != '-') || s[22] != ':' || oh < 0 || oh > 23 || om < 0 ||
            om > 59)
            return INVALID;
        offset = (s[19] == '+' ? 1 : -1) * (oh * 60 + om);
    }
    y -= m <= 2;
    I days = 365LL * y + y / 4 - y / 100 + y / 400 + (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d -
             1 - 719468;
    return days * DAY + ((h * 60 + mi - offset) * 60 + sec) * 1000LL;
}
using Dict = unordered_map<S, int>;
struct People {
    Dict ids;
    vector<S> names;
    vector<int> parent, small, sz;
    int id(S s) {
        auto [it, added] = ids.emplace(s, parent.size());
        if (added) {
            int k = it->second;
            parent.push_back(k);
            small.push_back(k);
            sz.push_back(1);
            names.push_back(s);
        }
        return it->second;
    }
    int root(int i) {
        while (i != parent[i]) {
            parent[i] = parent[parent[i]];
            i = parent[i];
        }
        return i;
    }
    void link(S a, S b) {
        int x = root(id(a)), y = root(id(b));
        if (x == y)
            return;
        if (sz[x] < sz[y])
            swap(x, y);
        parent[y] = x;
        sz[x] += sz[y];
        if (names[small[y]] < names[small[x]])
            small[x] = small[y];
    }
    void finish() {
        for (auto &kv : ids)
            kv.second = small[root(kv.second)];
    }
    S person(S s) const {
        auto it = ids.find(s);
        return it == ids.end() ? s : names[it->second];
    }
};
// A short ID travels in the record itself. The general case retains a
// view into the mapped input. Both representations compare the exact bytes.
class Key {
    uint64_t first = 0, second = 0;
    static constexpr uint64_t indirect = uint64_t(1) << 63;

  public:
    Key() = default;
    Key(S value) {
        if (value.size() <= 15) {
            std::memcpy(&first, value.data(), std::min<size_t>(8, value.size()));
            if (value.size() > 8)
                std::memcpy(&second, value.data() + 8, value.size() - 8);
            second |= uint64_t(value.size()) << 56;
        } else {
            first = reinterpret_cast<uintptr_t>(value.data());
            second = indirect | value.size();
        }
    }
    bool empty() const { return !second; }
    S view() const {
        if (second & indirect)
            return S(reinterpret_cast<const char *>(first), second ^ indirect);
        return S(reinterpret_cast<const char *>(this), second >> 56);
    }
    bool operator==(const Key &b) const {
        if (second != b.second)
            return false;
        return first == b.first || ((second & indirect) && view() == b.view());
    }
    bool operator>(const Key &b) const { return view() > b.view(); }
    size_t hash_value() const {
        if (second & indirect)
            return hash<S>{}(view());
        uint64_t h = (first ^ (second * 0x9e3779b97f4a7c15ULL)) * 0xbf58476d1ce4e5b9ULL;
        h ^= h >> 31;
        h *= 0x94d049bb133111ebULL;
        return h ^ (h >> 31);
    }
};
static_assert(sizeof(Key) == 16);
struct KeyHash {
    size_t operator()(const Key &k) const { return k.hash_value(); }
};
struct Event {
    Key person;
    I ts;
    int price, cat;
};
struct Click {
    Key person, id;
    I ts;
    int campaign;
};
template <class T> using Bins = array<vector<T>, B>;
int bin(const Key &s) { return s.hash_value() & (B - 1); }
template <class T> vector<T> gather(vector<Bins<T>> &parts, int b) {
    size_t n = 0;
    for (auto &part : parts)
        n += part[b].size();
    vector<T> out;
    out.reserve(n);
    for (auto &part : parts) {
        auto &v = part[b];
        out.insert(out.end(), v.begin(), v.end());
        vector<T>().swap(v);
    }
    return out;
}
struct Stats {
    I sessions = 0, purchasing = 0, revenue = 0, assists = 0;
};
using Totals = unordered_map<I, Stats>;
I key(int campaign, I day) { return I(campaign) * 4294967296LL + uint32_t(day); }
void summarize(vector<Event> &ev, vector<Click> &cl, const vector<S> &cats, UserOutput &out,
               Totals &totals) {
    // Collect each person's rows once. Sorting each timeline on integer times
    // avoids comparing UTF-8 user IDs throughout a global sort.
    struct PurchaseEvent {
        I ts;
        int price, cat;
    };
    struct Touch {
        I ts;
        Key id;
        int campaign;
    };
    struct Timeline {
        Key person;
        vector<PurchaseEvent> events;
        vector<Touch> clicks;
    };
    unordered_map<Key, int, KeyHash> index;
    vector<Timeline> timelines;
    for (auto &e : ev) {
        auto [it, added] = index.try_emplace(e.person, timelines.size());
        if (added)
            timelines.push_back({e.person, {}, {}});
        timelines[it->second].events.push_back({e.ts, e.price, e.cat});
    }
    vector<Event>().swap(ev);
    for (auto &c : cl) {
        auto it = index.find(c.person);
        if (it != index.end())
            timelines[it->second].clicks.push_back({c.ts, c.id, c.campaign});
    }
    vector<Click>().swap(cl);
    vector<I> amounts(cats.size());
    vector<int> used;
    unordered_map<int, int> active;
    for (auto &timeline : timelines) {
        S person = timeline.person.view();
        auto &ev = timeline.events;
        auto &cl = timeline.clicks;
        sort(ev.begin(), ev.end(), [](const auto &a, const auto &b) { return a.ts < b.ts; });
        sort(cl.begin(), cl.end(),
             [](const auto &a, const auto &b) { return a.ts != b.ts ? a.ts < b.ts : a.id > b.id; });
        size_t i = 0, left = 0, right = 0;
        active.clear();
        while (i < ev.size()) {
            I day = ev[i].ts / DAY - (ev[i].ts % DAY < 0), ns = 0, n = 0, rev = 0;
            do {
                I start = ev[i].ts, prev, price = 0;
                while (right < cl.size() && cl[right].ts <= start)
                    ++active[cl[right++].campaign];
                while (left < right && cl[left].ts < start - WEEK) {
                    int c = cl[left++].campaign;
                    if (--active[c] == 0)
                        active.erase(c);
                }
                int credit = right > left ? cl[right - 1].campaign : 0;
                for (auto [c, count] : active)
                    ++totals[key(c, day)].assists;
                do {
                    auto &e = ev[i++];
                    prev = e.ts;
                    ++n;
                    if (e.price) {
                        if (!amounts[e.cat])
                            used.push_back(e.cat);
                        amounts[e.cat] += e.price;
                        price += e.price;
                    }
                } while (i < ev.size() && ev[i].ts - prev <= 1800000);
                auto &t = totals[key(credit, day)];
                ++t.sessions;
                t.revenue += price;
                t.purchasing += price != 0;
                ++ns;
                rev += price;
            } while (i < ev.size() && ev[i].ts < (day + 1) * DAY);
            out.user.add(person);
            out.date.add(day);
            out.sessions.add(ns);
            out.events.add(n);
            out.revenue.add(rev);
            if (!used.empty()) {
                int best = used[0];
                for (int c : used)
                    if (amounts[c] > amounts[best] ||
                        (amounts[c] == amounts[best] && cats[c] < cats[best]))
                        best = c;
                out.category.add(cats[best]);
                for (int c : used)
                    amounts[c] = 0;
                used.clear();
            } else
                out.category.null();
        }
    }
}

int main() try {
    auto previous = std::chrono::steady_clock::now();
    auto mark = [&](const char *phase) {
        if (!getenv("PROFILE"))
            return;
        auto now = std::chrono::steady_clock::now();
        cerr << phase << " " << std::chrono::duration<double>(now - previous).count() << " s\n";
        previous = now;
    };
    auto env = [](const char *key, const char *fallback) {
        auto val = getenv(key);
        return val ? val : fallback;
    };
    fs::path data = env("DATA_DIR", "/data"), output = env("OUTPUT_DIR", "/output");
    ifstream manifest(data / "manifest.json");
    string json((istreambuf_iterator<char>(manifest)), {});
    auto window = [&](string key) {
        size_t i = json.find('"' + key + '"');
        if (i == string::npos)
            return INVALID;
        i = json.find(':', i + key.size() + 2);
        if (i == string::npos)
            return INVALID;
        i = json.find('"', i + 1);
        if (i == string::npos)
            return INVALID;
        size_t j = json.find('"', i + 1);
        if (j == string::npos)
            return INVALID;
        return timestamp(S(json).substr(i + 1, j - i - 1));
    };
    I start = window("window_start"), end = window("window_end");
    workers = clamp(atoi(env("NCPU", "8")), 1, 64);
    if (start == INVALID || end <= start)
        throw runtime_error("invalid window");
    Inputs links, lookup, events, clicks;
    links.folder(data / "links");
    People people;
    for (auto c : links.chunks)
        lines(c, [&](S line) {
            array<S, 2> a;
            if (fields(line, a) && !a[1].empty())
                people.link(a[0], a[1]);
        });
    people.finish();
    vector<S> cats = {"unknown"}, campaigns = {"organic"}, channels = {"organic"};
    Dict products, category_ids = {{"unknown", 0}}, campaign_ids;
    auto load_dim = [&](const char *name, auto fn) {
        size_t begin = lookup.chunks.size();
        lookup.add(data / name);
        for (size_t i = begin; i < lookup.chunks.size(); i++)
            lines(lookup.chunks[i], [&](S line) {
                array<S, 2> a;
                if (fields(line, a))
                    fn(a[0], a[1]);
            });
    };
    load_dim("products.csv", [&](S p, S c) {
        auto [it, fresh] = category_ids.emplace(c, cats.size());
        if (fresh)
            cats.push_back(c);
        products.emplace(p, it->second);
    });
    load_dim("campaigns.csv", [&](S c, S ch) {
        campaign_ids.emplace(c, campaigns.size());
        campaigns.push_back(c);
        channels.push_back(ch);
    });
    events.folder(data / "events");
    clicks.folder(data / "clicks");
    // A repeated ID always sets both repeat bits. Hash collisions merely send
    // additional IDs to exact deduplication; the hash never decides a winner.
    mark("dimensions");
    vector<Bins<uint64_t>> hashes(events.chunks.size());
    parallel(hashes.size(), [&](size_t i) {
        lines(events.chunks[i], [&](S row) {
            auto pos = row.find(',');
            if (pos == S::npos)
                return;
            S id = row.substr(0, pos);
            if (!unquote(id) || id.empty())
                return;
            uint64_t h = hash<S>{}(id);
            hashes[i][h & (B - 1)].push_back(h);
        });
    });
    struct Filter {
        vector<uint64_t> bits;
        size_t word(uint64_t h) const { return (h >> 8) & (bits.size() - 1); }
        static uint64_t mask(uint64_t h) {
            return (1ULL << (h >> 58)) | (1ULL << ((h >> 49) & 63));
        }
    };
    array<Filter, B> duplicates;
    parallel(B, [&](size_t b) {
        auto keys = gather(hashes, b);
        size_t words = 1;
        while (words < keys.size() / 4)
            words *= 2;
        auto &f = duplicates[b];
        f.bits.resize(words);
        vector<uint64_t> first(words);
        for (uint64_t h : keys) {
            auto w = f.word(h), m = Filter::mask(h);
            if ((first[w] & m) == m)
                f.bits[w] |= m;
            first[w] |= m;
        }
    });
    mark("duplicate filter");
    struct Candidate {
        Key id;
        I ingest;
        Event event;
    };
    vector<Bins<Candidate>> possible(events.chunks.size());
    vector<Bins<Event>> valid(events.chunks.size() + B);
    parallel(events.chunks.size(), [&](size_t i) {
        lines(events.chunks[i], [&](S row) {
            array<S, 7> a;
            if (!fields(row, a))
                return;
            auto h = hash<S>{}(a[0]);
            auto b = h & (B - 1);
            auto &f = duplicates[b];
            auto mask = Filter::mask(h);
            bool repeat = (f.bits[f.word(h)] & mask) == mask;
            I ingest = timestamp(a[3]), ts = timestamp(a[2]);
            Event e{};
            if (!a[1].empty() && ts >= start && ts < end && ingest <= ts + DAY && !a[5].empty()) {
                bool ok = a[4] == "view" || a[4] == "cart";
                if (a[4] == "purchase") {
                    I price = number(a[6]);
                    if (price >= 1 && price <= 10000000) {
                        ok = true;
                        e.price = price;
                        auto p = products.find(a[5]);
                        if (p != products.end())
                            e.cat = p->second;
                    }
                }
                if (ok) {
                    e.ts = ts;
                    e.person = people.person(a[1]);
                }
            }
            if (repeat)
                possible[i][b].push_back({a[0], ingest, e});
            else if (!e.person.empty())
                valid[i][bin(e.person)].push_back(e);
        });
    });
    mark("parse events");
    parallel(B, [&](size_t b) {
        auto rows = gather(possible, b);
        unordered_map<Key, size_t, KeyHash> winners;
        winners.reserve(rows.size());
        for (size_t i = 0; i < rows.size(); i++) {
            auto [it, fresh] = winners.try_emplace(rows[i].id, i);
            if (!fresh && rows[i].ingest < rows[it->second].ingest)
                it->second = i;
        }
        auto &bins = valid[events.chunks.size() + b];
        for (auto &kv : winners) {
            auto &e = rows[kv.second].event;
            if (!e.person.empty())
                bins[bin(e.person)].push_back(e);
        }
    });
    mark("deduplicate");
    // Identical click copies leave the winning ID and set of active campaigns invariant.
    vector<Bins<Click>> ads(clicks.chunks.size());
    parallel(ads.size(), [&](size_t i) {
        lines(clicks.chunks[i], [&](S row) {
            array<S, 4> a;
            if (!fields(row, a) || a[1].empty())
                return;
            auto c = campaign_ids.find(a[2]);
            if (c == campaign_ids.end())
                return;
            I ts = timestamp(a[3]);
            if (ts < start - WEEK || ts >= end)
                return;
            Key p = people.person(a[1]);
            ads[i][bin(p)].push_back({p, a[0], ts, c->second});
        });
    });
    mark("parse clicks");
    vector<Totals> totals(B);
    fs::create_directories(output / "user_daily");
    fs::create_directories(output / "campaign_daily");
    parallel(B, [&](size_t b) {
        auto ev = gather(valid, b);
        auto cl = gather(ads, b);
        if (ev.empty() && b != 0)
            return;
        UserOutput out;
        summarize(ev, cl, cats, out, totals[b]);
        save(out, output / "user_daily" / (to_string(b) + ".parquet"));
    });
    mark("sessionize and write users");
    Totals sum;
    for (auto &part : totals)
        for (auto &kv : part) {
            auto &dst = sum[kv.first];
            auto &v = kv.second;
            dst.sessions += v.sessions;
            dst.purchasing += v.purchasing;
            dst.revenue += v.revenue;
            dst.assists += v.assists;
        }
    CampaignOutput out;
    for (auto &kv : sum) {
        int c = kv.first >> 32, day = int32_t(kv.first);
        auto &t = kv.second;
        out.campaign.add(campaigns[c]);
        out.channel.add(channels[c]);
        out.date.add(day);
        out.sessions.add(t.sessions);
        out.purchasing.add(t.purchasing);
        out.revenue.add(t.revenue);
        out.assists.add(t.assists);
    }
    save(out, output / "campaign_daily" / "part.parquet");
    return 0;
} catch (const exception &e) {
    cerr << e.what() << '\n';
    return 1;
}
