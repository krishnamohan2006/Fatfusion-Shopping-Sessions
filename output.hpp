#pragma once
#include <array>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace pq {
// Flat Parquet V1 pages with PLAIN data and RLE definition levels. All scalar
// payloads are little endian on the target linux/amd64 platform.
inline void u32(std::string &dst, uint32_t n) {
    dst.append(reinterpret_cast<const char *>(&n), sizeof(n));
}
inline void varint(std::string &dst, uint64_t n) {
    while (n >= 128) {
        dst.push_back(static_cast<char>((n & 127) | 128));
        n >>= 7;
    }
    dst.push_back(static_cast<char>(n));
}
enum Wire { I32 = 5, I64 = 6, BINARY = 8, LIST = 9, STRUCT = 12 };
struct Compact {
    std::string bytes;
    int previous = 0;
    void field(int id, Wire type) {
        bytes.push_back(static_cast<char>(((id - previous) << 4) | type));
        previous = id;
    }
    void integer(int64_t n) { varint(bytes, (uint64_t(n) << 1) ^ uint64_t(-(n < 0))); }
    void number(int id, int64_t n, Wire type = I32) {
        field(id, type);
        integer(n);
    }
    void string(std::string_view text) {
        varint(bytes, text.size());
        bytes.append(text);
    }
    void text(int id, std::string_view value) {
        field(id, BINARY);
        string(value);
    }
    void list(int id, Wire type, size_t size) {
        field(id, LIST);
        bytes.push_back(static_cast<char>((std::min<size_t>(size, 15) << 4) | type));
        if (size >= 15)
            varint(bytes, size);
    }
    void element(const Compact &obj) {
        bytes += obj.bytes;
        bytes.push_back(0);
    }
    void object(int id, const Compact &value) {
        field(id, STRUCT);
        element(value);
    }
    void end() { bytes.push_back(0); }
};
enum Physical { INT32 = 1, INT64 = 2, BYTE_ARRAY = 6 };
struct Column {
    const char *name;
    Physical type;
    bool optional = false;
    std::string payload;
    std::vector<uint8_t> validity;
    uint32_t rows = 0;
    int64_t offset = 0, size = 0;
    Column(const char *name, Physical type, bool optional = false)
        : name(name), type(type), optional(optional) {}
    void add(std::string_view s) {
        u32(payload, static_cast<uint32_t>(s.size()));
        payload.append(s);
        ++rows;
        if (optional)
            validity.push_back(1);
    }
    void null() {
        ++rows;
        validity.push_back(0);
    }
    void add(int64_t n) {
        if (type == INT32)
            u32(payload, static_cast<uint32_t>(n));
        else
            payload.append(reinterpret_cast<const char *>(&n), sizeof(n));
        ++rows;
    }
    Compact schema() const {
        Compact c;
        c.number(1, type);
        c.number(3, optional ? 1 : 0);
        c.text(4, name);
        if (type == BYTE_ARRAY)
            c.number(6, 0); // UTF8
        if (type == INT32)
            c.number(6, 6); // DATE (the only 32-bit column)
        return c;
    }
    void page(std::ofstream &file) {
        std::string levels;
        if (optional) {
            // Bit-packed groups of eight one-bit definition levels, padded
            // at the end. V1 pages prefix the level stream with its byte size.
            std::string stream;
            size_t groups = (validity.size() + 7) / 8;
            varint(stream, (groups << 1) | 1);
            size_t start = stream.size();
            stream.resize(start + groups, 0);
            for (size_t i = 0; i < validity.size(); ++i)
                if (validity[i])
                    stream[start + i / 8] |= static_cast<char>(1U << (i % 8));
            u32(levels, stream.size());
            levels += stream;
        }
        size_t body = payload.size() + levels.size();
        if (body > std::numeric_limits<int32_t>::max())
            throw std::runtime_error("Parquet page too large");
        Compact data;
        data.number(1, rows);
        data.number(2, 0);
        data.number(3, 3);
        data.number(4, 3);
        Compact header;
        header.number(1, 0);
        header.number(2, body);
        header.number(3, body);
        header.object(5, data);
        header.end();
        offset = file.tellp();
        size = header.bytes.size() + body;
        file.write(header.bytes.data(), header.bytes.size());
        file.write(levels.data(), levels.size());
        file.write(payload.data(), payload.size());
    }
    Compact chunk() const {
        Compact meta;
        meta.number(1, type);
        meta.list(2, I32, optional ? 2 : 1);
        meta.integer(0);
        if (optional)
            meta.integer(3);
        meta.list(3, BINARY, 1);
        meta.string(name);
        meta.number(4, 0);
        meta.number(5, rows, I64);
        meta.number(6, size, I64);
        meta.number(7, size, I64);
        meta.number(9, offset, I64);
        Compact chunk;
        chunk.number(2, 0, I64);
        chunk.object(3, meta);
        return chunk;
    }
};
inline void write(const std::filesystem::path &path, std::initializer_list<Column *> columns) {
    std::ofstream file;
    file.exceptions(std::ios::failbit | std::ios::badbit);
    file.open(path, std::ios::binary);
    file.write("PAR1", 4);
    uint32_t rows = (*columns.begin())->rows;
    int64_t size = 0;
    Compact group;
    group.list(1, STRUCT, columns.size());
    Compact root;
    root.text(4, "schema");
    root.number(5, columns.size());
    Compact footer;
    footer.number(1, 1);
    footer.list(2, STRUCT, 1 + columns.size());
    footer.element(root);
    for (auto c : columns) {
        if (c->rows != rows)
            throw std::runtime_error("inconsistent Parquet columns");
        footer.element(c->schema());
        if (rows) {
            c->page(file);
            size += c->size;
            group.element(c->chunk());
        }
    }
    footer.number(3, rows, I64);
    footer.list(4, STRUCT, rows ? 1 : 0);
    if (rows) {
        group.number(2, size, I64);
        group.number(3, rows, I64);
        footer.element(group);
    }
    footer.end();
    uint32_t length = footer.bytes.size();
    file.write(footer.bytes.data(), length);
    file.write(reinterpret_cast<const char *>(&length), 4);
    file.write("PAR1", 4);
    file.close();
}
} // namespace pq
struct UserOutput {
    pq::Column user{"user_id", pq::BYTE_ARRAY}, date{"day", pq::INT32},
        sessions{"sessions", pq::INT64}, events{"events", pq::INT64},
        revenue{"revenue_cents", pq::INT64}, category{"top_category", pq::BYTE_ARRAY, true};
};
struct CampaignOutput {
    pq::Column campaign{"campaign_id", pq::BYTE_ARRAY}, channel{"channel", pq::BYTE_ARRAY},
        date{"day", pq::INT32}, sessions{"sessions", pq::INT64},
        purchasing{"purchasing_sessions", pq::INT64}, revenue{"revenue_cents", pq::INT64},
        assists{"assisted_sessions", pq::INT64};
};
inline void save(UserOutput &u, const std::filesystem::path &path) {
    pq::write(path, {&u.user, &u.date, &u.sessions, &u.events, &u.revenue, &u.category});
}
inline void save(CampaignOutput &c, const std::filesystem::path &path) {
    pq::write(path, {&c.campaign, &c.channel, &c.date, &c.sessions, &c.purchasing, &c.revenue,
                     &c.assists});
}
