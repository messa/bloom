/*
 * PageBloomFilter implementation for benchmarking.
 *
 * Based on https://github.com/PeterRK/PageBloomFilter
 * License: BSD-3-Clause
 *
 * Usage:
 *   INDEX_DIR=indices/cpp_pbf ./cpp_pagebloomfilter_ngram add foo.log.xz
 *   INDEX_DIR=indices/cpp_pbf ./cpp_pagebloomfilter_ngram check foo.log.xz slovo
 */

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <regex>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <lzma.h>
#include <zlib.h>
#include "include/pbf.h"

namespace fs = std::filesystem;

// Configuration
constexpr size_t CAPACITY = 10000000;  // 10M items
constexpr float FP_RATE = 0.01f;       // 1% false positive rate
constexpr int NGRAM_SIZE = 3;

// Get n-grams from a token
std::vector<std::string> get_ngrams(const std::string& token, int n = NGRAM_SIZE) {
    std::vector<std::string> ngrams;
    if (token.length() < static_cast<size_t>(n)) {
        return ngrams;
    }
    for (size_t i = 0; i <= token.length() - n; i++) {
        ngrams.push_back(token.substr(i, n));
    }
    return ngrams;
}

// Extract tokens from a line
std::vector<std::string> tokenize(const std::string& line) {
    std::vector<std::string> tokens;
    std::regex token_pattern("[a-zA-Z0-9:_-]+");
    auto begin = std::sregex_iterator(line.begin(), line.end(), token_pattern);
    auto end = std::sregex_iterator();
    for (auto it = begin; it != end; ++it) {
        tokens.push_back(it->str());
    }
    return tokens;
}

// Read XZ compressed file
std::string read_xz_file(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file) {
        throw std::runtime_error("Cannot open file: " + path);
    }

    std::vector<uint8_t> compressed((std::istreambuf_iterator<char>(file)),
                                     std::istreambuf_iterator<char>());

    lzma_stream strm = LZMA_STREAM_INIT;
    if (lzma_stream_decoder(&strm, UINT64_MAX, LZMA_CONCATENATED) != LZMA_OK) {
        throw std::runtime_error("Failed to initialize LZMA decoder");
    }

    std::string result;
    std::vector<uint8_t> outbuf(65536);

    strm.next_in = compressed.data();
    strm.avail_in = compressed.size();

    do {
        strm.next_out = outbuf.data();
        strm.avail_out = outbuf.size();

        lzma_ret ret = lzma_code(&strm, LZMA_FINISH);

        if (ret != LZMA_OK && ret != LZMA_STREAM_END) {
            lzma_end(&strm);
            throw std::runtime_error("LZMA decompression failed");
        }

        result.append(reinterpret_cast<char*>(outbuf.data()),
                      outbuf.size() - strm.avail_out);

        if (ret == LZMA_STREAM_END) break;
    } while (strm.avail_in > 0 || strm.avail_out == 0);

    lzma_end(&strm);
    return result;
}

// Read GZ compressed file
std::string read_gz_file(const std::string& path) {
    gzFile file = gzopen(path.c_str(), "rb");
    if (!file) {
        throw std::runtime_error("Cannot open file: " + path);
    }

    std::string result;
    char buffer[65536];
    int bytes_read;

    while ((bytes_read = gzread(file, buffer, sizeof(buffer))) > 0) {
        result.append(buffer, bytes_read);
    }

    gzclose(file);
    return result;
}

// Read file (auto-detect compression)
std::string read_file(const std::string& path) {
    fs::path p(path);
    std::string ext = p.extension().string();

    if (ext == ".xz") {
        return read_xz_file(path);
    } else if (ext == ".gz") {
        return read_gz_file(path);
    } else {
        std::ifstream file(path);
        if (!file) {
            throw std::runtime_error("Cannot open file: " + path);
        }
        std::stringstream buffer;
        buffer << file.rdbuf();
        return buffer.str();
    }
}

// Get filename from path
std::string get_filename(const std::string& path) {
    return fs::path(path).filename().string();
}

// Add file to index
void add_to_index(const std::string& input_file, const std::string& index_dir) {
    fs::create_directories(index_dir);

    // Create bloom filter
    auto bf = pbf::New(CAPACITY, FP_RATE);
    if (!bf) {
        throw std::runtime_error("Failed to create bloom filter");
    }

    // Read and process file
    std::string content = read_file(input_file);
    std::istringstream stream(content);
    std::string line;

    while (std::getline(stream, line)) {
        auto tokens = tokenize(line);
        for (const auto& token : tokens) {
            auto ngrams = get_ngrams(token);
            for (const auto& ngram : ngrams) {
                bf->set(reinterpret_cast<const uint8_t*>(ngram.data()),
                       static_cast<unsigned>(ngram.size()));
            }
        }
    }

    // Write bloom filter to file
    std::string filename = get_filename(input_file);
    fs::path bloom_path = fs::path(index_dir) / (filename + ".pbf");

    std::ofstream out(bloom_path, std::ios::binary);
    if (!out) {
        throw std::runtime_error("Cannot create index file: " + bloom_path.string());
    }

    // Write header: way, page_level, page_num, unique_cnt
    uint32_t way = bf->way();
    uint32_t page_level = bf->page_level();
    uint32_t page_num = bf->page_num();
    uint64_t unique_cnt = bf->unique_cnt();

    out.write(reinterpret_cast<const char*>(&way), sizeof(way));
    out.write(reinterpret_cast<const char*>(&page_level), sizeof(page_level));
    out.write(reinterpret_cast<const char*>(&page_num), sizeof(page_num));
    out.write(reinterpret_cast<const char*>(&unique_cnt), sizeof(unique_cnt));

    // Write data
    out.write(reinterpret_cast<const char*>(bf->data()), bf->data_size());
}

// Check term in index
bool check_in_index(const std::string& input_file, const std::string& index_dir,
                    const std::string& search_term) {
    std::string filename = get_filename(input_file);
    fs::path bloom_path = fs::path(index_dir) / (filename + ".pbf");

    std::ifstream in(bloom_path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("Index file not found: " + bloom_path.string());
    }

    // Read header
    uint32_t way, page_level, page_num;
    uint64_t unique_cnt;

    in.read(reinterpret_cast<char*>(&way), sizeof(way));
    in.read(reinterpret_cast<char*>(&page_level), sizeof(page_level));
    in.read(reinterpret_cast<char*>(&page_num), sizeof(page_num));
    in.read(reinterpret_cast<char*>(&unique_cnt), sizeof(unique_cnt));

    // Read data
    size_t data_size = static_cast<size_t>(page_num) << page_level;
    std::vector<uint8_t> data(data_size);
    in.read(reinterpret_cast<char*>(data.data()), data_size);

    // Create bloom filter from data
    auto bf = pbf::New(way, page_level, page_num, unique_cnt, data.data());
    if (!bf) {
        throw std::runtime_error("Failed to load bloom filter");
    }

    // Check all n-grams
    auto ngrams = get_ngrams(search_term);
    for (const auto& ngram : ngrams) {
        if (!bf->test(reinterpret_cast<const uint8_t*>(ngram.data()),
                     static_cast<unsigned>(ngram.size()))) {
            return false;
        }
    }

    return true;
}

void print_usage(const char* prog) {
    std::cerr << "Usage: " << prog << " <add|check> ..." << std::endl;
    std::cerr << "  " << prog << " add <file>" << std::endl;
    std::cerr << "  " << prog << " check <file> <term>" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        print_usage(argv[0]);
        return 2;
    }

    const char* index_dir = std::getenv("INDEX_DIR");
    if (!index_dir) {
        std::cerr << "Error: INDEX_DIR environment variable not set" << std::endl;
        return 2;
    }

    std::string command = argv[1];

    try {
        if (command == "add") {
            if (argc != 3) {
                std::cerr << "Usage: " << argv[0] << " add <file>" << std::endl;
                return 2;
            }
            add_to_index(argv[2], index_dir);
            return 0;

        } else if (command == "check") {
            if (argc != 4) {
                std::cerr << "Usage: " << argv[0] << " check <file> <term>" << std::endl;
                return 2;
            }
            if (check_in_index(argv[2], index_dir, argv[3])) {
                return 0;  // Might contain
            } else {
                return 1;  // Definitely not
            }

        } else {
            std::cerr << "Unknown command: " << command << std::endl;
            print_usage(argv[0]);
            return 2;
        }

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 2;
    }
}
