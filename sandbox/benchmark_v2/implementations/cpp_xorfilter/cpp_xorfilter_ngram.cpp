/*
 * XOR Filter implementation for benchmarking.
 *
 * XOR filters have ~23% space overhead (vs. 44% for Bloom filters)
 * with exactly 3 memory accesses per query.
 *
 * Based on: https://github.com/FastFilter/fastfilter_cpp
 * Paper: "Xor Filters: Faster and Smaller Than Bloom and Cuckoo Filters" (2019)
 *
 * Usage:
 *   INDEX_DIR=indices/xorfilter ./cpp_xorfilter_ngram add foo.log.xz
 *   INDEX_DIR=indices/xorfilter ./cpp_xorfilter_ngram check foo.log.xz slovo
 */

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <lzma.h>
#include <zlib.h>

#include "xorfilter_singleheader.h"

// Configuration
constexpr size_t NGRAM_SIZE = 3;

// FNV-1a hash for converting strings to uint64_t keys
constexpr uint64_t FNV_OFFSET = 14695981039346656037ULL;
constexpr uint64_t FNV_PRIME = 1099511628211ULL;

uint64_t fnv1a_64(const std::string& str) {
    uint64_t hash = FNV_OFFSET;
    for (char c : str) {
        hash ^= static_cast<uint8_t>(c);
        hash *= FNV_PRIME;
    }
    return hash;
}

std::vector<std::string> get_ngrams(const std::string& token, size_t n = NGRAM_SIZE) {
    std::vector<std::string> ngrams;
    if (token.length() >= n) {
        for (size_t i = 0; i <= token.length() - n; ++i) {
            ngrams.push_back(token.substr(i, n));
        }
    }
    return ngrams;
}

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

// Read and decompress .xz file
std::string read_xz_file(const std::string& filepath) {
    std::ifstream file(filepath, std::ios::binary);
    if (!file) {
        throw std::runtime_error("Cannot open file: " + filepath);
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

    lzma_ret ret;
    do {
        strm.next_out = outbuf.data();
        strm.avail_out = outbuf.size();

        ret = lzma_code(&strm, LZMA_FINISH);

        if (ret != LZMA_OK && ret != LZMA_STREAM_END) {
            lzma_end(&strm);
            throw std::runtime_error("LZMA decompression failed");
        }

        result.append(reinterpret_cast<char*>(outbuf.data()),
                      outbuf.size() - strm.avail_out);
    } while (ret != LZMA_STREAM_END);

    lzma_end(&strm);
    return result;
}

// Read and decompress .gz file
std::string read_gz_file(const std::string& filepath) {
    gzFile file = gzopen(filepath.c_str(), "rb");
    if (!file) {
        throw std::runtime_error("Cannot open file: " + filepath);
    }

    std::string result;
    char buffer[65536];
    int bytes_read;

    while ((bytes_read = gzread(file, buffer, sizeof(buffer))) > 0) {
        result.append(buffer, bytes_read);
    }

    if (bytes_read < 0) {
        gzclose(file);
        throw std::runtime_error("Error reading gzip file");
    }

    gzclose(file);
    return result;
}

// Read plain text file
std::string read_plain_file(const std::string& filepath) {
    std::ifstream file(filepath);
    if (!file) {
        throw std::runtime_error("Cannot open file: " + filepath);
    }
    std::stringstream buffer;
    buffer << file.rdbuf();
    return buffer.str();
}

std::string read_file(const std::string& filepath) {
    std::string ext;
    size_t dot_pos = filepath.rfind('.');
    if (dot_pos != std::string::npos) {
        ext = filepath.substr(dot_pos);
        std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);
    }

    if (ext == ".xz") {
        return read_xz_file(filepath);
    } else if (ext == ".gz") {
        return read_gz_file(filepath);
    } else {
        return read_plain_file(filepath);
    }
}

std::string get_filename(const std::string& filepath) {
    size_t pos = filepath.rfind('/');
    if (pos != std::string::npos) {
        return filepath.substr(pos + 1);
    }
    return filepath;
}

void add_to_index(const std::string& input_file, const std::string& index_dir) {
    // Create index directory if needed
    std::string mkdir_cmd = "mkdir -p " + index_dir;
    system(mkdir_cmd.c_str());

    // Collect all unique n-gram hashes
    std::set<uint64_t> unique_keys;

    // Read and process file
    std::string content = read_file(input_file);
    std::istringstream stream(content);
    std::string line;

    while (std::getline(stream, line)) {
        auto tokens = tokenize(line);
        for (const auto& token : tokens) {
            auto ngrams = get_ngrams(token);
            for (const auto& ngram : ngrams) {
                unique_keys.insert(fnv1a_64(ngram));
            }
        }
    }

    // Convert to vector for XOR filter
    std::vector<uint64_t> keys(unique_keys.begin(), unique_keys.end());

    if (keys.empty()) {
        // Handle empty file - create minimal filter
        keys.push_back(0);
    }

    // Create XOR filter
    xor8_t filter;
    if (!xor8_allocate(static_cast<uint32_t>(keys.size()), &filter)) {
        throw std::runtime_error("Failed to allocate XOR filter");
    }

    if (!xor8_populate(keys.data(), static_cast<uint32_t>(keys.size()), &filter)) {
        free(filter.fingerprints);
        throw std::runtime_error("Failed to populate XOR filter");
    }

    // Write filter to file
    std::string filename = get_filename(input_file);
    std::string filter_path = index_dir + "/" + filename + ".xor8";
    std::ofstream out(filter_path, std::ios::binary);
    if (!out) {
        free(filter.fingerprints);
        throw std::runtime_error("Cannot create filter file: " + filter_path);
    }

    // Write header
    out.write(reinterpret_cast<const char*>(&filter.seed), sizeof(filter.seed));
    out.write(reinterpret_cast<const char*>(&filter.blockLength), sizeof(filter.blockLength));

    // Write fingerprints (3 * blockLength)
    uint64_t fingerprints_size = 3 * filter.blockLength;
    out.write(reinterpret_cast<const char*>(filter.fingerprints), fingerprints_size);

    free(filter.fingerprints);
}

bool check_in_index(const std::string& input_file, const std::string& index_dir,
                    const std::string& search_term) {
    // Read filter
    std::string filename = get_filename(input_file);
    std::string filter_path = index_dir + "/" + filename + ".xor8";
    std::ifstream in(filter_path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("Cannot open filter file: " + filter_path);
    }

    // Read header
    xor8_t filter;
    in.read(reinterpret_cast<char*>(&filter.seed), sizeof(filter.seed));
    in.read(reinterpret_cast<char*>(&filter.blockLength), sizeof(filter.blockLength));

    // Read fingerprints
    uint64_t fingerprints_size = 3 * filter.blockLength;
    filter.fingerprints = new uint8_t[fingerprints_size];
    in.read(reinterpret_cast<char*>(filter.fingerprints), fingerprints_size);

    // Check all n-grams
    auto ngrams = get_ngrams(search_term);
    bool found = true;
    for (const auto& ngram : ngrams) {
        uint64_t key = fnv1a_64(ngram);
        if (!xor8_contain(key, &filter)) {
            found = false;
            break;
        }
    }

    delete[] filter.fingerprints;
    return found;
}

void print_usage(const char* program) {
    std::cerr << "Usage: " << program << " <add|check> ..." << std::endl;
    std::cerr << "  " << program << " add <file>" << std::endl;
    std::cerr << "  " << program << " check <file> <term>" << std::endl;
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
                return 0;  // Might contain the term
            } else {
                return 1;  // Definitely does not contain the term
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
