/*
 * Trivial Bloom filter implementation in C++ for benchmarking.
 *
 * Usage:
 *   INDEX_DIR=indices/cpp ./trivial_cpp_bloom_ngram add foo.log.xz
 *   INDEX_DIR=indices/cpp ./trivial_cpp_bloom_ngram check foo.log.xz slovo
 */

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

#include <lzma.h>
#include <zlib.h>

// Configuration
constexpr size_t BITMAP_SIZE = 32 * 1024 * 1024;  // 32 MB
constexpr size_t BITMAP_BITS = BITMAP_SIZE * 8;
constexpr size_t NGRAM_SIZE = 3;

// FNV-1a hash constants for 64-bit
constexpr uint64_t FNV_OFFSET = 14695981039346656037ULL;
constexpr uint64_t FNV_PRIME = 1099511628211ULL;

uint64_t fnv1a_hash(const std::string& str) {
    uint64_t hash = FNV_OFFSET;
    for (char c : str) {
        hash ^= static_cast<uint8_t>(c);
        hash *= FNV_PRIME;
    }
    return hash;
}

size_t hash_ngram(const std::string& ngram) {
    return fnv1a_hash(ngram) % BITMAP_BITS;
}

void set_bit(std::vector<uint8_t>& bitmap, size_t bit_pos) {
    size_t byte_pos = bit_pos / 8;
    size_t bit_offset = bit_pos % 8;
    bitmap[byte_pos] |= (1 << bit_offset);
}

bool get_bit(const std::vector<uint8_t>& bitmap, size_t bit_pos) {
    size_t byte_pos = bit_pos / 8;
    size_t bit_offset = bit_pos % 8;
    return bitmap[byte_pos] & (1 << bit_offset);
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
    std::vector<uint8_t> outbuf(8192);

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
    char buffer[8192];
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
    // Create index directory if needed (simplified - assumes parent exists)
    std::string mkdir_cmd = "mkdir -p " + index_dir;
    system(mkdir_cmd.c_str());

    // Create empty bitmap
    std::vector<uint8_t> bitmap(BITMAP_SIZE, 0);

    // Read and process file
    std::string content = read_file(input_file);
    std::istringstream stream(content);
    std::string line;

    while (std::getline(stream, line)) {
        auto tokens = tokenize(line);
        for (const auto& token : tokens) {
            auto ngrams = get_ngrams(token);
            for (const auto& ngram : ngrams) {
                size_t bit_pos = hash_ngram(ngram);
                set_bit(bitmap, bit_pos);
            }
        }
    }

    // Write bitmap to file
    std::string filename = get_filename(input_file);
    std::string bitmap_path = index_dir + "/" + filename + ".bitmap";
    std::ofstream out(bitmap_path, std::ios::binary);
    if (!out) {
        throw std::runtime_error("Cannot create bitmap file: " + bitmap_path);
    }
    out.write(reinterpret_cast<char*>(bitmap.data()), bitmap.size());
}

bool check_in_index(const std::string& input_file, const std::string& index_dir,
                    const std::string& search_term) {
    // Read bitmap
    std::string filename = get_filename(input_file);
    std::string bitmap_path = index_dir + "/" + filename + ".bitmap";
    std::ifstream in(bitmap_path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("Cannot open bitmap file: " + bitmap_path);
    }

    std::vector<uint8_t> bitmap(BITMAP_SIZE);
    in.read(reinterpret_cast<char*>(bitmap.data()), bitmap.size());

    // Check all n-grams of the search term
    auto ngrams = get_ngrams(search_term);
    for (const auto& ngram : ngrams) {
        size_t bit_pos = hash_ngram(ngram);
        if (!get_bit(bitmap, bit_pos)) {
            return false;
        }
    }

    return true;
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
