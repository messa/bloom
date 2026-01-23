# Makefile for PageBloomFilter n-gram implementation

CXX = g++
CXXFLAGS = -std=c++17 -O3 -march=native -Wall -Wextra -I.
LDFLAGS = -llzma -lz

# Source files
SOURCES = cpp_pagebloomfilter_ngram.cpp src/pbf.cc src/hash.cc
TARGET = cpp_pagebloomfilter_ngram

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(SOURCES)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -f $(TARGET)
