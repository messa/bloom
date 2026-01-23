# Makefile for native bloom algorithm C++ implementation

CXX = g++
CXXFLAGS = -std=c++17 -O3 -march=native -Wall -Wextra
LDFLAGS = -llzma -lz

TARGET = cpp_bloom_native_ngram
SOURCES = cpp_bloom_native_ngram.cpp

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(SOURCES)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -f $(TARGET)
