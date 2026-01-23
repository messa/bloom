# Makefile for Binary Fuse Filter implementation

CXX = g++
CXXFLAGS = -std=c++17 -O3 -march=native -Wall -Wextra
LDFLAGS = -llzma -lz

TARGET = cpp_binaryfuse_ngram
SOURCES = cpp_binaryfuse_ngram.cpp

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(SOURCES)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -f $(TARGET)
