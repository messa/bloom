# Makefile for XOR Filter implementation

CXX = g++
CXXFLAGS = -std=c++17 -O3 -march=native -Wall -Wextra
LDFLAGS = -llzma -lz

TARGET = cpp_xorfilter_ngram
SOURCES = cpp_xorfilter_ngram.cpp

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(SOURCES)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -f $(TARGET)
