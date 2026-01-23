CXX = g++
CXXFLAGS = -std=c++17 -O2 -Wall
LDFLAGS = -llzma -lz

TARGET = trivial_cpp_bloom_ngram
SRC = trivial_cpp_bloom_ngram.cpp

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(TARGET)

.PHONY: all clean
