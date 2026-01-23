# Learnings - Bloom Filter Benchmark

Tento soubor obsahuje poznámky a zjištění, která se mohou hodit pro další práci na benchmarku.

## Python knihovny

Pro Python knihovny stačí nainstalovat přes pip, není potřeba extrahovat jejich implementaci:

- **fastbloom-rs**: `pip install fastbloom-rs` - Rust implementace s Python binding
- **rbloom**: `pip install rbloom` - Rust implementace s Python binding

## Zdroje a URL adresy

### Bloom filter implementace

| Implementace | URL | Jazyk | Poznámky |
|-------------|-----|-------|----------|
| PageBloomFilter | https://github.com/PeterRK/PageBloomFilter | C++, Go, Rust, Java, C# | Nejrychlejší, AES-NI optimalizace |
| fastbloom-rs | https://github.com/yankun1992/fastbloom | Rust + Python | 10x rychlejší než pybloom |
| rbloom | https://github.com/KenanHanke/rbloom | Rust + Python | Pythonic API |
| bits-and-blooms/bloom | https://github.com/bits-and-blooms/bloom | Go | Používá murmurhash |
| DCSO/bloom | https://github.com/DCSO/bloom | Go | CLI + knihovna |
| DCSO/flor | https://github.com/DCSO/flor | Python | Kompatibilní s DCSO/bloom |
| hashlookup/fleur | https://github.com/hashlookup/fleur | C | Kompatibilní s DCSO/bloom |

### Hash funkce

| Implementace | URL | Poznámky |
|-------------|-----|----------|
| xxHash | https://github.com/Cyan4973/xxHash | Referenční C implementace |
| xxhash3 (Go) | https://github.com/zeebo/xxh3 | Go implementace xxHash3 |

### Dokumentace

| Téma | URL |
|------|-----|
| Kirsch-Mitzenmacher paper | https://www.eecs.harvard.edu/~michaelm/postscripts/rsa2008.pdf |
| Parquet Bloom Filter spec | https://parquet.apache.org/docs/file-format/bloomfilter/ |
| Lance Bloom Filter | https://lance.org/format/table/index/scalar/bloom_filter/ |
| Loki Bloom Filters | https://grafana.com/docs/loki/latest/operations/bloom-filters/ |
| ClickHouse Skip Indexes | https://clickhouse.com/docs/optimize/skipping-indexes |

## Technické poznámky

### Kirsch-Mitzenmacher optimalizace

Technika pro simulaci k hash funkcí pomocí pouze 2 hash funkcí:
```
gi(x) = h1(x) + i * h2(x) mod m
```

Používá se v většině moderních implementací.

### SIMD optimalizace

Moderní implementace (Parquet, Lance, PageBloomFilter) používají 256-bit bloky pro SIMD efektivitu:
- 256-bit bloky rozdělené na 8x 32-bit slov
- Split Block Bloom Filter (SBBF) architektura

### Doporučené hash funkce

- **xxHash64** - široce používaný v Parquet, Lance
- **murmurhash** - běžný v Go implementacích
- **FNV-1a** - jednoduchý, používá messa/bloom

### Cílové metriky

- FPP < 1% (ideálně < 0.1%)
- Index velikost < 10% komprimované velikosti logů

## Instalace závislostí

```bash
# Python knihovny
pip install fastbloom-rs rbloom

# Go build pro CLI wrappery
cd implementations/bits-and-blooms-bloom && go build -o bloom-cli .
cd implementations/dcso-bloom && go build -o bloom-cli .
cd implementations/pagebloomfilter && go build -o pagebloom-cli .
```

## Benchmark architektura

Benchmark používá:
1. Python jako orchestrátor
2. Native Python knihovny (fastbloom-rs, rbloom)
3. Go CLI wrappery volaní přes subprocess

N-gram sampling:
- Extrakce 4, 5, 6-byte vzorků z každého řádku logu
- Lowercase normalizace
