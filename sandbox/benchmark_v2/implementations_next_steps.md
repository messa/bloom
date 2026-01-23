# Bloom Filter Implementations - Next Steps

Výzkum dalších možných implementací a technologií pro benchmark.

---

## 1. PageBloomFilter

**Typ:** Standalone knihovna
**Jazyky:** C++, Go, Rust, Java, C#, Python
**Zdroj:** [github.com/PeterRK/PageBloomFilter](https://github.com/PeterRK/PageBloomFilter)

### Popis
Tvrdí, že je "nejrychlejší bloom filter". C++ verze s AES-NI instrukcemi je extrémně rychlá. Benchmark ukazuje 2x rychlejší než bits-and-blooms v Go.

### Klíčové vlastnosti
- Cross-language serializace (kromě C++ AES-NI verze)
- Pouze 3 skalární parametry: `way`, `page_level`, `unique_cnt`
- Go verze bez CGO (function injection pro AMD64)

### Jak otestovat
```bash
# Go verze - wrapper script
go build -o pagebloom_ngram pagebloom_ngram.go
```

**Obtížnost:** Střední - potřebujeme Go wrapper nebo Python bindings (pokud existují)

---

## 2. bits-and-blooms/bloom

**Typ:** Go knihovna
**Zdroj:** [github.com/bits-and-blooms/bloom](https://github.com/bits-and-blooms/bloom)

### Popis
Populární Go knihovna používaná v Milvus a Beego. Používá MurmurHash.

### Klíčové vlastnosti
- Jednoduchá API: `NewWithEstimates(n, fp)`, `Add()`, `Test()`
- Serializace přes `WriteTo()` / `ReadFrom()`
- Není thread-safe

### Jak otestovat
```go
// Go wrapper
bf, _ := bloom.NewWithEstimates(10000000, 0.01)
bf.AddString("ngram")
bf.TestString("ngram")
```

**Obtížnost:** Střední - Go wrapper

---

## 3. DCSO/bloom

**Typ:** Go knihovna + CLI nástroj
**Zdroj:** [github.com/DCSO/bloom](https://github.com/DCSO/bloom)

### Popis
Efektivní implementace s CLI nástrojem. Má kompatibilní implementace v Python ([flor](https://github.com/DCSO/flor)), Rust ([poppy](https://github.com/hashlookup/poppy)), a C ([fleur](https://github.com/hashlookup/fleur)).

### Klíčové vlastnosti
- CLI pro pipeline workflows (stdin/stdout)
- Cross-platform formát souborů
- Podpora CSV s field splitting

### Jak otestovat
```bash
# Použít Python knihovnu flor
pip install flor
```

```python
from flor import BloomFilter
bf = BloomFilter(n=1000000, p=0.01)
bf.add("ngram")
"ngram" in bf
```

**Obtížnost:** Nízká - Python knihovna `flor` již existuje!

---

## 4. xxHash3

**Typ:** Hash funkce
**Zdroj:** [github.com/Cyan4973/xxHash](https://github.com/Cyan4973/xxHash)

### Popis
Extrémně rychlá non-kryptografická hash funkce. xxHash3 je nejnovější verze s lepším výkonem než xxHash64.

### Použití v bloom filtrech
- Vhodná náhrada za MD5/SHA v našich implementacích
- Python binding: `xxhash` package

### Jak otestovat
```python
import xxhash
def hash_ngram(ngram: str) -> int:
    return xxhash.xxh3_64_intdigest(ngram.encode()) % BITMAP_BITS
```

**Obtížnost:** Nízká - jen změna hash funkce v existující implementaci

---

## 5. Kirsch-Mitzenmacher optimalizace

**Typ:** Technika/algoritmus
**Zdroj:** [Harvard paper](https://www.eecs.harvard.edu/~michaelm/postscripts/rsa2008.pdf)

### Popis
Optimalizace, která umožňuje simulovat k hash funkcí pomocí pouze 2 hash funkcí:
```
g_i(x) = h1(x) + i * h2(x)
```

### Výhody
- Méně výpočtů při zachování stejné false positive rate
- Většina produkčních implementací toto používá

### Jak otestovat
Upravit naši triviální implementaci:
```python
def get_k_hashes(ngram: str, k: int) -> list[int]:
    h = hashlib.md5(ngram.encode()).digest()
    h1 = int.from_bytes(h[:8], 'little')
    h2 = int.from_bytes(h[8:16], 'little')
    return [(h1 + i * h2) % BITMAP_BITS for i in range(k)]
```

**Obtížnost:** Nízká - úprava existující implementace

---

## 6. Grafana Loki

**Typ:** Log aggregation systém
**Zdroj:** [grafana.com/docs/loki](https://grafana.com/docs/loki/latest/operations/bloom-filters/)

### Popis
Loki 3.0+ používá bloom filtry pro akceleraci "needle in haystack" queries. Bloom filtry umožňují přeskočit 70-90% chunks při queries.

### Klíčové vlastnosti
- Bloom filtry jsou ~2% velikosti logů
- Split Block Bloom Filter (SBBF) formát
- Určeno pro >75TB logů/měsíc

### Jak otestovat
**Nelze přímo** - bloom filtry jsou interní součást Loki. Ale můžeme:
- Studovat jejich SBBF formát
- Případně extrahovat jejich bloom filter knihovnu z Go kódu

**Obtížnost:** Vysoká - komplexní systém, bloom filter není samostatná knihovna

---

## 7. ClickHouse

**Typ:** OLAP databáze
**Zdroj:** [clickhouse.com/docs/optimize/skipping-indexes](https://clickhouse.com/docs/optimize/skipping-indexes)

### Popis
ClickHouse má 3 typy bloom filter indexů:
- `bloom_filter` - základní s konfigurovatelnou FP rate
- `tokenbf_v1` - tokenizovaný pro full-text
- `ngrambf_v1` - n-gram based

### Klíčové vlastnosti
- Integrované do MergeTree storage engine
- Funguje na granule úrovni
- Podpora pro Array a Map typy

### Jak otestovat
**Nelze přímo jako knihovnu** - bloom filtry jsou součást ClickHouse engine.

Možnosti:
1. Extrahovat C++ kód z ClickHouse repo
2. Použít ClickHouse jako benchmark endpoint (ne jako knihovnu)

**Obtížnost:** Vysoká - integrované do databáze

---

## 8. Apache Parquet

**Typ:** Columnar file formát
**Zdroj:** [parquet.apache.org/docs/file-format/bloomfilter](https://parquet.apache.org/docs/file-format/bloomfilter/)

### Popis
Parquet 1.12+ podporuje Split Block Bloom Filter (SBBF) pro column predicate pushdown.

### Klíčové vlastnosti
- 256-bit bloky (8 x 32-bit words)
- Uloženo na začátku column chunk
- Spark 3.2.0+ podpora

### Jak otestovat
```python
import pyarrow.parquet as pq
# PyArrow má podporu pro čtení/zápis bloom filters
# Ale API je orientované na Parquet soubory, ne standalone
```

Možnosti:
1. Použít PyArrow pro benchmark s Parquet soubory
2. Extrahovat SBBF implementaci z arrow-rs nebo parquet-java

**Obtížnost:** Střední-Vysoká - formát je dokumentovaný, ale standalone použití je netypické

---

## 9. Lance

**Typ:** Columnar formát pro AI/ML
**Zdroj:** [lance.org/format/table/index/scalar/bloom_filter](https://lance.org/format/table/index/scalar/bloom_filter/)

### Popis
Lance používá Split Block Bloom Filter (SBBF) s xxHash64 pro scalar indexy.

### Klíčové vlastnosti
- SBBF optimalizovaný pro SIMD
- xxHash64 se seed=0
- Automatické určení velikosti filtru z NDV a FPP

### Jak otestovat
```python
import lance
# Lance API je orientované na datasety, ne standalone bloom filtry
```

**Obtížnost:** Střední-Vysoká - podobně jako Parquet

---

## Doporučené další kroky

### Priorita 1 - Snadné (Python knihovny)
1. **flor (DCSO)** - Python implementace kompatibilní s DCSO/bloom
2. **xxHash3 varianta** - upravit trivial_python_bloom s xxhash
3. **Kirsch-Mitzenmacher** - upravit trivial_python_bloom s K-M optimalizací

### Priorita 2 - Střední (Go wrapper)
4. **PageBloomFilter** - Go wrapper script
5. **bits-and-blooms/bloom** - Go wrapper script

### Priorita 3 - Obtížné (extrakce z větších projektů)
6. **Parquet SBBF** - extrahovat nebo použít PyArrow
7. **Lance SBBF** - studovat implementaci
8. **Loki bloom** - studovat Go kód

### Priorita 4 - Nelze jako standalone
9. **ClickHouse** - pouze jako benchmark s DB, ne jako knihovna

---

## Zajímavé poznatky

### Split Block Bloom Filter (SBBF)
Používá Parquet, Lance i Loki. Cache-friendly varianta s 256-bit bloky. Může být zajímavé implementovat vlastní SBBF pro srovnání.

### Hash funkce
- **xxHash3** - nejrychlejší, dobrá distribuce
- **MurmurHash3** - standard v mnoha knihovnách
- **FNV-1a** - jednoduchá, dobrá pro malé vstupy

### Kirsch-Mitzenmacher
Většina produkčních implementací používá tuto optimalizaci. Naše triviální implementace by měla být upravena pro spravedlivé srovnání.
