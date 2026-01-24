# Bloom Filter Research: Loki, ClickHouse, Parquet

Detailní výzkum tří produkčních implementací bloom filtrů.

---

## 1. Grafana Loki

### Zdrojové soubory

**Repozitář:** [github.com/grafana/loki](https://github.com/grafana/loki)

**Hlavní adresář:** `pkg/storage/bloom/v1/`

| Soubor | Popis |
|--------|-------|
| `bloom.go` | Hlavní bloom filter implementace |
| `bloom_builder.go` | Builder pro konstrukci bloom filtrů |
| `bloom_querier.go` | Querying bloom filtrů |
| `bloom_tokenizer.go` | Tokenizace vstupu |
| `filter/scalable.go` | Scalable Bloom Filter implementace |
| `versioned_builder.go` | Verzované buildery (V2, V3) |
| `block.go` | Struktura bloků |
| `merge.go` | Merge operace |

### Architektura

Loki používá **Scalable Bloom Filter (SBF)** - dynamicky rostoucí bloom filter.

```
┌─────────────────────────────────────────────────────┐
│              ScalableBloomFilter                     │
├─────────────────────────────────────────────────────┤
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ │
│  │PartitionedBF│ │PartitionedBF│ │PartitionedBF│ │
│  │  (filter 0)  │ │  (filter 1)  │ │  (filter 2)  │ │
│  │  FP: 0.01    │ │  FP: 0.005   │ │  FP: 0.0025  │ │
│  └──────────────┘ └──────────────┘ └──────────────┘ │
└─────────────────────────────────────────────────────┘
```

### Klíčové parametry

| Parametr | Hodnota | Popis |
|----------|---------|-------|
| Initial capacity | 1024 | Počáteční kapacita |
| Target FP rate | 0.01 (1%) | Cílová false positive rate |
| Tightening ratio (r) | Konfigurovatelný | Jak rychle klesá FP rate |
| Growth factor (s) | 2 nebo 4 | Faktor růstu kapacity |
| Fill ratio (p) | Konfigurovatelný | Kdy přidat nový filtr |

### Scalable Bloom Filter mechanismus

```go
type ScalableBloomFilter struct {
    filters   []*PartitionedBloomFilter  // Pole filtrů
    r         float64                     // Tightening ratio
    fp        float64                     // Target FP rate
    p         float64                     // Partition fill ratio
    hint      uint                        // Initial size hint
    s         uint                        // Growth factor (2 or 4)
}
```

**Jak funguje škálování:**
1. Začíná s jedním filtrem s kapacitou `hint` a FP rate `fp`
2. Když filtr dosáhne fill ratio `p`, přidá se nový filtr
3. Nový filtr má:
   - Kapacitu zvětšenou o faktor `s`
   - FP rate sníženou geometricky (fp × r^i)
4. Test membership prochází všechny filtry

### Metody

```go
// Základní operace
func (f *ScalableBloomFilter) Add(data []byte)
func (f *ScalableBloomFilter) Test(data []byte) bool
func (f *ScalableBloomFilter) TestAndAdd(data []byte) bool

// Utility
func (f *ScalableBloomFilter) Capacity() uint
func (f *ScalableBloomFilter) Count() uint
func (f *ScalableBloomFilter) FillRatio() float64

// Serializace
func (f *ScalableBloomFilter) WriteTo(w io.Writer) (int64, error)
func (f *ScalableBloomFilter) ReadFrom(r io.Reader) (int64, error)
```

### Použití v Loki

- Bloom filtry akcelerují "needle in haystack" queries
- Umožňují přeskočit 70-90% chunks při queries
- Bloom filtry jsou ~2% velikosti logů
- Určeno pro >75TB logů/měsíc

### Obtížnost implementace pro benchmark

**VYSOKÁ** - Bloom filter je interní součást Loki, není samostatná knihovna.

**Možnosti:**
1. Extrahovat Go kód z `pkg/storage/bloom/v1/filter/`
2. Použít jako inspiraci pro vlastní Scalable Bloom Filter

---

## 2. ClickHouse

### Zdrojové soubory

**Repozitář:** [github.com/ClickHouse/ClickHouse](https://github.com/ClickHouse/ClickHouse)

**Hlavní adresář:** `src/Storages/MergeTree/`

| Soubor | Popis |
|--------|-------|
| `MergeTreeIndexBloomFilter.h` | Header - class definitions |
| `MergeTreeIndexBloomFilter.cpp` | Základní bloom filter index |
| `MergeTreeIndexBloomFilterText.h` | Header pro text bloom filter |
| `MergeTreeIndexBloomFilterText.cpp` | Tokenizovaný bloom filter (tokenbf_v1, ngrambf_v1) |

### Architektura

ClickHouse má 4 hlavní třídy:

```
┌─────────────────────────────────────────────────────────────┐
│                  MergeTreeIndexBloomFilter                   │
│                    (Factory/Orchestrator)                    │
├─────────────────────────────────────────────────────────────┤
│ ┌─────────────────────┐ ┌─────────────────────────────────┐ │
│ │ MergeTreeIndex      │ │ MergeTreeIndexCondition         │ │
│ │ GranuleBloomFilter  │ │ BloomFilter                     │ │
│ │ (Storage)           │ │ (Query evaluation - RPN)        │ │
│ └─────────────────────┘ └─────────────────────────────────┘ │
│ ┌─────────────────────┐                                     │
│ │ MergeTreeIndex      │                                     │
│ │ AggregatorBloomFilter│                                    │
│ │ (Index building)    │                                     │
│ └─────────────────────┘                                     │
└─────────────────────────────────────────────────────────────┘
```

### Typy bloom filtrů

| Typ | Použití | Popis |
|-----|---------|-------|
| `bloom_filter` | Obecné použití | Základní BF s konfigurovatelnou FP rate |
| `tokenbf_v1` | Full-text search | Tokenizovaný pro textová data |
| `ngrambf_v1` | N-gram matching | N-gram based filtering |

### Klíčové parametry

```sql
-- Příklad definice indexu
INDEX idx_name column_name TYPE bloom_filter(0.01) GRANULARITY 1
INDEX idx_text content TYPE tokenbf_v1(10240, 3, 0) GRANULARITY 1
INDEX idx_ngram content TYPE ngrambf_v1(3, 10240, 3, 0) GRANULARITY 1
```

| Parametr | Popis |
|----------|-------|
| `bits_per_row` | Velikost BF relativně k datům |
| `hash_functions` | Počet hash funkcí |
| `false_positive_rate` | Cílová FP rate (default 0.025) |

### Hash funkce

```cpp
// Seeded hashing - používá pole seedů
for (size_t i = 0; i < hash_functions; ++i) {
    bf->addHashWithSeed(bf_base_hash.getKey(),
                        BloomFilterHash::bf_hash_seed[i]);
}
```

### Sizing algoritmus

```cpp
// Výpočet velikosti v bytech
size_t bytes_size = (bits_per_row * total_rows + atom_size - 1) / atom_size;
// kde atom_size = 8

// Optimální parametry z FP rate
auto [bits, hashes] = BloomFilterHash::calculationBestPractices(false_positive_rate);
```

### Query evaluace (RPN)

ClickHouse používá Reverse Polish Notation pro evaluaci query predicates:

```cpp
// Podporované operátory
enum class RPNElement::Function {
    EQUALS,
    NOT_EQUALS,
    HAS,
    HAS_ANY,
    HAS_ALL,
    IN,
    NOT_IN
};

// Evaluace
bool mayBeTrueOnGranule(const MergeTreeIndexGranuleBloomFilter* granule) {
    // Stack-based RPN evaluation
    // Kontroluje zda hash hodnoty z query matchují bity v BF
}
```

### Obtížnost implementace pro benchmark

**VYSOKÁ** - Bloom filter je integrován do MergeTree storage engine.

**Možnosti:**
1. Extrahovat C++ kód z ClickHouse repo
2. Benchmark pouze s ClickHouse jako DB endpoint

---

## 3. Apache Parquet - Split Block Bloom Filter (SBBF)

### Specifikace

**Dokumentace:** [parquet.apache.org/docs/file-format/bloomfilter](https://parquet.apache.org/docs/file-format/bloomfilter/)

### Architektura SBBF

```
┌───────────────────────────────────────────────────────────┐
│              Split Block Bloom Filter (SBBF)               │
├───────────────────────────────────────────────────────────┤
│ ┌─────────┐ ┌─────────┐ ┌─────────┐     ┌─────────┐       │
│ │ Block 0 │ │ Block 1 │ │ Block 2 │ ... │ Block z │       │
│ │ 256 bit │ │ 256 bit │ │ 256 bit │     │ 256 bit │       │
│ └─────────┘ └─────────┘ └─────────┘     └─────────┘       │
│                                                            │
│ Každý block = 8 × 32-bit words                            │
└───────────────────────────────────────────────────────────┘
```

### Struktura bloku

Jeden blok = 256 bitů = 8 × 32-bit words

```
Block (256 bits):
┌────────────┬────────────┬────────────┬────────────┐
│  Word 0    │  Word 1    │  Word 2    │  Word 3    │
│  (32 bit)  │  (32 bit)  │  (32 bit)  │  (32 bit)  │
├────────────┼────────────┼────────────┼────────────┤
│  Word 4    │  Word 5    │  Word 6    │  Word 7    │
│  (32 bit)  │  (32 bit)  │  (32 bit)  │  (32 bit)  │
└────────────┴────────────┴────────────┴────────────┘
```

### Hash funkce

**xxHash64** s seed = 0

```python
import xxhash
hash_value = xxhash.xxh64(data, seed=0).intdigest()
```

### Algoritmus

```python
# SALT konstanty (liché čísla)
SALT = [
    0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
    0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31
]

def block_insert(block, hash_value):
    """Vloží hash do bloku."""
    for i in range(8):
        # Maska pro i-tý word
        mask = 1 << ((hash_value * SALT[i]) >> 27)
        block[i] |= mask

def block_check(block, hash_value):
    """Kontroluje přítomnost hashe v bloku."""
    for i in range(8):
        mask = 1 << ((hash_value * SALT[i]) >> 27)
        if (block[i] & mask) == 0:
            return False
    return True

def filter_insert(filter, value):
    """Vloží hodnotu do SBBF."""
    hash_value = xxhash.xxh64(value, seed=0).intdigest()
    block_index = (hash_value >> 32) % len(filter.blocks)
    block_insert(filter.blocks[block_index], hash_value & 0xFFFFFFFF)

def filter_check(filter, value):
    """Kontroluje přítomnost hodnoty v SBBF."""
    hash_value = xxhash.xxh64(value, seed=0).intdigest()
    block_index = (hash_value >> 32) % len(filter.blocks)
    return block_check(filter.blocks[block_index], hash_value & 0xFFFFFFFF)
```

### Klíčové vlastnosti

| Vlastnost | Popis |
|-----------|-------|
| Block size | 256 bitů (8 × 32-bit words) |
| Hash function | xxHash64, seed=0 |
| Block selection | Horních 32 bitů hash |
| Bit selection | Dolních 32 bitů × SALT, shift 27 |
| Cache-friendly | Ano - lokalizované přístupy |

### Rozdíly od klasického Bloom filtru

| Klasický BF | SBBF |
|-------------|------|
| Modulo pro pozice bitů | Násobení + shift |
| Náhodné přístupy do paměti | Lokalizované přístupy (256-bit blok) |
| k nezávislých hash funkcí | 1 hash + 8 SALT konstant |
| Horší cache locality | Optimalizováno pro CPU cache |

### Obtížnost implementace pro benchmark

**STŘEDNÍ** - Algoritmus je dobře dokumentovaný, lze implementovat standalone.

**Možnosti:**
1. Implementovat vlastní SBBF v Pythonu/Rustu
2. Použít PyArrow (ale API orientované na Parquet soubory)
3. Extrahovat z arrow-rs nebo parquet-java

---

## Srovnání implementací

| Aspekt | Loki | ClickHouse | Parquet SBBF |
|--------|------|------------|--------------|
| **Typ** | Scalable BF | Standard BF | Split Block BF |
| **Hash** | Nespecifikováno | Seeded hashing | xxHash64 |
| **Škálování** | Dynamické | Statické | Statické |
| **Cache-friendly** | Částečně | Ne | Ano |
| **Standalone knihovna** | Ne | Ne | Algoritmus ano |
| **Obtížnost extrakce** | Vysoká | Vysoká | Střední |

---

## Doporučení pro benchmark

### Priorita 1: Parquet SBBF
- Nejlépe dokumentovaný algoritmus
- Lze snadno implementovat standalone
- Cache-friendly design je zajímavý pro benchmarking

### Priorita 2: Loki Scalable BF
- Zajímavý pro scénáře s neznámou velikostí dat
- Go kód lze extrahovat z `pkg/storage/bloom/v1/filter/`

### Priorita 3: ClickHouse
- Komplexní systém, obtížná extrakce
- Možná pouze jako referenční bod s DB

---

## Zdroje

- [Loki Bloom Filters Documentation](https://grafana.com/docs/loki/latest/operations/bloom-filters/)
- [Loki GitHub - pkg/storage/bloom/v1](https://github.com/grafana/loki/tree/main/pkg/storage/bloom/v1)
- [ClickHouse Bloom Filter Docs](https://clickhouse.com/docs/optimize/skipping-indexes)
- [ClickHouse GitHub - MergeTree](https://github.com/ClickHouse/ClickHouse/tree/master/src/Storages/MergeTree)
- [Parquet Bloom Filter Spec](https://parquet.apache.org/docs/file-format/bloomfilter/)
- [Kirsch-Mitzenmacher Paper](https://www.eecs.harvard.edu/~michaelm/postscripts/rsa2008.pdf)
