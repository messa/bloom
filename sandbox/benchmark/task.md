# Bloom filter implementation benchmark

**Note:** See `learnings.md` for accumulated knowledge, URLs, and implementation notes from previous work on this benchmark.

The benchmark is about false positive rate and index size.
It is not about implementation speed.
We expect that query performance is very fast and indexing performance reasonable fast for any C, C++, Rust or Go implementation.

The assignment is:

1. Find different bloom filter implementations

  - Including: PageBloomFilter, fastbloom, rbloom, bits-and-blooms/bloom, DCSO/bloom
  - Also including this project (github.com:messa/bloom)
  - Including xxHash3 and Kirsch-Mitzenmacher
  - I'm interested also in bloom filter implementation used in Loki, Clickhouse, Parquet and Lance
  - Write a report about the implementations and their features and characteristics

2. Choose 5+ of them (with available source code) and extract/implement them here (in sandbox/benchmark directory)

3. Create a script that generates a large dataset (e.g. 10 log files with 100-300 MB size each when compressed in .xz)

  - The script should be determistic and reproducible so the sample log file contents is the same every time
  - Do not commit the sample log files in git - add them to the .gitignore file
  - After you generate the log files, compute their sha1 hash and commit it to the git repository
  - The log files should simulate real-world log files from different sources (e.g. fastapi servers, microservices, MongoDB)
  - Most of the log files are unstructured text. Only MongoDB log files are structured JSON.

4. Create a benchmark program that evaluates all these implementations (in one or more configurations) on the generated dataset.

  - Preferred architecture is a Python benchmark that calls the implementations (compiled from C++ etc.) in subprocesses

5. Run the benchmark program and write report about the benchmark results.

Preferred programming languages: Python, C++, C, Rust, Go.

Keep track of the progress in file `progress.md`. Add more todo items as needed. Commit everything in git and perform git push.

We aim for false positive rate < 1 % and index size < 10 % if the compressed log size.
Ideally the false positive rate woul be < 0.1 %.

THe point of this benchmark is to run different implementations against the same dataset of log files and queries and compare their false positive rate and index size.

Sample queries:

- specific URL path or part of it
- request id
- user/account/business/campaign/order id
- part of an error message
- function name or filename from error message or error traceback
