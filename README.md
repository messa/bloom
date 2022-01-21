bloom
=====

When you need to search a string in a lot of files, `bloom` can index these files (using [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) data structure) and then effectively filter out files that do not contain the searched string.

Compressed files are automatically decompressed before indexing.

`bloom` accepts list of files on standard input and searched phrase(s) as command line argument.
List of possibly matching files is printed to standard output.
If the `--verbose` (or `-v`) option is passed on command line, debug and statistics messages are printed to standard error output.

```shell
$ ls -1
mail.log
mail.log.1
mail.log.2.gz
$ find -type f | xargs xzegrep -H -c B3BC8A1F1D
./mail.log:0
./mail.log.1:5
./mail.log.2.gz:0
$ find -type f | bloom B3BC8A1F1D | xargs xzegrep -H -c B3BC8A1F1D
mail.log.1:5
```

The index data is stored in a SQLite database file, default path is `~/.cache/bloom/db`.
This path can be changed using command line option `--db` or environment variable `BLOOM_DB`.


Installation
------------

```shell
$ python3 -m pip install https://github.com/messa/bloom/archive/main.zip
```

