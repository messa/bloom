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

### Using pip

```shell
$ python3 -m pip install https://github.com/messa/bloom/archive/main.zip
```

### Using uv

[uv](https://github.com/astral-sh/uv) is a fast Python package installer and resolver, written in Rust.

```shell
$ uv tool install https://github.com/messa/bloom/archive/main.zip
```

### Development

Clone the repository and install in editable mode with development dependencies:

```shell
$ git clone https://github.com/messa/bloom.git
$ cd bloom
$ make install
```

Or manually:

```shell
$ uv venv
$ uv add -e ".[dev]"
```

Run tests:

```shell
$ make check
```

Run linting:

```shell
$ make lint       # Check for style issues
$ make lint-fix   # Automatically fix style issues
```

Run type checking:

```shell
$ make typecheck
```

All available commands:

```shell
$ make help
```

