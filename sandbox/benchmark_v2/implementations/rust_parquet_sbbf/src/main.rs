/*
 * Split Block Bloom Filter (SBBF) implementation in Rust.
 *
 * Based on Apache Parquet specification and arrow-rs implementation:
 * https://parquet.apache.org/docs/file-format/bloomfilter/
 * https://github.com/apache/arrow-rs/blob/main/parquet/src/bloom_filter/mod.rs
 *
 * Usage:
 *   INDEX_DIR=indices/rust_sbbf ./rust_parquet_sbbf_ngram add foo.log.xz
 *   INDEX_DIR=indices/rust_sbbf ./rust_parquet_sbbf_ngram check foo.log.xz slovo
 */

use flate2::read::GzDecoder;
use regex::Regex;
use std::env;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::ExitCode;
use xz2::read::XzDecoder;
use xxhash_rust::xxh64::xxh64;

// Configuration
const NUM_BLOCKS: usize = 1024 * 1024; // 1M blocks = 32 MB
const NGRAM_SIZE: usize = 3;
const XXHASH_SEED: u64 = 0;

// Salt values from Parquet specification
const SALT: [u32; 8] = [
    0x47b6137b,
    0x44974d91,
    0x8824ad5b,
    0xa2b7289d,
    0x705495c7,
    0x2df1424b,
    0x9efc4947,
    0x5c6bfb31,
];

/// A 256-bit block consisting of 8 x 32-bit words.
#[derive(Clone, Copy)]
#[repr(transparent)]
struct Block([u32; 8]);

impl Block {
    const ZERO: Block = Block([0; 8]);

    /// Generate a mask with one bit set per word using SALT constants.
    #[inline]
    fn mask(x: u32) -> [u32; 8] {
        let mut result = [0u32; 8];
        for i in 0..8 {
            // Wrapping multiply
            let y = x.wrapping_mul(SALT[i]);
            // Shift right by 27 to get bit position (0-31)
            let bit_pos = y >> 27;
            result[i] = 1 << bit_pos;
        }
        result
    }

    /// Insert a hash value into the block.
    #[inline]
    fn insert(&mut self, hash: u32) {
        let mask = Self::mask(hash);
        for i in 0..8 {
            self.0[i] |= mask[i];
        }
    }

    /// Check if a hash value might be in the block.
    #[inline]
    fn check(&self, hash: u32) -> bool {
        let mask = Self::mask(hash);
        for i in 0..8 {
            if (self.0[i] & mask[i]) == 0 {
                return false;
            }
        }
        true
    }

    /// Convert block to little-endian bytes.
    fn to_le_bytes(&self) -> [u8; 32] {
        let mut bytes = [0u8; 32];
        for (i, word) in self.0.iter().enumerate() {
            let word_bytes = word.to_le_bytes();
            bytes[i * 4..(i + 1) * 4].copy_from_slice(&word_bytes);
        }
        bytes
    }

    /// Create block from little-endian bytes.
    fn from_le_bytes(bytes: &[u8]) -> Self {
        let mut words = [0u32; 8];
        for i in 0..8 {
            words[i] = u32::from_le_bytes([
                bytes[i * 4],
                bytes[i * 4 + 1],
                bytes[i * 4 + 2],
                bytes[i * 4 + 3],
            ]);
        }
        Block(words)
    }
}

/// Split Block Bloom Filter as specified by Apache Parquet.
struct SplitBlockBloomFilter {
    blocks: Vec<Block>,
}

impl SplitBlockBloomFilter {
    /// Create a new SBBF with given number of blocks.
    fn new(num_blocks: usize) -> Self {
        Self {
            blocks: vec![Block::ZERO; num_blocks],
        }
    }

    /// Calculate block index from hash value.
    #[inline]
    fn block_index(&self, hash: u64) -> usize {
        // Use high 32 bits of hash, then map to block range
        let high_bits = hash >> 32;
        let num_blocks = self.blocks.len() as u64;
        ((high_bits.wrapping_mul(num_blocks)) >> 32) as usize
    }

    /// Insert a value into the filter.
    fn insert(&mut self, value: &str) {
        let hash = xxh64(value.as_bytes(), XXHASH_SEED);
        let block_index = self.block_index(hash);
        self.blocks[block_index].insert(hash as u32);
    }

    /// Check if a value might be in the filter.
    fn check(&self, value: &str) -> bool {
        let hash = xxh64(value.as_bytes(), XXHASH_SEED);
        let block_index = self.block_index(hash);
        self.blocks[block_index].check(hash as u32)
    }

    /// Write the filter to a file.
    fn write_to_file(&self, path: &Path) -> std::io::Result<()> {
        let mut file = File::create(path)?;
        for block in &self.blocks {
            file.write_all(&block.to_le_bytes())?;
        }
        Ok(())
    }

    /// Read the filter from a file.
    fn read_from_file(path: &Path) -> std::io::Result<Self> {
        let mut file = File::open(path)?;
        let file_size = file.metadata()?.len() as usize;
        let num_blocks = file_size / 32;

        let mut blocks = Vec::with_capacity(num_blocks);
        let mut buf = [0u8; 32];

        for _ in 0..num_blocks {
            file.read_exact(&mut buf)?;
            blocks.push(Block::from_le_bytes(&buf));
        }

        Ok(Self { blocks })
    }
}

fn get_ngrams(token: &str) -> Vec<String> {
    let chars: Vec<char> = token.chars().collect();
    if chars.len() < NGRAM_SIZE {
        return vec![];
    }
    (0..=chars.len() - NGRAM_SIZE)
        .map(|i| chars[i..i + NGRAM_SIZE].iter().collect())
        .collect()
}

fn open_file(filepath: &Path) -> Result<Box<dyn BufRead>, std::io::Error> {
    let file = File::open(filepath)?;
    let extension = filepath
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();

    match extension.as_str() {
        "xz" => {
            let decoder = XzDecoder::new(file);
            Ok(Box::new(BufReader::new(decoder)))
        }
        "gz" => {
            let decoder = GzDecoder::new(file);
            Ok(Box::new(BufReader::new(decoder)))
        }
        _ => Ok(Box::new(BufReader::new(file))),
    }
}

fn get_filename(filepath: &Path) -> String {
    filepath
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown")
        .to_string()
}

fn add_to_index(input_file: &str, index_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    let input_path = Path::new(input_file);
    let index_path = Path::new(index_dir);

    // Create index directory if needed
    fs::create_dir_all(index_path)?;

    // Create SBBF
    let mut sbbf = SplitBlockBloomFilter::new(NUM_BLOCKS);

    // Token pattern
    let token_pattern = Regex::new(r"[a-zA-Z0-9:_-]+")?;

    // Read file line by line
    let reader = open_file(input_path)?;
    for line in reader.lines() {
        let line = line.unwrap_or_default();
        for mat in token_pattern.find_iter(&line) {
            let token = mat.as_str();
            for ngram in get_ngrams(token) {
                sbbf.insert(&ngram);
            }
        }
    }

    // Save SBBF
    let filename = get_filename(input_path);
    let sbbf_path = index_path.join(format!("{}.rustsbbf", filename));
    sbbf.write_to_file(&sbbf_path)?;

    Ok(())
}

fn check_in_index(
    input_file: &str,
    index_dir: &str,
    search_term: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let input_path = Path::new(input_file);
    let index_path = Path::new(index_dir);

    let filename = get_filename(input_path);
    let sbbf_path = index_path.join(format!("{}.rustsbbf", filename));

    if !sbbf_path.exists() {
        return Err(format!("Index file not found: {}", sbbf_path.display()).into());
    }

    // Load SBBF
    let sbbf = SplitBlockBloomFilter::read_from_file(&sbbf_path)?;

    // Check all n-grams
    for ngram in get_ngrams(search_term) {
        if !sbbf.check(&ngram) {
            return Ok(false);
        }
    }

    Ok(true)
}

fn print_usage(program: &str) {
    eprintln!("Usage: {} <add|check> ...", program);
    eprintln!("  {} add <file>", program);
    eprintln!("  {} check <file> <term>", program);
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        print_usage(&args[0]);
        return ExitCode::from(2);
    }

    let index_dir = match env::var("INDEX_DIR") {
        Ok(dir) => dir,
        Err(_) => {
            eprintln!("Error: INDEX_DIR environment variable not set");
            return ExitCode::from(2);
        }
    };

    let command = &args[1];

    match command.as_str() {
        "add" => {
            if args.len() != 3 {
                eprintln!("Usage: {} add <file>", args[0]);
                return ExitCode::from(2);
            }
            match add_to_index(&args[2], &index_dir) {
                Ok(_) => ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("Error: {}", e);
                    ExitCode::from(2)
                }
            }
        }
        "check" => {
            if args.len() != 4 {
                eprintln!("Usage: {} check <file> <term>", args[0]);
                return ExitCode::from(2);
            }
            match check_in_index(&args[2], &index_dir, &args[3]) {
                Ok(true) => ExitCode::SUCCESS,  // Might contain
                Ok(false) => ExitCode::from(1), // Definitely not
                Err(e) => {
                    eprintln!("Error: {}", e);
                    ExitCode::from(2)
                }
            }
        }
        _ => {
            eprintln!("Unknown command: {}", command);
            print_usage(&args[0]);
            ExitCode::from(2)
        }
    }
}
