/*
 * Bloom filter implementation in Rust using fastbloom crate.
 *
 * Usage:
 *   INDEX_DIR=indices/rust ./rust_fastbloom_ngram add foo.log.xz
 *   INDEX_DIR=indices/rust ./rust_fastbloom_ngram check foo.log.xz slovo
 */

use fastbloom::BloomFilter;
use flate2::read::GzDecoder;
use regex::Regex;
use std::env;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::ExitCode;
use xz2::read::XzDecoder;

// Configuration
const CAPACITY: usize = 10_000_000;
const FP_RATE: f64 = 0.01;
const NGRAM_SIZE: usize = 3;

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

    // Create bloom filter
    let mut bf = BloomFilter::with_false_pos(FP_RATE).expected_items(CAPACITY);

    // Token pattern
    let token_pattern = Regex::new(r"[a-zA-Z0-9:_-]+")?;

    // Read file line by line
    let reader = open_file(input_path)?;
    for line in reader.lines() {
        let line = line.unwrap_or_default();
        for mat in token_pattern.find_iter(&line) {
            let token = mat.as_str();
            for ngram in get_ngrams(token) {
                bf.insert(&ngram);
            }
        }
    }

    // Save bloom filter using bincode
    let filename = get_filename(input_path);
    let bloom_path = index_path.join(format!("{}.rustbloom", filename));
    let encoded = bincode::serialize(&bf)?;
    fs::write(&bloom_path, encoded)?;

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
    let bloom_path = index_path.join(format!("{}.rustbloom", filename));

    if !bloom_path.exists() {
        return Err(format!("Index file not found: {}", bloom_path.display()).into());
    }

    // Load bloom filter using bincode
    let data = fs::read(&bloom_path)?;
    let bf: BloomFilter = bincode::deserialize(&data)?;

    // Check all n-grams
    for ngram in get_ngrams(search_term) {
        if !bf.contains(&ngram) {
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
                Ok(true) => ExitCode::SUCCESS,        // Might contain
                Ok(false) => ExitCode::from(1),       // Definitely not
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
