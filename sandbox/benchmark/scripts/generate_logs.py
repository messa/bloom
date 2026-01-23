#!/usr/bin/env python3
"""
Deterministic log file generator for bloom filter benchmark.

Generates 10 log files with 100-300 MB compressed size each.
Uses fixed seed for reproducibility.
"""

import argparse
import gzip
import hashlib
import json
import lzma
import os
import random
import string
from datetime import datetime, timedelta
from pathlib import Path

# Fixed seed for reproducibility
RANDOM_SEED = 42

# Sample data for generating realistic logs
HTTP_METHODS = ['GET', 'POST', 'PUT', 'DELETE', 'PATCH']
HTTP_STATUS_CODES = [200, 201, 204, 301, 302, 400, 401, 403, 404, 500, 502, 503]
URL_PATHS = [
    '/api/v1/users', '/api/v1/users/{id}', '/api/v1/orders', '/api/v1/orders/{id}',
    '/api/v1/products', '/api/v1/products/{id}', '/api/v1/auth/login', '/api/v1/auth/logout',
    '/api/v1/campaigns', '/api/v1/campaigns/{id}/stats', '/api/v1/accounts/{id}',
    '/api/v1/businesses/{id}', '/api/v2/analytics', '/api/v2/reports', '/health', '/metrics',
    '/api/v1/search', '/api/v1/notifications', '/api/v1/settings', '/api/v1/webhooks',
]
ERROR_MESSAGES = [
    'Connection refused', 'Timeout waiting for response', 'Invalid JSON payload',
    'Authentication failed', 'Permission denied', 'Resource not found',
    'Database connection lost', 'Rate limit exceeded', 'Internal server error',
    'Service unavailable', 'Bad gateway', 'Invalid request parameters',
    'Session expired', 'Token validation failed', 'Duplicate key error',
]
PYTHON_FILES = [
    'main.py', 'api/routes.py', 'services/user_service.py', 'services/order_service.py',
    'models/user.py', 'models/order.py', 'utils/helpers.py', 'middleware/auth.py',
    'handlers/error_handler.py', 'database/connection.py', 'cache/redis_client.py',
]
FUNCTION_NAMES = [
    'process_request', 'validate_input', 'get_user_by_id', 'create_order',
    'update_product', 'authenticate_user', 'check_permissions', 'handle_error',
    'send_notification', 'generate_report', 'calculate_stats', 'fetch_data',
]
SERVICE_NAMES = [
    'user-service', 'order-service', 'product-service', 'auth-service',
    'notification-service', 'analytics-service', 'payment-service', 'gateway',
]
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
MONGODB_COLLECTIONS = ['users', 'orders', 'products', 'sessions', 'logs', 'events']
MONGODB_OPERATIONS = ['find', 'insert', 'update', 'delete', 'aggregate', 'createIndex']


class DeterministicRandom:
    """Wrapper around random module with fixed seed."""

    def __init__(self, seed: int):
        self.rng = random.Random(seed)

    def choice(self, seq):
        return self.rng.choice(seq)

    def randint(self, a, b):
        return self.rng.randint(a, b)

    def random(self):
        return self.rng.random()

    def choices(self, seq, k=1):
        return self.rng.choices(seq, k=k)

    def uuid(self):
        """Generate a deterministic UUID-like string."""
        return ''.join(self.rng.choices(string.hexdigits.lower(), k=32))

    def request_id(self):
        """Generate a request ID."""
        return f"req-{self.uuid()[:16]}"

    def user_id(self):
        return f"usr_{self.randint(10000, 99999)}"

    def order_id(self):
        return f"ord_{self.randint(100000, 999999)}"

    def campaign_id(self):
        return f"cmp_{self.randint(1000, 9999)}"

    def account_id(self):
        return f"acc_{self.randint(10000, 99999)}"

    def business_id(self):
        return f"biz_{self.randint(1000, 9999)}"

    def ip_address(self):
        return f"{self.randint(1, 255)}.{self.randint(0, 255)}.{self.randint(0, 255)}.{self.randint(0, 255)}"


def generate_fastapi_log_line(rng: DeterministicRandom, timestamp: datetime) -> str:
    """Generate a FastAPI-style log line."""
    method = rng.choice(HTTP_METHODS)
    path = rng.choice(URL_PATHS).replace('{id}', str(rng.randint(1, 100000)))
    status = rng.choice(HTTP_STATUS_CODES)
    duration = rng.randint(1, 5000)
    request_id = rng.request_id()
    client_ip = rng.ip_address()
    user_id = rng.user_id() if rng.random() > 0.3 else '-'

    if status >= 400 and rng.random() > 0.5:
        error = rng.choice(ERROR_MESSAGES)
        return f'{timestamp.isoformat()} INFO     [{request_id}] {client_ip} - {user_id} "{method} {path}" {status} {duration}ms - {error}\n'

    return f'{timestamp.isoformat()} INFO     [{request_id}] {client_ip} - {user_id} "{method} {path}" {status} {duration}ms\n'


def generate_fastapi_error_traceback(rng: DeterministicRandom, timestamp: datetime) -> str:
    """Generate a FastAPI error with traceback."""
    request_id = rng.request_id()
    error_msg = rng.choice(ERROR_MESSAGES)
    file = rng.choice(PYTHON_FILES)
    func = rng.choice(FUNCTION_NAMES)
    line_no = rng.randint(10, 500)

    lines = [
        f'{timestamp.isoformat()} ERROR    [{request_id}] Unhandled exception:\n',
        f'Traceback (most recent call last):\n',
        f'  File "/app/{file}", line {line_no}, in {func}\n',
        f'    raise RuntimeError("{error_msg}")\n',
        f'RuntimeError: {error_msg}\n',
    ]
    return ''.join(lines)


def generate_microservice_log_line(rng: DeterministicRandom, timestamp: datetime, service_name: str) -> str:
    """Generate a microservice-style log line."""
    level = rng.choice(LOG_LEVELS)
    request_id = rng.request_id()

    if level == 'ERROR':
        error = rng.choice(ERROR_MESSAGES)
        return f'[{timestamp.isoformat()}] {level} [{service_name}] [{request_id}] {error}\n'
    elif level == 'WARNING':
        msg = rng.choice([
            f'Slow response from downstream service: {rng.randint(1000, 10000)}ms',
            f'Retry attempt {rng.randint(1, 5)} for request',
            f'Cache miss for key user:{rng.user_id()}',
            f'High memory usage: {rng.randint(70, 95)}%',
        ])
        return f'[{timestamp.isoformat()}] {level} [{service_name}] [{request_id}] {msg}\n'
    elif level == 'DEBUG':
        msg = rng.choice([
            f'Processing request for user {rng.user_id()}',
            f'Fetching order {rng.order_id()}',
            f'Cache hit for key campaign:{rng.campaign_id()}',
            f'Database query executed in {rng.randint(1, 100)}ms',
        ])
        return f'[{timestamp.isoformat()}] {level} [{service_name}] [{request_id}] {msg}\n'
    else:  # INFO
        msg = rng.choice([
            f'Request completed successfully',
            f'User {rng.user_id()} authenticated',
            f'Order {rng.order_id()} created for account {rng.account_id()}',
            f'Campaign {rng.campaign_id()} stats updated',
            f'Business {rng.business_id()} profile loaded',
        ])
        return f'[{timestamp.isoformat()}] {level} [{service_name}] [{request_id}] {msg}\n'


def generate_mongodb_log_line(rng: DeterministicRandom, timestamp: datetime) -> str:
    """Generate a MongoDB-style JSON log line."""
    collection = rng.choice(MONGODB_COLLECTIONS)
    operation = rng.choice(MONGODB_OPERATIONS)
    duration_ms = rng.randint(1, 1000)

    log_entry = {
        "t": {"$date": timestamp.isoformat() + "Z"},
        "s": rng.choice(["I", "W", "E"]),
        "c": "COMMAND",
        "id": rng.randint(10000, 99999),
        "ctx": f"conn{rng.randint(1, 1000)}",
        "msg": f"{operation} command",
        "attr": {
            "type": "command",
            "ns": f"mydb.{collection}",
            "command": {
                operation: collection,
            },
            "durationMillis": duration_ms,
            "planSummary": rng.choice(["IXSCAN", "COLLSCAN", "IDHACK"]),
        }
    }

    if operation in ['find', 'update', 'delete']:
        log_entry["attr"]["command"]["filter"] = {"_id": rng.uuid()}

    if rng.random() > 0.7:
        log_entry["attr"]["user_id"] = rng.user_id()
        log_entry["attr"]["account_id"] = rng.account_id()

    return json.dumps(log_entry) + '\n'


def generate_log_file(output_path: Path, file_type: str, seed_offset: int,
                      target_compressed_mb: int) -> tuple[int, int]:
    """
    Generate a log file of specified type.

    Returns tuple of (uncompressed_size, compressed_size).
    """
    rng = DeterministicRandom(RANDOM_SEED + seed_offset)

    # Start from a fixed date
    base_timestamp = datetime(2024, 1, 1, 0, 0, 0)
    current_timestamp = base_timestamp

    # Estimate: compressed is roughly 5-10% of uncompressed for text logs
    # So target 10-20x the compressed size for uncompressed
    target_uncompressed_bytes = target_compressed_mb * 1024 * 1024 * 15

    uncompressed_size = 0
    lines = []

    if file_type == 'fastapi':
        service_name = None
        while uncompressed_size < target_uncompressed_bytes:
            # Generate regular log line
            line = generate_fastapi_log_line(rng, current_timestamp)
            lines.append(line)
            uncompressed_size += len(line.encode('utf-8'))

            # Occasionally add error tracebacks
            if rng.random() < 0.01:
                traceback = generate_fastapi_error_traceback(rng, current_timestamp)
                lines.append(traceback)
                uncompressed_size += len(traceback.encode('utf-8'))

            # Advance timestamp by 1-1000ms
            current_timestamp += timedelta(milliseconds=rng.randint(1, 1000))

    elif file_type == 'microservice':
        service_name = rng.choice(SERVICE_NAMES)
        while uncompressed_size < target_uncompressed_bytes:
            line = generate_microservice_log_line(rng, current_timestamp, service_name)
            lines.append(line)
            uncompressed_size += len(line.encode('utf-8'))
            current_timestamp += timedelta(milliseconds=rng.randint(1, 500))

    elif file_type == 'mongodb':
        while uncompressed_size < target_uncompressed_bytes:
            line = generate_mongodb_log_line(rng, current_timestamp)
            lines.append(line)
            uncompressed_size += len(line.encode('utf-8'))
            current_timestamp += timedelta(milliseconds=rng.randint(1, 200))

    # Write to compressed file
    content = ''.join(lines).encode('utf-8')

    if output_path.suffix == '.xz':
        with lzma.open(output_path, 'wb', preset=6) as f:
            f.write(content)
    elif output_path.suffix == '.gz':
        with gzip.open(output_path, 'wb', compresslevel=6) as f:
            f.write(content)
    else:
        with open(output_path, 'wb') as f:
            f.write(content)

    compressed_size = output_path.stat().st_size
    return len(content), compressed_size


def compute_sha1(file_path: Path) -> str:
    """Compute SHA1 hash of a file."""
    sha1 = hashlib.sha1()
    with open(file_path, 'rb') as f:
        while chunk := f.read(8192):
            sha1.update(chunk)
    return sha1.hexdigest()


def main():
    parser = argparse.ArgumentParser(description='Generate benchmark log files')
    parser.add_argument('--output-dir', '-o', type=Path, default=Path('logs'),
                        help='Output directory for log files')
    parser.add_argument('--small', action='store_true',
                        help='Generate smaller files for testing (10 MB each)')
    args = parser.parse_args()

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    # Target size per file (compressed MB)
    if args.small:
        target_sizes = [10] * 10  # 10 MB each for testing
    else:
        target_sizes = [150, 200, 100, 180, 220, 130, 170, 250, 140, 300]

    # File configurations: (name, type, seed_offset)
    files_config = [
        ('fastapi_server_1.log.xz', 'fastapi', 0),
        ('fastapi_server_2.log.xz', 'fastapi', 1),
        ('user_service.log.xz', 'microservice', 2),
        ('order_service.log.xz', 'microservice', 3),
        ('payment_service.log.xz', 'microservice', 4),
        ('auth_service.log.xz', 'microservice', 5),
        ('gateway.log.xz', 'microservice', 6),
        ('mongodb_primary.log.xz', 'mongodb', 7),
        ('mongodb_secondary.log.xz', 'mongodb', 8),
        ('analytics_service.log.xz', 'microservice', 9),
    ]

    results = []
    total_uncompressed = 0
    total_compressed = 0

    print(f"Generating {len(files_config)} log files...")
    print(f"Output directory: {output_dir.absolute()}")
    print()

    for (filename, file_type, seed_offset), target_mb in zip(files_config, target_sizes):
        output_path = output_dir / filename
        print(f"Generating {filename} ({file_type}, target ~{target_mb} MB compressed)...")

        uncompressed, compressed = generate_log_file(
            output_path, file_type, seed_offset, target_mb
        )

        sha1 = compute_sha1(output_path)

        results.append({
            'filename': filename,
            'type': file_type,
            'uncompressed_bytes': uncompressed,
            'compressed_bytes': compressed,
            'sha1': sha1,
        })

        total_uncompressed += uncompressed
        total_compressed += compressed

        print(f"  Uncompressed: {uncompressed / 1024 / 1024:.1f} MB")
        print(f"  Compressed:   {compressed / 1024 / 1024:.1f} MB")
        print(f"  Ratio:        {compressed / uncompressed * 100:.1f}%")
        print(f"  SHA1:         {sha1}")
        print()

    print("=" * 60)
    print(f"Total uncompressed: {total_uncompressed / 1024 / 1024:.1f} MB")
    print(f"Total compressed:   {total_compressed / 1024 / 1024:.1f} MB")
    print(f"Overall ratio:      {total_compressed / total_uncompressed * 100:.1f}%")

    # Write SHA1 hashes to file
    hashes_file = output_dir / 'sha1sums.txt'
    with open(hashes_file, 'w') as f:
        for r in results:
            f.write(f"{r['sha1']}  {r['filename']}\n")
    print(f"\nSHA1 hashes written to: {hashes_file}")

    # Write detailed manifest
    manifest_file = output_dir / 'manifest.json'
    with open(manifest_file, 'w') as f:
        json.dump({
            'generated_at': datetime.now().isoformat(),
            'random_seed': RANDOM_SEED,
            'files': results,
            'total_uncompressed_bytes': total_uncompressed,
            'total_compressed_bytes': total_compressed,
        }, f, indent=2)
    print(f"Manifest written to: {manifest_file}")


if __name__ == '__main__':
    main()
