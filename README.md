# SQL Splitter

A command-line tool for splitting large SQL files into smaller ones while preserving statement integrity.

## Installation

```bash
cargo install sql-splitter
```

This will make the `sql-split` command available system-wide.

## Usage

```bash
# Basic usage
sql-split -i large_file.sql -o output_dir

# Specify maximum file size (in KB) and concurrent writes
sql-split -i large_file.sql -o output_dir -m 2000 -c 8

# Show help
sql-split --help
```

## Features

- Preserves SQL statement integrity
- Async I/O for better performance
- Concurrent file writing
- Progress indication
- Configurable file size limits

## License

This project is licensed under the MIT License - see the LICENSE file for details.