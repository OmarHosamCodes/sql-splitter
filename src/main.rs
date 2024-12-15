use clap::Parser;
use futures::StreamExt;
use std::path::{Path, PathBuf};
use tokio::fs::{self, File};
use tokio::io::{AsyncWriteExt, BufWriter};

#[derive(Parser, Debug)]
#[command(
    name = "sql-splitter",
    about = "Split large SQL files into smaller ones while preserving statement integrity",
    version
)]
struct Args {
    /// Input SQL file path
    #[arg(short, long)]
    input: PathBuf,

    /// Output directory for split files
    #[arg(short, long)]
    output_dir: PathBuf,

    /// Maximum size of each split file in kilobytes
    #[arg(short, long, default_value = "1000")]
    max_size_kb: usize,

    /// Number of concurrent write operations
    #[arg(short, long, default_value = "4")]
    concurrent_writes: usize,
}

#[derive(Debug)]
struct SqlSplitter {
    max_size_kb: usize,
    output_dir: PathBuf,
    concurrent_writes: usize,
}

impl SqlSplitter {
    pub fn new<P: AsRef<Path>>(
        output_dir: P,
        max_size_kb: usize,
        concurrent_writes: usize,
    ) -> Self {
        SqlSplitter {
            max_size_kb,
            output_dir: output_dir.as_ref().to_path_buf(),
            concurrent_writes,
        }
    }

    fn split_statements(content: &str) -> Vec<String> {
        let mut statements = Vec::new();
        let mut current_statement = String::new();
        let mut in_string = false;
        let mut escape_next = false;

        for c in content.chars() {
            match c {
                '\\' if in_string => {
                    current_statement.push(c);
                    escape_next = !escape_next;
                }
                '\'' if !escape_next => {
                    current_statement.push(c);
                    in_string = !in_string;
                }
                ';' if !in_string => {
                    current_statement = current_statement.trim().to_string();
                    if !current_statement.is_empty() {
                        statements.push(current_statement);
                    }
                    current_statement = String::new();
                }
                _ => {
                    if c == '\'' {
                        escape_next = false;
                    }
                    current_statement.push(c);
                }
            }
        }

        // Add the last statement if it doesn't end with a semicolon
        let final_statement = current_statement.trim().to_string();
        if !final_statement.is_empty() {
            statements.push(final_statement);
        }

        statements
    }

    async fn write_sql_file(
        statements: Vec<String>,
        output_path: PathBuf,
    ) -> Result<(), std::io::Error> {
        let file = File::create(output_path).await?;
        let mut writer = BufWriter::new(file);

        for (i, statement) in statements.iter().enumerate() {
            if i > 0 {
                writer.write_all(b"\n\n").await?;
            }
            writer.write_all(statement.as_bytes()).await?;
            writer.write_all(b";").await?;
        }
        writer.flush().await?;
        Ok(())
    }

    async fn split_file(&self, input_file: impl AsRef<Path>) -> Result<usize, std::io::Error> {
        // Create output directory if it doesn't exist
        fs::create_dir_all(&self.output_dir).await?;

        // Read the entire file content
        let content = fs::read_to_string(input_file).await?;
        let statements = Self::split_statements(&content);
        let max_size = self.max_size_kb * 1024;

        let mut batches = Vec::new();
        let mut current_batch = Vec::new();
        let mut current_size = 0;

        for statement in statements {
            let statement = statement.trim().to_string();
            let statement_size = statement.as_bytes().len() + 1; // +1 for semicolon

            if current_size + statement_size > max_size && !current_batch.is_empty() {
                batches.push(current_batch);
                current_batch = Vec::new();
                current_size = 0;
            }

            current_batch.push(statement);
            current_size += statement_size;
        }

        if !current_batch.is_empty() {
            batches.push(current_batch);
        }

        // Process batches concurrently with limited parallelism
        let mut futures = futures::stream::iter(
            batches
                .into_iter()
                .enumerate()
                .map(|(i, batch)| {
                    let output_path = self.output_dir.join(format!("split_{:03}.sql", i + 1));
                    Self::write_sql_file(batch, output_path)
                })
                .collect::<Vec<_>>(),
        )
        .buffer_unordered(self.concurrent_writes);

        let mut file_count = 0;
        while let Some(result) = futures.next().await {
            result?;
            file_count += 1;
        }

        Ok(file_count)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let splitter = SqlSplitter::new(args.output_dir, args.max_size_kb, args.concurrent_writes);

    println!("Starting to split SQL file...");
    let start = std::time::Instant::now();

    match splitter.split_file(args.input).await {
        Ok(num_files) => {
            let duration = start.elapsed();
            println!("Successfully split SQL file into {} files", num_files);
            println!("Time taken: {:.2?}", duration);
        }
        Err(e) => eprintln!("Error splitting file: {}", e),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_split_statements() {
        let input = "SELECT * FROM table1; INSERT INTO table2 VALUES ('test;test'); UPDATE table3 SET col = 1;";
        let statements = SqlSplitter::split_statements(input);
        assert_eq!(statements.len(), 3);
        assert_eq!(
            statements[1].trim(),
            "INSERT INTO table2 VALUES ('test;test')"
        );
    }

    #[tokio::test]
    async fn test_split_statements_with_escaped_quotes() {
        let input = "SELECT 'it\\'s working'; INSERT INTO table2 VALUES ('test');";
        let statements = SqlSplitter::split_statements(input);
        assert_eq!(statements.len(), 2);
        assert_eq!(statements[0].trim(), "SELECT 'it\\'s working'");
    }

    #[tokio::test]
    async fn test_file_splitting() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempdir()?;
        let splitter = SqlSplitter::new(temp_dir.path(), 1, 2);

        // Create a test input file
        let input_path = temp_dir.path().join("input.sql");
        let mut input_file = File::create(&input_path).await?;
        input_file
            .write_all(b"SELECT 1; SELECT 2; SELECT 3;")
            .await?;

        let num_files = splitter.split_file(input_path).await?;
        assert!(num_files > 1);

        Ok(())
    }
}
