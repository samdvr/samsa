# Samsa CLI Documentation

The Samsa CLI provides a comprehensive command-line interface for interacting with the Samsa streaming storage system. It connects directly to any node in the cluster.

## Installation

Build the CLI from source:

```bash
cargo build --bin samsa-cli --release
```

The binary will be available at `target/release/samsa-cli`.

## Global Options

- `-a, --address <ADDRESS>`: Node service address (default: http://127.0.0.1:50052)
- `-h, --help`: Print help information
- `-V, --version`: Print version information

## Usage

```bash
samsa-cli [OPTIONS] <COMMAND>
```

## Commands

### Bucket Management

#### List Buckets

```bash
samsa-cli bucket list [OPTIONS]
```

Options:

- `-p, --prefix <PREFIX>`: Filter by prefix
- `--start-after <BUCKET>`: Start after this bucket for pagination
- `-l, --limit <LIMIT>`: Limit number of results

Example:

```bash
samsa-cli bucket list --prefix "test" --limit 10
```

#### Create Bucket

```bash
samsa-cli bucket create [OPTIONS] <NAME>
```

Options:

- `--auto-create-on-append`: Automatically create streams on append
- `--auto-create-on-read`: Automatically create streams on read

Example:

```bash
samsa-cli bucket create my-bucket --auto-create-on-append
```

#### Delete Bucket

```bash
samsa-cli bucket delete <NAME>
```

#### Get Bucket Configuration

```bash
samsa-cli bucket config <NAME>
```

### Stream Management

All stream commands require specifying the bucket with `-b, --bucket <BUCKET>`.

#### List Streams

```bash
samsa-cli stream -b <BUCKET> list [OPTIONS]
```

Options:

- `-p, --prefix <PREFIX>`: Filter by prefix
- `--start-after <STREAM>`: Start after this stream for pagination
- `-l, --limit <LIMIT>`: Limit number of results

Example:

```bash
samsa-cli stream -b my-bucket list --prefix "events"
```

#### Create Stream

```bash
samsa-cli stream -b <BUCKET> create [OPTIONS] <NAME>
```

Options:

- `--storage-class <CLASS>`: Storage class (0=unspecified, 1=standard, 2=cold) [default: 1]
- `--retention-age <SECONDS>`: Retention age in seconds
- `--timestamping-mode <MODE>`: Timestamping mode (0=unspecified, 1=client, 2=server, 3=arrival) [default: 3]
- `--allow-future`: Allow future timestamps

Example:

```bash
samsa-cli stream -b my-bucket create events --retention-age 86400 --storage-class 1
```

#### Delete Stream

```bash
samsa-cli stream -b <BUCKET> delete <NAME>
```

#### Get Stream Configuration

```bash
samsa-cli stream -b <BUCKET> config <NAME>
```

#### Check Stream Tail

```bash
samsa-cli stream -b <BUCKET> tail <NAME>
```

### Data Operations

All data commands require specifying both bucket (`-b, --bucket`) and stream (`-s, --stream`).

#### Append Records

##### Append a Single Record

```bash
samsa-cli data -b <BUCKET> -s <STREAM> append --body "Hello, World!"
```

##### Append with Custom Timestamp

```bash
samsa-cli data -b my-bucket -s events append --body "Event data" --timestamp 1234567890000
```

##### Append from stdin (JSON format)

```bash
echo '{"body": "Record 1"}' | samsa-cli data -b my-bucket -s events append --from-stdin
```

Multiple records from stdin:

```bash
cat << EOF | samsa-cli data -b my-bucket -s events append --from-stdin
{"body": "Record 1", "timestamp": 1234567890000}
{"body": "Record 2"}
Raw text record
EOF
```

#### Read Records

##### Read from Beginning

```bash
samsa-cli data -b <BUCKET> -s <STREAM> read
```

##### Read with Limit

```bash
samsa-cli data -b my-bucket -s events read --limit 10
```

##### Read from Specific Sequence ID

```bash
samsa-cli data -b my-bucket -s events read --start-seq-id "01234567-89ab-cdef-0123-456789abcdef"
```

##### Read from Timestamp

```bash
samsa-cli data -b my-bucket -s events read --start-timestamp 1234567890000
```

##### Read from Tail Offset

```bash
samsa-cli data -b my-bucket -s events read --tail-offset 100
```

##### Control Output Format

```bash
# JSON format (default)
samsa-cli data -b my-bucket -s events read --format json

# Raw format
samsa-cli data -b my-bucket -s events read --format raw
```

#### Subscribe to Stream (Real-time)

##### Subscribe from Tail

```bash
samsa-cli data -b <BUCKET> -s <STREAM> subscribe
```

##### Subscribe with Heartbeats

```bash
samsa-cli data -b my-bucket -s events subscribe --heartbeats
```

##### Subscribe from Specific Point

```bash
samsa-cli data -b my-bucket -s events subscribe --start-seq-id "01234567-89ab-cdef-0123-456789abcdef"
```

##### Subscribe from Timestamp

```bash
samsa-cli data -b my-bucket -s events subscribe --start-timestamp 1234567890000
```

### Access Token Management

#### Issue Access Token

```bash
samsa-cli token issue [OPTIONS]
```

Options:

- `-e, --expires-in <SECONDS>`: Expiration time in seconds from now

Example:

```bash
samsa-cli token issue --expires-in 3600  # Expires in 1 hour
```

#### Revoke Access Token

```bash
samsa-cli token revoke <ID>
```

#### List Access Tokens

```bash
samsa-cli token list [OPTIONS]
```

Options:

- `-p, --prefix <PREFIX>`: Filter by prefix
- `--start-after <TOKEN>`: Start after this token for pagination
- `-l, --limit <LIMIT>`: Limit number of results

## Examples

### Complete Workflow Example

```bash
# 1. Create a bucket
samsa-cli bucket create logs --auto-create-on-append

# 2. Create a stream
samsa-cli stream -b logs create application-events --retention-age 604800

# 3. Append some data
samsa-cli data -b logs -s application-events append --body "Application started"
samsa-cli data -b logs -s application-events append --body "User login: john@example.com"

# 4. Read the data back
samsa-cli data -b logs -s application-events read --format json

# 5. Subscribe to real-time updates (in another terminal)
samsa-cli data -b logs -s application-events subscribe --heartbeats
```

### Batch Data Import

```bash
# Prepare data file (data.jsonl)
cat << EOF > data.jsonl
{"body": "Event 1", "timestamp": 1700000000000}
{"body": "Event 2", "timestamp": 1700000001000}
{"body": "Event 3", "timestamp": 1700000002000}
EOF

# Import the data
cat data.jsonl | samsa-cli data -b my-bucket -s events append --from-stdin
```

### Stream Statistics

```bash
# Check stream tail to see latest position
samsa-cli stream -b my-bucket tail events

# Read recent records
samsa-cli data -b my-bucket -s events read --tail-offset 10
```

## Output Formats

### JSON Format

When using `--format json`, records are output as JSON objects:

```json
{
  "seq_id": "01234567-89ab-cdef-0123-456789abcdef",
  "timestamp": 1700000000000,
  "body": "Hello, World!",
  "headers": []
}
```

### Raw Format

When using `--format raw`, records are output in a human-readable format:

```
Record 1:
  Seq ID: 01234567-89ab-cdef-0123-456789abcdef
  Timestamp: 1700000000000
  Body: Hello, World!
```

## Error Handling

The CLI will exit with a non-zero status code on errors and print error messages to stderr. Common errors include:

- **Connection refused**: Node service is not running
- **Invalid argument**: Check command syntax and required parameters
- **Not found**: Bucket or stream doesn't exist
- **Already exists**: Resource already exists

## Environment Variables

- `Samsa_NODE_ADDRESS`: Default node address (overrides default, can be overridden by `--address`)

## Tips

1. **JSON Input**: When using `--from-stdin`, each line should be a valid JSON object or plain text
2. **Timestamps**: All timestamps are in milliseconds since Unix epoch
3. **Sequence IDs**: Use UUID v7 format for sequence IDs
4. **Pagination**: Use `--start-after` with the last item from previous page for pagination
5. **Real-time**: Use `subscribe` for real-time data consumption, `read` for batch processing

## Advanced Usage

### Batch Operations

```bash
# Append multiple records from a file
cat data.jsonl | samsa-cli data -b my-bucket -s my-stream append --from-stdin

# Read with pagination
samsa-cli data -b my-bucket -s my-stream read --limit 100 --start-seq-id "01234567-89ab-cdef-0123-456789abcdef"
```

### Custom Node Address

```bash
samsa-cli -a "http://production-node:50052" bucket list
```

### Large Data Operations

```bash
# Read large amounts of data with byte limits
samsa-cli data -b logs -s events read --limit 1000 --max-bytes 1048576
```

### Continuous Monitoring

```bash
# Monitor a stream continuously with JSON output for processing
samsa-cli data -b logs -s events subscribe --format json | jq '.body'
```
