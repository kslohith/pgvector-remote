# pgvector-remote

[introduction](https://medium.com/@sweta.vooda/pgvector-remote-a-pgvector-fork-with-the-performance-of-pinecone-5d8a7f6a50bd)

pgvector-remote is a fork of pgvector which combines the simplicity of [pgvector](https://github.com/pgvector/pgvector)
with the power of remote vector databases, by introducing a new remote vector index type. Currently, pgvector-remote only supports [pinecone]("https://www.pinecone.io/")
, but we plan to support other vendors in the future.
- [Short Version](#short-version)
- [Use Cases](#use-cases)
- [Installation](#installation)
- [Configuration](#configuration)
- [Index Creation](#index-creation)
- [Performance Considerations](#performance-considerations)
- [Docker](#docker)
- [Credits](#credits)


![image](https://github.com/georgia-tech-db/pgvector-remote/assets/66109536/707eb52e-50f5-4675-8e66-d0a97066afd0)
*Benchmarks for a 10M filtered search workload. https://big-ann-benchmarks.com/neurips23.html#tracks. Results for pgvector are shown for a tuned hnsw index on a t2.2xlarge (32GB RAM). Results for pinecone are for a p2.x8 pod.*

## Short Version

```sql
CREATE TABLE products (name text, embedding vector(1536), price float);
CREATE INDEX my_remote_index ON products USING remote (embedding, price) with (host = 'my-pinecone-index.pinecone.io');
-- [insert, update, and delete billions of records in products]
SELECT * FROM products WHERE price < 40.0 ORDER BY embedding <-> '[...]' LIMIT 10; -- pinecone performs this query, including the price predicate

```

## Use Cases
### Benefits of using pgvector-remote for pinecone users

- Vector databases like pinecone aren't docstores (and they shouldn't try to be). That means your document and its embedding live in separate databases. pgvector-remote lets you keep your metadata in postgres and your embeddings in pinecone, while hiding this complexity from the user by presenting a unified sql interface to creating, querying, and updating pinecone indexes.
- Control your data. Using pgvector-remote means that all your vectors are in postgres. This makes it easy to test out a different index type (like hnsw) and drop pinecone in favor of a different vendor.

### Benefits of using pinecone for pgvector users
- **Scalability**: Pinecone is designed to scale to billions of vectors. pgvector does not easily accomodate such large datasets. Large vector indexes are incredibly highly memory intensive and therefore it makes sense to separate this from the main database. For example indexing 200M vectors of 1536 dimensions would require 1.2TB of memory.

### Benefits of using pgvector-remote for users who already use pinecone and pgvector
- **Seamless integration**: You don't need to write a line of pinecone application logic. Use a unified sql interface to leverage pinecone as if it were any other postgres index type.
- **Synchronization**: pgvector-remote ensures that the data in pinecone and postgres are always in sync. For example, if your postgres transaction rolls back you don't need to worry about cleaning up the data in pinecone.

### Why is this integration better than [confluent's kafka-connect](https://www.pinecone.io/confluent-integration/)?
- **Liveness and correctness**: pgvector-remote sends inserted vectors to pinecone in batches and locally scans unflushed records, guaranteeing that all data is always visible to index queries.
- **Query and integration logic**: traditional ETL won't help you write queries like the one above. pgvector-remote translates select predicates to pinecone filters.

### When should I just use pgvector?
- **Small datasets**: If you have a small to medium dataset (10M vectors at 768 dimensions), you can use pgvector without a remote vector store. The local hnsw indexes will be sufficient.
- **Minimal metadata**: You aren't performing metadata filtering. Currently, pgvector does not handle metadata filtering, meaning that queries like the one above can sometimes be inefficient and inaccurate.

## Installation


Follow the [installation instructions for pgvector](https://github.com/pgvector/pgvector?tab=readme-ov-file#installation-notes---linux-and-mac)
You can specify which remote providers to install using MAKEFLAGS. For example,
```sh
make USE_PINECONE=1 USE_MILVUS=1
sudo make install
```

### Pinecone Installation
Install libcurl headers. For example,
```sh
sudo apt-get install libcurl4-openssl-dev
```

### Milvus Installation
- build the milvus c++ sdk
- `git clone https://github.com/oscarlaird/milvus-sdk-cpp`
- `sudo apt install libgrpc++-dev libgrpc-dev libprotobuf-dev`
- `git submodule update --init`
- `make && sudo make install`



## Configuration

Set the pinecone API key in the postgres configuration. For example,
```sql
ALTER DATABASE mydb SET pinecone.api_key = 'xxxxxxxx-xxxx-xxxx-xxxx–xxxxxxxxxxxx';
```

## Index Creation

There are two ways to specify the pinecone index:
- By providing the host of an existing pinecone index. For example,
```sql
CREATE INDEX my_remote_index ON products USING remote (embedding) with (host = 'example-23kshha.svc.us-east-1-aws.pinecone.io');
```
- By specifying the `spec` of the pinecone index. For example,
```sql
CREATE INDEX my_remote_index ON products USING remote (embedding) with (spec = '{
        "serverless": {
            "region": "us-west-2",
            "cloud": "aws"
        }
    }');
```
All spec options can be found [here](https://docs.pinecone.io/reference/api/control-plane/create_index)

## Performance Considerations

- Place your pinecone index in the same region as your postgres instance to minimize latency.
- Make use of connection pooling to run queries in postgres concurrently. For example, use `asyncpg` in python.
- Records are sent to the remote index in batches. Therefore pgvector-remote performs a local scan of the unflushed records before every query. To disable this set `pinecone.max_buffer_scan` to 0. For example,
```sql
ALTER DATABASE mydb SET pinecone.max_buffer_scan = 0;
```
- You can adjust the number of vectors sent in each request and the number of concurrent requests per batch using `pinecone.vectors_per_request` and `pinecone.requests_per_batch` respectively. For example,
```sql
ALTER DATABASE mydb SET pinecone.vectors_per_request = 100; --default
ALTER DATABASE mydb SET pinecone.requests_per_batch = 40; --default
```
- You can control the number of results returned by pinecone using `pinecone.top_k`. Lowering this parameter can decrease latencies, but keep in mind that setting this too low could cause fewer results to be returned than expected.

## Docker

An example docker image can be obtained with,

```sh
docker pull kslohith17/pgvector-remote:latest
```

This contains postgres along with pgvector-remote configured to run on it.

## Credits

We give special thanks to these projects, which enabled us to develop our extension:

- [pgvector: Open-source vector similarity search for Postgres](https://github.com/pgvector/pgvector)
- [PASE: PostgreSQL Ultra-High-Dimensional Approximate Nearest Neighbor Search Extension](https://dl.acm.org/doi/pdf/10.1145/3318464.3386131)
- [Faiss: A Library for Efficient Similarity Search and Clustering of Dense Vectors](https://github.com/facebookresearch/faiss)
- [Using the Triangle Inequality to Accelerate k-means](https://cdn.aaai.org/ICML/2003/ICML03-022.pdf)
- [k-means++: The Advantage of Careful Seeding](https://theory.stanford.edu/~sergei/papers/kMeansPP-soda.pdf)
- [Concept Decompositions for Large Sparse Text Data using Clustering](https://www.cs.utexas.edu/users/inderjit/public_papers/concept_mlj.pdf)
- [Efficient and Robust Approximate Nearest Neighbor Search using Hierarchical Navigable Small World Graphs](https://arxiv.org/ftp/arxiv/papers/1603/1603.09320.pdf)
- [Pinecone: Vector database and search service designed for real-time applications](https://docs.pinecone.io/introduction)
