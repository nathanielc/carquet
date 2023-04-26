# Carquet

This repo explores the answer to the question: What if we use Parquet to store sets of IPLD data objects that have the same or similar schemas?

The code here is a quick hack that has several bugs however it was able to show some space savings gains and that generally Parquests support for nested data works well for IPLD objects.



Using 6 unique schemas (from Ceramic event data) I found the following early results:

| # | Car File Size | Parquet File Size | (Parquet / Car ) |
| - | ------------- | ----------------- | ---------------- |
| 0 | 4.4M          | 909K              | 0.20             |
| 1 | 2.2M          | 1.2M              | 0.55             |
| 2 | 25M           | 18M               | 0.72             |
| 3 | 2.0M          | 458K              | 0.22             |
| 4 | 1.2M          | 264K              | 0.21             |
| 5 | 11M           | 5.6M              | 0.51             |

Note that CAR files do not have any compression and the Parquet file is using Snappy compression.
However the Parquet files are still generally smaller than gzipped car files while still providing seek access to individual objects within the Parquet files.
