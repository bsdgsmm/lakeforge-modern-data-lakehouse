# Data directory

Everything in this folder except this file is generated and ignored by Git.

| Path | Written by | Contents |
| --- | --- | --- |
| `landing/<entity>/batch_date=YYYY-MM-DD/` | `lakeforge generate` | Raw CSV and JSON Lines extracts, one folder per daily batch |
| `bronze/<entity>/_batch_date=.../` | Bronze | Raw rows as Parquet with ingestion metadata |
| `silver/<entity>/` | Silver | Typed, validated, deduplicated entities |
| `quarantine/<entity>/_batch_date=.../` | Silver | Rejected rows with `_rejection_reasons` |
| `gold/<table>/` | Gold | Star schema and aggregates |
| `_state/manifest.json` | Pipeline | Processed batches and per-batch quality metrics |
| `_staging/` | Pipeline | Temporary write location, empty after a successful run |

All records are **synthetic**. They are produced by a seeded generator for a fictional
marketplace and do not describe real people, products or companies. Email addresses use
the reserved `example.com` / `example.org` domains.
