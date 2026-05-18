# Snowflake + Iceberg Interoperability Quickstart

Build Snowflake-managed Iceberg tables, stream live data via Snowpipe Streaming V2, and read from Databricks through Snowflake's Horizon Iceberg REST Catalog — with governance enforced across engine boundaries.

## What You Will Build

- **Snowflake-managed Iceberg tables** — NYC taxi trips and weather data stored as open Apache Iceberg (Parquet + metadata), managed entirely by Snowflake with zero bucket configuration
- **Real-time streaming ingest** — A Java application that calls the Open-Meteo weather API and streams rows into an Iceberg V3 VARIANT table via Snowpipe Streaming V2 (SSV2)
- **Multi-engine reads from Databricks** — Databricks reads the same Iceberg tables through Snowflake's Horizon Iceberg REST Catalog (IRC) endpoint
- **Governance across engine boundaries** — Column masking and row-level security (RLS) policies applied in Snowflake are enforced when Databricks reads via Horizon
- **Iceberg V3 VARIANT** — Semi-structured JSON data stored natively in Iceberg V3 format and queried from both Snowflake (dot notation) and Spark 4.0 (`variant_get`)

### Architecture

![End-to-End Data Flow](arch.png)

### Consumer Layer — External + Internal AI

```mermaid
flowchart LR
    subgraph SOURCES["Data Sources"]
        API[Open-Meteo API]
        S3[External Stage<br/>s3://sfquickstarts]
    end

    subgraph INGEST["Ingestion"]
        SSV2[Snowpipe Streaming V2]
        CTAS[SSI CTAS<br/>CATALOG = SNOWFLAKE]
    end

    subgraph ICEBERG["Iceberg Tables"]
        WT[nyc_weather_ssv2<br/>V3 VARIANT]
        YT[yellow_trips<br/>Path A — no policy]
        GT[green_trips<br/>Path B — masking + RLS]
        ZL[zone_lookup]
    end

    subgraph CATALOG["Horizon Catalog"]
        IRC[Iceberg REST Catalog]
        GOV[Governance Policies<br/>masking • RLS]
    end

    subgraph EXTERNAL["External Consumers"]
        DBX_A[Databricks Path A<br/>Direct Parquet • 0 SF credits]
        DBX_B[Databricks Path B<br/>Governed • SF compute]
    end

    subgraph INTERNAL["Internal AI Consumers"]
        SV[Semantic View<br/>nyc_taxi_analytics]
        AGENT[Cortex Agent<br/>nyc_taxi_agent]
        SI[Snowflake Intelligence<br/>Natural Language → SQL]
    end

    API --> SSV2 --> WT
    S3 --> CTAS --> YT & GT & ZL

    YT --> IRC --> DBX_A
    GT --> IRC --> GOV --> DBX_B

    YT & ZL --> SV --> AGENT --> SI
```

## Prerequisites

| Requirement | Details |
|---|---|
| Snowflake account | Enterprise edition or higher (governance features require Enterprise+) |
| Snowflake role | ACCOUNTADMIN or a role with equivalent privileges |
| Databricks workspace | With access to create clusters (DBR 17.3, Spark 4.0) |
| Java JDK | Version 11 or higher — verify: `java -version` |
| Apache Maven | Version 3.6 or higher — verify: `mvn -version` |
| OpenSSL | For RSA key pair generation — verify: `openssl version` |

## Setup

### Authentication Overview

This quickstart uses **two** authentication methods. Understanding why avoids confusion later:

| Auth Method | Used By | Why |
|---|---|---|
| **PAT** (Programmatic Access Token) | Horizon IRC catalog credential (Databricks SparkSession) | The Iceberg REST Catalog spec uses OAuth bearer tokens. A PAT serves as that bearer token. |
| **RSA key-pair** | Spark Connector JDBC fallback (Databricks `spark.snowflake.*` config) | Path B tables (with masking/RLS) are routed through JDBC. JDBC does **not** accept PATs — key-pair auth is required. |
| **RSA key-pair** | Java SSV2 ingest app (`ssv2-streaming/profile.json`) | The Snowpipe Streaming SDK authenticates via key-pair, not PAT. |

**In short:** PAT handles catalog-level access (discovering and listing tables). Key-pair handles compute-level access (JDBC queries and streaming ingest).

You will set up the RSA key pair first (Step 1 below), then create the PAT later in Module 3.

### 1. Generate an RSA Key Pair

The RSA key pair is used by both the Spark Connector (Path B governance routing) and the Java SSV2 ingest app.

```bash
# Generate a 2048-bit RSA private key in PKCS#8 format (no passphrase)
openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out rsa_key.p8

# Extract the public key
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

You will register the public key on your Snowflake user in Module 0 (`00_setup.sql`), and paste the private key (base64, no PEM headers) into:
- `ssv2-streaming/profile.json` — for the Java SSV2 ingest app
- `scripts/04_databricks_read.ipynb` cell 1 — for the Databricks Spark Connector

To extract the private key as a single base64 line (no PEM headers):
```bash
grep -v "BEGIN\|END" rsa_key.p8 | tr -d '\n'
```

### 2. Configure `profile.json`

The Java SSV2 streaming application reads credentials from `ssv2-streaming/profile.json`.

```bash
cd ssv2-streaming
cp profile.json.example profile.json
```

Edit `profile.json` and fill in your values:

| Field | How to find it |
|---|---|
| `user` | Your Snowflake username |
| `account` | Run: `SELECT CURRENT_ORGANIZATION_NAME() \|\| '-' \|\| CURRENT_ACCOUNT_NAME()` |
| `url` | `https://<account_identifier>.snowflakecomputing.com:443` |
| `private_key` | Base64 content of `rsa_key.p8` (no PEM headers, no newlines) |
| `role` | Leave as `DEMO_ADMIN` (created in Module 0) |

### 3. Register Your Public Key in Snowflake

In `scripts/00_setup.sql`, Section 4b contains the `ALTER USER` command to register your public key. Copy the single-line content of `rsa_key.pub` (strip the `-----BEGIN PUBLIC KEY-----` and `-----END PUBLIC KEY-----` headers) and paste it as the `RSA_PUBLIC_KEY` value.

## Quickstart Modules

Run the modules in order. Each script includes inline comments explaining what each statement does and what output you should expect.

| Module | Script | What You Will Do |
|---|---|---|
| 0 | [scripts/00_setup.sql](scripts/00_setup.sql) | Create roles, warehouses, database, external stage, network rules, and Iceberg V3 defaults |
| 1 | [scripts/01_create_tables.sql](scripts/01_create_tables.sql) | Create Iceberg tables from NYC taxi trip Parquet files (yellow + green) and a zone lookup |
| 2 | [scripts/02_ssv2_streaming_setup.sql](scripts/02_ssv2_streaming_setup.sql) | Create the SSV2 weather table + pipe, run the Java ingest app, explore VARIANT queries |
| 3 | [scripts/03_horizon_governance.sql](scripts/03_horizon_governance.sql) | Apply masking + RLS policies (Path B), generate a PAT for Databricks, verify Path A/B setup |
| 4 | [scripts/04_databricks_read.ipynb](scripts/04_databricks_read.ipynb) | Read all tables from Databricks via Horizon IRC — observe Path A (direct) vs Path B (governed) |
| 5 | [scripts/05_semantic_view.sql](scripts/05_semantic_view.sql) | Create a semantic view over Iceberg tables, build a Cortex Agent, test with Snowflake Intelligence |
| 6 (Optional) | [scripts/06_cortex_code_prompts.sql](scripts/06_cortex_code_prompts.sql) | Curated Cortex Code prompts — explore the entire quickstart with AI assistance |

### Running the Java SSV2 Ingest (Module 2)

After running `02_ssv2_streaming_setup.sql` to create the table and pipe:

```bash
cd ssv2-streaming
mvn package
mvn exec:java -Dexec.mainClass="com.snowflake.snowpipestreaming.demo.SSV2WeatherIngest"
```

The application fetches weather data from the Open-Meteo API for three NYC airports (JFK, LGA, EWR) and streams it into Snowflake via Snowpipe Streaming V2. You should see output like:

```
Streamed 36 rows.
All 36 rows committed to nyc_weather_ssv2.
```

### Setting Up Databricks (Module 4)

See [DATABRICKS_SETUP.md](DATABRICKS_SETUP.md) for detailed cluster configuration, Maven library installation, and troubleshooting.

## Cleanup

Two teardown scripts are provided:

| Script | What It Does |
|---|---|
| [scripts/teardown_governance.sql](scripts/teardown_governance.sql) | Removes policies, tags, and grants only. Tables and data are preserved. Use this to re-run Module 3. |
| [scripts/teardown_full.sql](scripts/teardown_full.sql) | Drops all tables, pipes, policies, and tags. Preserves database, warehouse, roles, and stage. Use this to rebuild from Module 1. |

## Folder Structure

```
sfguide-snowflake-iceberg-interoperability/
├── README.md                       ← You are here
├── DATABRICKS_SETUP.md             ← Databricks cluster configuration guide
├── EliminateStorageAndInteropTax.pdf ← Strategic narrative — what taxes are eliminated
├── LICENSE                         ← Apache 2.0
├── .gitignore                      ← Ignores secrets and build artifacts
├── scripts/
│   ├── 00_setup.sql                ← Module 0: environment setup
│   ├── 01_create_tables.sql        ← Module 1: Iceberg table creation
│   ├── 02_ssv2_streaming_setup.sql ← Module 2: SSV2 streaming + VARIANT
│   ├── 03_horizon_governance.sql   ← Module 3: governance (Path A / Path B)
│   ├── 04_databricks_read.ipynb    ← Module 4: Databricks multi-engine read
│   ├── 05_semantic_view.sql        ← Module 5: Semantic View + Cortex Agent + RBAC demo
│   ├── 06_cortex_code_prompts.sql  ← Module 6 (Optional): CoCo prompts for all modules
│   ├── teardown_governance.sql     ← Teardown: governance objects only
│   └── teardown_full.sql           ← Teardown: full reset
└── ssv2-streaming/
    ├── pom.xml                     ← Maven project (Java 11)
    ├── profile.json.example        ← Credential template (copy to profile.json)
    ├── rsa_key.p8.example          ← RSA private key placeholder
    └── src/main/java/com/snowflake/snowpipestreaming/demo/
        └── SSV2WeatherIngest.java  ← Java SSV2 weather ingest application
```

## Key Concepts

### Snowflake Storage for Iceberg (SSI)
All Iceberg tables in this quickstart use `CATALOG = SNOWFLAKE` and `EXTERNAL_VOLUME = SNOWFLAKE_MANAGED`. This means Snowflake manages the underlying storage (Parquet data files + Iceberg metadata) internally — no customer S3/GCS/ADLS bucket required.

### Horizon Iceberg REST Catalog (IRC)
Snowflake's built-in catalog endpoint that implements the open-source Apache Iceberg REST Catalog specification. External engines connect to this endpoint to discover and read Snowflake-managed Iceberg tables.

### Path A vs Path B
- **Path A** (yellow_trips): No governance policies. Horizon vends temporary STS credentials so Databricks reads Parquet directly from storage. Zero Snowflake compute consumed.
- **Path B** (green_trips): Masking + RLS policies attached. Horizon routes the query through Snowflake compute for server-side policy enforcement. Databricks receives only masked/filtered results.

### Iceberg V3 VARIANT
Iceberg format version 3 adds native support for complex/semi-structured types. The `nyc_weather_ssv2` table stores full JSON API responses as VARIANT, queryable from both Snowflake (dot notation) and Spark 4.0 (`variant_get`).

### Semantic View (nyc_taxi_analytics)
A schema-level object that defines business dimensions, metrics, synonyms, and verified queries over the physical Iceberg tables. It is the "contract" between raw data and AI consumers — Cortex Analyst reads the semantic view to translate natural language into SQL. The SQL DDL and equivalent YAML representation are both shown in Module 5.

### Cortex Agent (nyc_taxi_agent)
Wraps the semantic view and exposes it via Snowflake Intelligence. Users ask questions in plain English; the agent routes to Cortex Analyst, which generates SQL grounded by the semantic view's definitions and VQRs (Verified Query Representations).

## What This Architecture Eliminates

This quickstart demonstrates how Snowflake eliminates three taxes that traditionally plague multi-engine architectures:

| Tax | Traditional Pain | What Snowflake Eliminates |
|---|---|---|
| **Storage Tax** | Separate copies for each engine (Snowflake tables + S3 Parquet for Spark) | Single Iceberg copy, Snowflake-managed, readable by any engine |
| **Interoperability Tax** | Custom ETL to expose data to Spark; governance bypassed outside Snowflake | Horizon IRC + Open APIs serve Iceberg metadata; FGAC enforced server-side even on Spark |
| **AI Access Tax** | Data engineers write bespoke APIs/views for every BI/AI consumer | Semantic view defines meaning once; Cortex Agent + Intelligence serve any business user |

![Eliminate the Storage and Interoperability Tax](EliminateStorageAndInteropTax.png)

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
