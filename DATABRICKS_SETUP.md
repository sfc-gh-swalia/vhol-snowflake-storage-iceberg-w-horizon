# Databricks Cluster Setup Guide

This guide walks you through configuring a Databricks cluster to read Snowflake-managed Iceberg tables via the Horizon Iceberg REST Catalog (IRC).

## Step 1: Create a Cluster

Create a new all-purpose cluster with the following settings:

| Setting | Value | Why |
|---|---|---|
| Databricks Runtime | **17.3** (includes Spark 4.0) | Required for Iceberg V3 VARIANT support via `variant_get` |
| Photon | **Disabled** | Photon conflicts with the Iceberg Spark runtime at this version |
| Access mode | **No isolation shared** | Unity Catalog access modes block direct REST catalog connections |

> **Note:** DBR 17.3 ships with **Scala 2.13**. This affects which Maven artifact coordinates you install in Step 2.

## Step 2: Install Maven Libraries

Navigate to **Compute > Your Cluster > Libraries > Install New > Maven** and install the following three libraries one at a time:

| Maven Coordinate | Purpose |
|---|---|
| `org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1` | Iceberg Spark integration (catalog, table scans, file I/O) |
| `org.apache.iceberg:iceberg-aws-bundle:1.10.1` | S3FileIO for reading Parquet from Snowflake-managed storage |
| `net.snowflake:spark-snowflake_2.13:3.1.6` | Snowflake Connector for Spark (includes SnowflakeFallbackCatalog for Path B governance routing) |

### Important

- **Do NOT install `snowflake-jdbc` separately.** `spark-snowflake_2.13:3.1.6` transitively includes `snowflake-jdbc:3.24.2`. Installing a different JDBC version explicitly causes version conflicts.
- **Use `_2.13` artifacts, NOT `_2.12`.** DBR 17.3 runs Scala 2.13. Using `_2.12` artifacts causes `NoClassDefFoundError: scala/collection/GenTraversableOnce`.

After installing all three libraries, **restart the cluster** to ensure they are loaded.

## Step 3: Upload the Notebook

1. In your Databricks workspace, navigate to the target folder
2. Click **Import** and select `scripts/04_databricks_read.ipynb`
3. Attach the notebook to the cluster you configured in Steps 1-2

## Step 4: Configure Credentials

Edit **cell 1** of the notebook and fill in your values:

```python
ACCOUNT_IDENTIFIER = "<YOUR_ACCOUNT_IDENTIFIER>"  # e.g. "ORGNAME-ACCTNAME"
PAT_TOKEN          = "<YOUR_PAT_TOKEN>"            # From 03_horizon_governance.sql
SF_USER            = "<YOUR_SNOWFLAKE_USER>"       # Your Snowflake username
RSA_PRIVATE_KEY    = "<YOUR_RSA_PRIVATE_KEY_BASE64>" # From rsa_key.p8 (no PEM headers)
REGION             = "us-east-1"                   # Your Snowflake account region
```

### How to find each value

| Field | Source |
|---|---|
| `ACCOUNT_IDENTIFIER` | Run in Snowflake: `SELECT CURRENT_ORGANIZATION_NAME() \|\| '-' \|\| CURRENT_ACCOUNT_NAME()` — use hyphens, not underscores |
| `PAT_TOKEN` | Generated in Module 3 (`03_horizon_governance.sql`, Section 5). This is the Programmatic Access Token scoped to DEMO_DBX_READER. |
| `SF_USER` | Your Snowflake username |
| `RSA_PRIVATE_KEY` | Run: `grep -v "BEGIN\|END" rsa_key.p8 \| tr -d '\n'` to get the base64 key without PEM headers |
| `REGION` | Your Snowflake account's cloud region (e.g., `us-east-1`, `us-west-2`, `eu-west-1`) |

## Step 5: Run the Notebook

Execute cells sequentially. The notebook demonstrates:

1. **SparkSession initialization** with Horizon IRC + SnowflakeFallbackCatalog
2. **Path A read** (yellow_trips) — vended credentials, direct S3 read, zero SF compute
3. **Nanosecond timestamp test** (green_trips_nano) — expected failure showing Spark/Iceberg V3 compatibility
4. **Path B read** (green_trips) — routed through SF compute, masking returns `-1.0`, RLS filters to Manhattan
5. **VARIANT read** (nyc_weather_ssv2) — Iceberg V3 VARIANT queried via `variant_get` in Spark 4.0

## Troubleshooting

### `NoClassDefFoundError: scala/collection/GenTraversableOnce`
**Cause:** You installed the `_2.12` version of the Snowflake Connector or Iceberg runtime.
**Fix:** Uninstall and reinstall with `_2.13` artifacts. DBR 17.3 uses Scala 2.13.

### `403 Forbidden` when reading `green_trips`
**Cause:** This is expected behavior (not an error). `green_trips` has masking + RLS policies, so Horizon refuses to vend storage credentials.
**Fix:** The `SnowflakeFallbackCatalog` automatically detects this and routes the query through Snowflake compute. If you see 403 in logs but the query returns data, everything is working correctly. If the query fails entirely, verify that `spark.snowflake.*` configs are correct in cell 2.

### `timestamp_ns` error reading `green_trips_nano`
**Cause:** This is intentional. `green_trips_nano` uses `TIMESTAMP_NTZ(9)` (nanosecond precision), which Iceberg Spark runtime 1.10.x cannot read.
**Fix:** No fix needed — this is a demonstration of the compatibility limitation. The main tables use `TIMESTAMP_NTZ(6)` (microsecond) which works correctly.

### `spark.jars.packages` has no effect
**Cause:** Databricks pre-initializes the SparkSession at cluster startup. Specifying `spark.jars.packages` in notebook code is a no-op.
**Fix:** All JARs must be installed as cluster-level Maven libraries (Step 2). The `spark.jars.packages` line in the notebook is kept for documentation purposes only.

### `AnalysisException: Table not found`
**Cause:** The Horizon IRC endpoint cannot resolve the table.
**Fix:**
1. Verify the PAT token is valid and not expired (default: 1-day expiry)
2. Verify `DEMO_DBX_READER` has `SELECT` grants on the tables (run Module 3, Section 4)
3. Verify the `ACCOUNT_IDENTIFIER` uses hyphens (`ORG-ACCT`), not underscores

### Connection timeout to Horizon IRC
**Cause:** Network connectivity issue between Databricks and Snowflake.
**Fix:**
1. Verify the `REGION` matches your Snowflake account's region
2. Check that your Snowflake account does not have network policies blocking the Databricks cluster IP
3. Test connectivity: `curl -I https://<ACCOUNT_IDENTIFIER>.snowflakecomputing.com/polaris/api/catalog`
