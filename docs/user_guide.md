# RefDQ User Guide

RefDQ is a data quality application for uploading reference data files to Snowflake. It validates your data before upload, ensuring schema compatibility, correct data types, and passing custom data quality checks.

---

## Table of Contents

- [End User Guide](#end-user-guide)
  - [Uploading a File](#uploading-a-file)
  - [Selecting a Target Table](#selecting-a-target-table)
  - [Understanding Validation Results](#understanding-validation-results)
  - [Completing the Upload](#completing-the-upload)
- [Administrator Guide](#administrator-guide)
  - [Configuration Overview](#configuration-overview)
  - [Configuring Tables](#configuring-tables)
  - [Configuring Data Quality Checks](#configuring-data-quality-checks)
  - [Snowflake Setup](#snowflake-setup)

---

# End User Guide

## Uploading a File

1. **Open RefDQ** in your browser
2. **Upload your file** using the file uploader
   - Supported formats: CSV (`.csv`) and Excel (`.xlsx`)
   - Column names are automatically converted to uppercase

3. **Auto-detection**: RefDQ will attempt to match your file's columns to a configured table:
   - **Green message** ("Auto-detected table: X"): Exact column match found
   - **Blue message** ("Suggested table: X"): Partial match found (your file is a subset of the table columns)
   - **Yellow warning**: No match found - select the table manually

4. A preview of your uploaded data (first 100 rows) is displayed for verification

## Selecting a Target Table

After uploading, configure where and how to upload:

### Upload Type

| Type | Description |
|------|-------------|
| **Merge (upsert)** | Updates existing rows (matched by primary key) and inserts new rows. Existing rows not in your file are preserved. |
| **Replace** | Deletes all existing rows and inserts only the rows from your file. |

### Group and Table

1. **Group** (optional): Filter tables by category/group
2. **Table**: Select the destination table

A sample of the current table data is shown to help verify you've selected the correct target.

## Understanding Validation Results

RefDQ runs several validation checks. All must pass before upload is allowed.

### Schema Check

Verifies your file has all required columns.

| Result | Meaning | Action Required |
|--------|---------|-----------------|
| **Schema matches!** | All table columns found in your file | None |
| **Schema differs** | Missing columns listed | Add missing columns to your file, or click "Ignore and continue" if columns are optional |

### Data Type Check

Verifies values can be converted to the expected Snowflake data types.

| Result | Meaning | Action Required |
|--------|---------|-----------------|
| **Data types match!** | All values are compatible | None |
| **Data types differ** | Invalid values listed | Fix the values shown in the error table. The table shows: primary key, column name, invalid value, and expected data type |

**Common data type issues:**
- Text in a numeric column (e.g., "N/A" in an INTEGER field)
- Invalid date formats (e.g., "31/02/2024")
- Numbers exceeding precision limits

### Impact Summary

Shows what will happen when you upload:

**For Merge uploads:**
- Number of rows to be **inserted** (new rows)
- Number of rows to be **updated** (existing rows with changes)

**For Replace uploads:**
- Number of rows to be **deleted** (current table contents)
- Number of rows to be **inserted** (from your file)

If "No changes to be uploaded" appears, your file matches the existing table data exactly.

### Data Quality Checks

Custom business rules configured for each table. Each check appears as an expandable section:

| Icon | Status | Meaning |
|------|--------|---------|
| ✓ (green) | Passed | All rows satisfy this rule |
| ✗ (red) | Failed | Some rows violate this rule |

**When a check fails:**
1. Expand the check to see the description and failed rows
2. The table shows all rows that violate the rule (up to 10,000)
3. Fix the data in your source file
4. Re-upload the corrected file

## Completing the Upload

Once all checks pass:

1. Review the impact summary
2. Click **Upload** to write data to Snowflake
3. Wait for confirmation: "Upload complete!"

Use the **Reset** button (top right) to start over with a new file.

---

# Administrator Guide

## Configuration Overview

RefDQ uses YAML configuration files organized in directories:

```
refdq/
├── config.yaml          # Global settings
├── tables/              # Table definitions (one file per table)
│   ├── names.yaml
│   └── places.yaml
└── checks/              # Check type definitions
    ├── unique.yaml
    ├── upper_bound.yaml
    └── like.yaml
```

## Global Configuration

**File:** `config.yaml`

```yaml
# Path to configuration directories
config_path: /path/to/refdq

# Snowflake schema for temporary staging tables
temp_schema: database.schema
```

| Setting | Description |
|---------|-------------|
| `config_path` | Absolute path to the directory containing `tables/` and `checks/` folders |
| `temp_schema` | Snowflake database.schema where temporary tables are created during validation |

## Configuring Tables

Create a YAML file in the `tables/` directory for each target table.

**File:** `tables/{table_name}.yaml`

```yaml
group: Customer                           # Optional: for filtering in UI
target_table: database.schema.table_name  # Fully qualified Snowflake table
primary_key: id                           # Column(s) used for merge matching
checks:                                   # List of data quality checks
  - type: unique
    column: id
    ignore_null: false
  - type: upper_bound
    column: age
    upper_bound: 150
    description: Age must be 150 or less  # Optional: additional context
action:                                   # Optional: post-upload action
  name: Run task
  trigger: button
  command: CALL my_procedure()
```

### Required Fields

| Field | Description |
|-------|-------------|
| `target_table` | Fully qualified table name: `database.schema.table` |
| `primary_key` | Column name(s) for identifying unique rows. Single column: `primary_key: id`. Multiple columns: `primary_key: [col1, col2]` |

### Optional Fields

| Field | Description |
|-------|-------------|
| `group` | Category name for grouping tables in the UI dropdown |
| `checks` | List of data quality checks to run (see below) |
| `action` | Post-upload SQL command to execute |

### Configuring Checks for a Table

Each check in the `checks` list requires:

```yaml
checks:
  - type: check_type_name    # Must match a file in checks/
    column: column_name      # Column to check (if required by check type)
    # ... additional parameters required by the check type
    description: Optional additional context shown to users
```

## Configuring Data Quality Checks

Check definitions are reusable templates. Create files in the `checks/` directory.

**File:** `checks/{check_type}.yaml`

```yaml
type: check_type_name
description: Description with {placeholders} for dynamic values
sql: |
  SELECT *
  FROM {table}
  WHERE {column} fails some condition
```

### Required Fields

| Field | Description |
|-------|-------------|
| `type` | Identifier matching the filename (without .yaml) |
| `description` | User-facing explanation. Use `{placeholder}` for dynamic values |
| `sql` | SQL query returning rows that **fail** the check |

### Available Placeholders

| Placeholder | Source | Description |
|-------------|--------|-------------|
| `{table}` | System | The merged data (upload + existing, for merge) or staging table |
| `{primary_key}` | System | The table's primary key column(s) |
| `{column}`, `{expression}`, etc. | Table config | Values from the table's check configuration |

### Built-in Check Types

#### unique
Checks for duplicate values.

```yaml
# checks/unique.yaml
type: unique
description: Checks for duplicate values in column "{column}".
sql: |
  SELECT *
  FROM {table}
  WHERE ({column} IS NOT NULL OR NOT {ignore_null})
  QUALIFY COUNT(*) OVER(PARTITION BY {column}) > 1
```

**Table config:**
```yaml
- type: unique
  column: id
  ignore_null: false   # true = allow multiple NULLs
```

#### upper_bound
Checks values don't exceed a maximum.

```yaml
# checks/upper_bound.yaml
type: upper_bound
description: Checks for values greater than {upper_bound} in column "{column}".
sql: |
  SELECT *
  FROM {table}
  WHERE {column} > {upper_bound}
```

**Table config:**
```yaml
- type: upper_bound
  column: age
  upper_bound: 150
```

#### like
Checks string values match a pattern.

```yaml
# checks/like.yaml
type: like
description: Checks string values in column "{column}" match the "like" expression "{expression}".
sql: |
  SELECT *
  FROM {table}
  WHERE {column} NOT LIKE '{expression}'
```

**Table config:**
```yaml
- type: like
  column: email
  expression: '%@%.%'
```

### Creating Custom Checks

1. Create a new file in `checks/` (e.g., `checks/range.yaml`)
2. Define `type`, `description`, and `sql`
3. Use placeholders for configurable values
4. Reference the check type in table configurations

**Example: Range check**
```yaml
# checks/range.yaml
type: range
description: Checks values in "{column}" are between {min_value} and {max_value}.
sql: |
  SELECT *
  FROM {table}
  WHERE {column} < {min_value} OR {column} > {max_value}
```

**Usage:**
```yaml
# tables/products.yaml
checks:
  - type: range
    column: price
    min_value: 0
    max_value: 10000
```

## Snowflake Setup

RefDQ can be deployed as a **Streamlit in Snowflake** app or run **locally**. This section covers both approaches.

### Prerequisites

1. **Target tables** — the Snowflake tables that will receive uploaded data
2. **Temporary schema** — used for staging data during validation
3. **Configuration files** — YAML definitions for tables and checks (see [Configuring Tables](#configuring-tables))

### Step 1: Create the Temp Schema

RefDQ writes uploaded data to a temporary staging table for validation before committing to the target table. Create a dedicated schema for this:

```sql
CREATE DATABASE IF NOT EXISTS my_database;
CREATE SCHEMA IF NOT EXISTS my_database.refdata_tmp;
```

### Step 2: Create Target Tables

Create the tables that RefDQ will upload data to:

```sql
CREATE TABLE my_database.my_schema.names (
    ID INTEGER PRIMARY KEY,
    NAME VARCHAR(100),
    AGE INTEGER
);
```

### Step 3: Create Configuration Files

Create a directory to hold your table and check definitions. This can be anywhere accessible to the app (see deployment options below).

```
refdq/
├── config.yaml
├── tables/
│   └── names.yaml
└── checks/
    └── unique.yaml
```

Update `config.yaml` to point to the configuration directory and specify the temp schema:

```yaml
config_path: /path/to/refdq
temp_schema: my_database.refdata_tmp
```

See [Configuring Tables](#configuring-tables) and [Configuring Data Quality Checks](#configuring-data-quality-checks) for the format of these files.

---

### Deploying as a Streamlit in Snowflake App

There are two ways to get your files into Snowflake: uploading to a stage manually, or using Snowflake's Git integration. The Git approach is recommended as it keeps your code and configuration in version control.

#### Option A: Using Snowflake Git Integration (Recommended)

Snowflake can connect directly to a Git repository, making it the source of truth for your app code and configuration. This is the recommended approach because:

- **Version control** — all changes to code and check definitions are tracked with full history
- **Collaboration** — multiple administrators can propose changes via pull requests and code review
- **Consistency** — the same repository serves both local development and the Snowflake deployment
- **Auditability** — you can trace exactly when and why a data quality check was added or modified
- **Simple updates** — deploying changes is a single `ALTER GIT REPOSITORY ... FETCH` command

##### 1. Structure Your Repository

Keep the application code and configuration files together in the same repository:

```
RefDQ/
├── home.py
├── refdata.py
├── get_snowflake.py
├── config.yaml
├── tables/
│   └── names.yaml
└── checks/
    └── unique.yaml
```

Set `config_path` in `config.yaml` to `.` so the app reads configuration from the working directory:

```yaml
config_path: .
temp_schema: my_database.refdata_tmp
```

##### 2. Create an API Integration for Git

This allows Snowflake to authenticate with your Git provider (GitHub, GitLab, Bitbucket, etc.):

```sql
CREATE API INTEGRATION IF NOT EXISTS git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/your-org/')
  ENABLED = TRUE;
```

If your repository is private, create a secret with a personal access token first:

```sql
CREATE SECRET IF NOT EXISTS my_database.my_schema.git_secret
  TYPE = password
  USERNAME = 'your-username'
  PASSWORD = 'your-personal-access-token';
```

##### 3. Create a Git Repository in Snowflake

```sql
CREATE GIT REPOSITORY IF NOT EXISTS my_database.my_schema.refdq_repo
  API_INTEGRATION = git_api_integration
  GIT_CREDENTIALS = my_database.my_schema.git_secret  -- omit for public repos
  ORIGIN = 'https://github.com/your-org/RefDQ.git';
```

##### 4. Fetch the Latest Code

```sql
ALTER GIT REPOSITORY my_database.my_schema.refdq_repo FETCH;
```

You can verify the contents:

```sql
SHOW GIT BRANCHES IN my_database.my_schema.refdq_repo;
LIST @my_database.my_schema.refdq_repo/branches/main/;
```

##### 5. Create the Streamlit App

Point the app at the repository branch:

```sql
CREATE STREAMLIT IF NOT EXISTS my_database.my_schema.refdq
  ROOT_LOCATION = '@my_database.my_schema.refdq_repo/branches/main'
  MAIN_FILE = 'home.py'
  QUERY_WAREHOUSE = my_warehouse;
```

##### 6. Grant Access to Users

```sql
GRANT USAGE ON STREAMLIT my_database.my_schema.refdq TO ROLE user_role;
```

Users also need permissions on the target tables and temp schema (see [Required Grants](#required-grants) below).

##### Updating the App

After pushing changes to the repository, fetch them into Snowflake:

```sql
ALTER GIT REPOSITORY my_database.my_schema.refdq_repo FETCH;
```

The Streamlit app will pick up the changes on the next page load.

---

#### Option B: Using a Named Stage

If you prefer not to use Git integration, you can upload files to a named internal stage.

##### 1. Create a Stage for App Files

```sql
CREATE STAGE IF NOT EXISTS my_database.my_schema.refdq_stage
  DIRECTORY = (ENABLE = TRUE)
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
```

##### 2. Upload Files to the Stage

The repository contains three Python files:

| File | Purpose |
|------|---------|
| `home.py` | Main Streamlit app (entry point) |
| `refdata.py` | Data validation engine |
| `get_snowflake.py` | Snowflake connection wrapper |

Upload them along with your configuration files:

```sql
-- Application code
PUT file:///path/to/RefDQ/home.py @my_database.my_schema.refdq_stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file:///path/to/RefDQ/refdata.py @my_database.my_schema.refdq_stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file:///path/to/RefDQ/get_snowflake.py @my_database.my_schema.refdq_stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Configuration
PUT file:///path/to/config.yaml @my_database.my_schema.refdq_stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file:///path/to/tables/names.yaml @my_database.my_schema.refdq_stage/tables/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file:///path/to/checks/unique.yaml @my_database.my_schema.refdq_stage/checks/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
```

> **Tip:** You can also upload files via Snowsight by navigating to **Data > Databases > your_database > your_schema > Stages > refdq_stage** and using the upload button.

##### 3. Set the Configuration Path

Update `config.yaml` so that `config_path` points to the stage's working directory. When a Streamlit in Snowflake app runs, staged files are available in the current working directory:

```yaml
config_path: .
temp_schema: my_database.refdata_tmp
```

##### 4. Create the Streamlit App

```sql
CREATE STREAMLIT IF NOT EXISTS my_database.my_schema.refdq
  ROOT_LOCATION = '@my_database.my_schema.refdq_stage'
  MAIN_FILE = 'home.py'
  QUERY_WAREHOUSE = my_warehouse;
```

##### 5. Grant Access to Users

```sql
GRANT USAGE ON STREAMLIT my_database.my_schema.refdq TO ROLE user_role;
```

Users also need permissions on the target tables and temp schema (see [Required Grants](#required-grants) below).

##### Updating the App

Re-upload the modified files to the stage:

```sql
PUT file:///path/to/RefDQ/home.py @my_database.my_schema.refdq_stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
```

The Streamlit app will pick up the changes on the next page load.

---

#### Opening the App

Navigate to **Snowsight > Streamlit** and open the RefDQ app, or go directly to:

```
https://<account>.snowflakecomputing.com/#/streamlit-apps/MY_DATABASE.MY_SCHEMA.REFDQ
```

The Snowflake connection is established automatically via `st.connection('snowflake')` — no credentials are needed. The app runs under the role of the signed-in user.

---

### Deploying Locally

For local or external deployment, RefDQ connects to Snowflake using environment variables.

#### 1. Install Dependencies

```bash
pip install streamlit pandas pyyaml snowflake-snowpark-python
```

#### 2. Set Environment Variables

```bash
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_DATABASE=your_database
export SNOWFLAKE_SCHEMA=your_schema
```

#### 3. Configure the Config Path

Update `config.yaml` with the absolute path to your configuration directory:

```yaml
config_path: /path/to/refdq
temp_schema: my_database.refdata_tmp
```

#### 4. Run the App

```bash
streamlit run home.py
```

---

### Required Grants

The user or role running RefDQ needs the following permissions:

```sql
-- Temp schema (full access for staging)
GRANT USAGE ON DATABASE my_database TO ROLE refdq_role;
GRANT USAGE ON SCHEMA my_database.refdata_tmp TO ROLE refdq_role;
GRANT CREATE TABLE ON SCHEMA my_database.refdata_tmp TO ROLE refdq_role;

-- Target tables
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE my_database.my_schema.table_name TO ROLE refdq_role;
```

> **Note:** For Streamlit in Snowflake deployments, the app runs under the role of the signed-in user, so grant permissions to the appropriate user roles. A dedicated service account role is only needed for local/external deployments.

---

## Troubleshooting

### "No matching table found"
- Verify column names match exactly (case-insensitive, converted to uppercase)
- Check that a table config exists in `tables/`

### Schema errors
- Add missing columns to your file, or
- Use "Ignore and continue" for optional columns

### Data type errors
- Check for text values in numeric columns
- Verify date formats match Snowflake expectations
- Look for special characters or encoding issues

### Check failures
- Review the failed rows in the expanded check section
- Correct data in your source file and re-upload
