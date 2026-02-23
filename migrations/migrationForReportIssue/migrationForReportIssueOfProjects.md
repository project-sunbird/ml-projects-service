# Migration Script – Private Project Bug Analysis & Fix

## 📌 Overview

This script analyzes and fixes incorrectly created **private projects** that should have been public projects for a given program component.

It performs the following:

1. 🔍 Identifies private projects created due to a targeting bug
2. 🧠 Evaluates eligibility & targeting logic
3. 🔁 Re-maps task references from public → private
4. 🏆 Re-issues certificates (only in update mode)
5. 🗑 Deletes corrupted private programs & solutions (only in update mode)
6. 📄 Generates a detailed JSON audit report

The script supports:

* **Dry Run Mode (Read Mode)** – No DB mutations
* **Write Mode (Update Mode)** – Performs actual fixes

---

# 🚀 Usage

```bash
node migrations/migrationForReportIssue/migrationForReportIssueOfProjects.js <programId> --update=<true|false> --token=<userToken>
```

### Example

```bash
node migrations/migrationForReportIssue/migrationForReportIssueOfProjects.js 680893ff3d8d030008cd037a --update=true --token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

---

# ⚙️ Required Arguments

| Argument    | Required | Description                                         |
| ----------- | -------- | --------------------------------------------------- |
| `programId` | ✅ Yes    | Public program `_id`                                |
| `--token`   | ✅ Yes    | User auth token (required for certificate re-issue) |
| `--update`  | ❌ No     | `true` → perform DB updates <br> `false` → dry run  |

---

# 🧪 Modes of Execution

## 1️⃣ Dry Run Mode

```bash
--update=false
```

* No DB updates
* No deletions
* No certificate re-issue
* Generates full analysis report
* Safe for validation

Recommended first step before running update mode.

---

## 2️⃣ Write Mode

```bash
--update=true
```

Performs:

* Delete incorrect public projects
* Convert private projects to public equivalent
* Update tasks (referenceId & externalId)
* Recalculate eligibility
* Update DB
* Re-issue certificates
* Delete corrupted private programs
* Delete corrupted private solutions

⚠️ **Use carefully in production.**

---

# 🧠 Core Logic Flow

## Step 1 – Fetch Program

* Validates program exists
* Loads program components

## Step 2 – Fetch Private Solutions

Filters:

```js
{
  isAPrivateProgram: true,
  parentSolutionId: componentId,
  type: "improvementProject"
}
```

---

## Step 3 – Group By Component → User → Projects

Builds structure:

```
componentId
  └── userId
        └── privateProjectIds[]
```

---

## Step 4 – Targeting Validation

For each user:

* Check public project exists
* Compare status priority
* Select highest priority private project
* Evaluate targeting:

  * Role match
  * Entity match
* Filter only wrongly created projects

---

## Step 5 – Generate Summary

Produces:

```json
{
  finalResult: [...],
  summary: [...]
}
```

Identifies:

* Projects created due to bug
* Projects needing migration

---

## Step 6 – Update Flow (Only in Write Mode)

For each affected project:

1. Delete public project
2. Update tasks using public reference
3. Validate eligibility
4. Update DB
5. Re-issue certificate
6. Wait 30 seconds between batches
7. Delete old private program & solution

---

# 📊 Status Priority Logic

```js
submitted  → 3
inprogress → 2
started    → 1
```

Higher priority private project replaces lower priority public project.

---

# 📁 Output

All output files are stored in:

```
/output/
```

File name format:

```
<programId>-<timestamp>.json
```

---

# 📄 Output JSON Structure

### 1️⃣ Component → User → Private Projects

```json
component_user_private_projects
```

---

### 2️⃣ Bug Analysis

```json
private_project_bug_analysis
```

Includes:

* evaluatedPrivateProjects
* targeting result
* eligibility status

---

### 3️⃣ Program Private Project Data

```json
program_private_project_data
```

---

### 4️⃣ Deleted → Replacement Map

```json
deleted_to_replacement_id_map
```

Maps:

```
publicProjectId → newPrivateProjectId
```

---

# 🔐 Environment Setup

Ensure `.env` contains:

```
MONGODB_URL=<your_mongodb_connection_string>
```

---

# 📦 Dependencies

Install required packages:

```bash
npm install mongodb lodash dotenv
```

---

# 🧾 External Helper Functions

Imported from:

```
./eligibilityAndTasksValidator
```

Includes:

* `updateTasksUsingPublicProject`
* `criteriaValidation`
* `updateCorruptedProjectsInDB`
* `reIssueCertificates`

---

# ⚠️ Safety Checklist Before Running in Production

* ✅ Run dry mode first
* ✅ Backup database
* ✅ Confirm certificate API token
* ✅ Verify programId
* ✅ Validate output JSON

---

# 🧹 Deletion Rules (Write Mode Only)

Deletes:

```js
programs  → isAPrivateProgram: true
solutions → isAPrivateProgram: true
```

Only those linked to corrupted projects.

---

# ⏳ Batch Processing

* Batch size: `100`
* 30 second delay between certificate re-issue batches

---

# 🛑 Exit Conditions

Script exits if:

* ❌ `MONGODB_URL` not set
* ❌ Invalid `programId`
* ❌ `--token` not provided
* ❌ Program not found

---

# 🧩 Example Execution Strategy

### Step 1 – Dry Run

```bash
--update=false
```

Review output JSON carefully.

### Step 2 – Write Mode

```bash
--update=true
```

Monitor logs.

---