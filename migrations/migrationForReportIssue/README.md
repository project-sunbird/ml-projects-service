# Private Project Migration Script

It supports:

* ✅ **Dry Run Mode** (No database updates)
* ✅ **Update Mode** (Performs actual updates and deletions)

---

# 📍 Script Location

```bash
migrations/migrationForReportIssue/migrationForReportIssueOfProjects.js
```

---
# 🚀 How to Run the Script

```bash
node migrations/migrationForReportIssue/migrationForReportIssueOfProjects.js <programId> --update=<true|false> --token=<userToken>
```

---

# 🧪 Execution Modes

## 1️⃣ Dry Run Mode (Recommended First Step)

This mode **does not perform any database updates**.

```bash
node migrations/migrationForReportIssue/migrationForReportIssueOfProjects.js <programId> --update=false --token=<userToken>
```

Use this mode to:

* Validate output
* Review generated report
* Ensure expected behavior before performing actual updates

---

## 2️⃣ Update Mode (Production Execution)

This mode performs:

* Project updates
* Certificate re-issue
* Deletion of related private programs/solutions

```bash
node migrations/migrationForReportIssue/migrationForReportIssueOfProjects.js <programId> --update=true --token=<userToken>
```

⚠️ **Use with caution.**

* Ensure database backup is taken.
* Always run dry mode first.

---

# 🛑 Mandatory Arguments

| Argument    | Required | Description          |
| ----------- | -------- | -------------------- |
| `programId` | ✅ Yes    | Target program ID    |
| `--token`   | ✅ Yes    | Authentication token |
| `--update`  | ❌ No     | `true` or `false`    |

---

# ✅ Recommended Execution Flow

### Step 1 – Run Dry Mode

```bash
--update=false
```

Review output file.

### Step 2 – Run Update Mode (If Validated)

```bash
--update=true
```

---

# ⚠️ Important Notes

* Script will exit if:

  * `MONGODB_URL` is not set
  * `programId` is invalid
  * `--token` is missing
* Always verify logs before and after execution.
* Take database backup before running in update mode.

---