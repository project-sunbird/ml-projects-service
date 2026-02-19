# Update User Role Information Migration Script

## 📌 Overview

This migration script updates the `userRoleInformation.school` field in the **`projects`** collection.

### 🎯 Purpose

* Identify projects created within a given date range.
* If `userRoleInformation.school` contains a **UUID**, replace it with the corresponding **UDISE code**.
* UDISE code is fetched:

  * From `userProfile.userLocations` (if available), OR
  * From the `location/search` API (fallback).

The script supports:

* **READ-MODE** → Dry run (no DB updates)
* **WRITE-MODE** → Performs actual DB updates

---

## 🛠 Prerequisites

* Node.js installed
* MongoDB connection configured
* Required environment variables in `.env` file

### Required `.env` Variables

```env
MONGODB_URL=<your-mongodb-connection-string>
USER_SERVICE_URL=<user-service-base-url>
```

Example:

```env
MONGODB_URL=mongodb://localhost:27017/your-db
USER_SERVICE_URL=http://localhost:3000
```

---

## 📂 Script Location

```
migrations/updateProjectDocuments/updateUserRoleInformation.js
```

---

## 🚀 How to Execute

### 1️⃣ Navigate to Project Root

```bash
cd <project-root-directory>
```

---

### 2️⃣ READ MODE (Dry Run – No Database Update)

Use this mode to:

* See how many projects are eligible
* Identify failures
* Generate log file
* **No DB changes are made**

```bash
node migrations/updateProjectDocuments/updateUserRoleInformation.js \
--update=false \
--fromDate=2025-09-01 \
--toDate=2025-09-30
```

---

### 3️⃣ WRITE MODE (Actual DB Update)

Use this mode to:

* Replace UUID with UDISE code
* Update database records

```bash
node migrations/updateProjectDocuments/updateUserRoleInformation.js \
--update=true \
--fromDate=2025-09-01 \
--toDate=2025-09-30
```

---

## 📅 Date Parameters

Both parameters are **mandatory**:

| Parameter    | Description            |
| ------------ | ---------------------- |
| `--fromDate` | Start date (inclusive) |
| `--toDate`   | End date (inclusive)   |

### Format

```
YYYY-MM-DD
```

Example:

```
--fromDate=2025-09-01
--toDate=2025-09-30
```

Internally:

* `fromDate` → `YYYY-MM-DDT00:00:00.000Z`
* `toDate` → Next day `00:00:00.000Z` (exclusive upper bound)

---

## 🔎 What the Script Does

### Step 1: Fetch Eligible Projects

* Filters projects where:

  ```js
  createdAt >= fromDate AND createdAt < toDate
  ```

---

### Step 2: Validate School Field

Skips project if:

* `userRoleInformation` does not exist
* `userRoleInformation.school` does not exist
* `school` is not a valid UUID

---

### Step 3: Fetch UDISE Code

#### Case A: Found in userProfile

If available in:

```js
project.userProfile.userLocations
```

→ Directly prepares update.

---

#### Case B: Not Found Locally

Calls:

```
POST {USER_SERVICE_URL}/v1/location/search
```

To fetch school code using UUID.

---

### Step 4: Bulk Update (WRITE MODE only)

Uses:

```js
collection.bulkWrite()
```

Batch size:

```
100 documents per batch
```

Wait time between batches:

```
5 seconds
```

---

## 📄 Log File

After execution, a log file is generated:

```
project-update-log.json-<timestamp>.json
```

### Contains:

```json
{
  "executionMode": "READ-MODE | WRITE-MODE",
  "dateRange": "fromDate to toDate",
  "projectsEligibleForUpdate": [],
  "failedProjectUpdateStatus": {}
}
```

### Fields Explained

| Field                       | Description                              |
| --------------------------- | ---------------------------------------- |
| `executionMode`             | READ-MODE or WRITE-MODE                  |
| `dateRange`                 | Provided date range                      |
| `projectsEligibleForUpdate` | List of project IDs considered           |
| `failedProjectUpdateStatus` | Projects where update failed with reason |

---

## ⚠️ Important Notes

* Always run in **READ-MODE first** before WRITE-MODE.
* Ensure `USER_SERVICE_URL` is accessible.
* Ensure MongoDB connection string is correct.
* UUID validation is handled using `UTILS.checkValidUUID`.
* Script processes in batches to prevent memory overload.

---

## 🧪 Recommended Execution Strategy

1. Run in READ-MODE
2. Review generated log file
3. Fix API/network issues (if any)
4. Run in WRITE-MODE
5. Verify DB changes

---

## 🧯 Error Handling

The script exits if:

* `--fromDate` or `--toDate` is missing
* MongoDB connection fails
* Unexpected runtime error occurs

---

## ✅ Example Full Execution

```bash
node migrations/updateProjectDocuments/updateUserRoleInformation.js \
--update=true \
--fromDate=2025-09-01 \
--toDate=2025-09-30
```

---