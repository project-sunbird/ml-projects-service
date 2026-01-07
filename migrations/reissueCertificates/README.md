# 📦 Project Certificate Reissue Migration

This migration script is used to **re-evaluate certificate eligibility**, **relink task references**, **update eligible projects in MongoDB**, and **trigger certificateReissue API** for eligible projects.

It is designed to be **safe, auditable, batched, and rerunnable**.

---

## 🧠 What this migration does (high level)

For a given list of project IDs, the script:

1. Connects to MongoDB
2. Fetches projects **in batches**
3. Filters only projects that have a **certificate**
4. Finds a **reference project** per solution
5. Updates task `referenceId`s using the reference project
6. Runs **criteria validation** to determine eligibility
7. Updates **only eligible projects** in MongoDB
8. Maintains an **audit file** of updated project IDs
9. Calls an **certificateReissue API** for each eligible project
10. Stores all API responses in a **JSON file**

---

## 📁 Folder Structure

```
migrations/
└── reissueCertificates/
    ├── reissueCertificates.js
    ├── input.json
    ├── README.md
    ├── updatedProjects-<timestamp>.txt
    └── apiResponses-<timestamp>.json
```

---

## 📄 input.json

```json
{
  "userToken": "<AUTH_TOKEN>",
  "projectServiceBaseUrl": "http://localhost:5000",
  "writeMode": true,
  "projectIds": [
    "695d65c007e87229bc11f05c",
    "695d65c007e87229bc11f061"
  ]
}
```

### Fields explained

| Field                   | Description                                          |
| ----------------------- | ---------------------------------------------------- |
| `userToken`             | Token for API calls                           |
| `projectServiceBaseUrl` | Base URL of project service                          |
| `writeMode`             | `true` → DB & file writes enabled, `false` → dry run |
| `projectIds`            | List of project `_id`s to process                    |

---

## ⚙️ Required Environment Variables (`.env`)

```env
MONGODB_URL=mongodb://localhost:27017/<db-name>
```

> `.env` is loaded from **two levels above** the migration folder.

---

## 📦 Required npm Packages

The migration script depends on the following npm packages.

```
mongoose
mongodb
fs
path
request
dotenv
```

---

## 🚀 Execution

Run the script **from the project root**:

```bash
node migrations/reissueCertificates/reissueCertificates.js --inputFile=input.json
```

---

## ✅ Final Outcome

After successful execution:

* Tasks have correct `referenceId`s
* Certificate eligibility is recalculated
* Eligible projects are updated
* External systems are notified
* Full audit trail is available

---