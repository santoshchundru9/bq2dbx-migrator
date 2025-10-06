
# 🧮 BigQuery ➜ Databricks SQL Converter

[![FastAPI](https://img.shields.io/badge/FastAPI-0.118.0+-009688?logo=fastapi)](https://fastapi.tiangolo.com/)
[![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python)](https://www.python.org/)
[![Poetry](https://img.shields.io/badge/Poetry-dependency%20manager-60A5FA?logo=poetry)](https://python-poetry.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> A lightweight, high-performance FastAPI service for converting **BigQuery SQL** into **Databricks SQL**, powered by SQLGlot.

---

## 🚀 Features

* 🔄 Converts **BigQuery SQL** syntax to **Databricks SQL**
* ⚡ Built with **FastAPI** for speed and simplicity
* 🧩 Uses **SQLGlot** for accurate SQL translation
* 🧱 Clean architecture and modular code
* 🧪 Fully testable via **pytest**
* 📦 Dependency and environment management via **Poetry**

---

## 🧰 Requirements

* **Python:** 3.11 (recommended)
  ⚠️ Python 3.13 is not fully supported due to dependency compatibility issues.
* **Poetry:** to manage environments and dependencies

Install Poetry globally if not already installed:

```bash
pip install poetry
```

---

## 🧩 Dependencies

| Package              | Version   | Description                          |
| -------------------- | --------- | ------------------------------------ |
| **FastAPI**          | ≥ 0.118.0 | Web framework for building REST APIs |
| **Uvicorn**          | ≥ 0.37.0  | ASGI server for running FastAPI apps |
| **SQLGlot**          | ≥ 27.19.0 | SQL parser and dialect translator    |
| **PyYAML**           | ≥ 6.0.3   | YAML parsing and serialization       |
| **Click**            | ≥ 8.3.0   | CLI utility library                  |
| **pytest**           | ≥ 8.4.2   | Unit testing framework               |
| **python-multipart** | ≥ 0.0.20  | Multipart/form-data parsing          |
| **Jinja2**           | ≥ 3.1.6   | Templating engine used by FastAPI    |

---

## ⚙️ Installation

### 1️⃣ Clone the repository

```bash
git clone https://github.com/your-username/bq2dbx-migrator.git
cd bq2dbx-migrator
```

### 2️⃣ Configure the Python environment

Create a Poetry virtual environment using **Python 3.11**:

```bash
poetry env use python3.11
```

### 3️⃣ Install dependencies

```bash
poetry install
```

> 💡 Using `pip` directly (alternative):
>
> ```bash
> pip install fastapi==0.118.0 uvicorn==0.37.0 sqlglot==27.19.0 pyyaml==6.0.3 click==8.3.0 pytest==8.4.2 python-multipart==0.0.20 jinja2==3.1.6
> ```

---

## ▶️ Run the Application

Start the API locally:

```bash
poetry run uvicorn app:app --reload
```

Then open in your browser:
👉 [http://127.0.0.1:8000](http://127.0.0.1:8000)

Interactive documentation:

* **Swagger UI:** [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)
* **ReDoc:** [http://127.0.0.1:8000/redoc](http://127.0.0.1:8000/redoc)

---

## 📡 API Usage

### 🔹 Example Request

**Endpoint:**

```
POST /convert
```

**Request Body:**

```json
{
  "dialect_from": "bigquery",
  "dialect_to": "databricks",
  "query": "SELECT * FROM `project.dataset.table` WHERE DATE(timestamp_col) = '2025-01-01'"
}
```

**Example (curl):**

```bash
curl -X POST "http://127.0.0.1:8000/convert" \
     -H "Content-Type: application/json" \
     -d '{
           "dialect_from": "bigquery",
           "dialect_to": "databricks",
           "query": "SELECT * FROM `project.dataset.table`"
         }'
```

**Response:**

```json
{
  "converted_query": "SELECT * FROM project.dataset.table"
}
```

---

## 🧪 Running Tests

Run all unit and integration tests:

```bash
poetry run pytest
```

---

## 📁 Project Structure

```
bq2dbx-migrator/
│
├── app/
│   ├── main.py          # FastAPI app entry point
│   ├── api/             # API route definitions
│   ├── core/            # Core logic (conversion logic)
│   └── utils/           # Utility functions and helpers
│
├── tests/               # Test cases
├── pyproject.toml       # Poetry config and dependencies
├── README.md            # Documentation
└── ...
```

---

## 🧠 Troubleshooting

If you encounter compatibility issues with **pydantic** or **FastAPI** under Python 3.13, switch to **Python 3.11**:

```bash
poetry env remove python
poetry env use python3.11
poetry install
```

---

## 🤝 Contributing

Contributions are welcome! To contribute:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-feature`)
3. Commit your changes (`git commit -m "Add new feature"`)
4. Push to your branch (`git push origin feature/new-feature`)
5. Open a Pull Request 🎉

Please make sure all tests pass before submitting a PR.

---