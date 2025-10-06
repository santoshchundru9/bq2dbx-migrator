from fastapi import FastAPI, UploadFile, Form, Request, File
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from bq2dbx.converter.sql_converter import convert_sql

from dotenv import load_dotenv
import io
import zipfile
import requests
import re
import os
import time

# ===========================
# Load environment variables
# ===========================
load_dotenv()

AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_KEY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION")

# Safety check
if not all([AZURE_OPENAI_KEY, AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_DEPLOYMENT, AZURE_OPENAI_API_VERSION]):
    raise EnvironmentError("❌ Missing Azure OpenAI credentials in .env file")

# ===========================
# FastAPI Setup
# ===========================
app = FastAPI(title="BQ → Databricks Migrator", version="0.3.6")
templates = Jinja2Templates(directory="templates")


@app.get("/")
def root():
    return {"message": "BQ → Databricks Migrator API is running!"}


@app.get("/upload", response_class=HTMLResponse)
def upload_page(request: Request):
    """Render file upload page."""
    return templates.TemplateResponse("upload.html", {"request": request})


# ===========================
# Helper: LLM-based UDF conversion
# ===========================
def convert_udf_with_llm(js_udf_code: str) -> str:
    """Use Azure OpenAI to convert BigQuery JavaScript UDF to Python/PySpark UDF."""
    try:
        url = f"{AZURE_OPENAI_ENDPOINT}/openai/deployments/{AZURE_OPENAI_DEPLOYMENT}/chat/completions?api-version={AZURE_OPENAI_API_VERSION}"
        headers = {
            "Content-Type": "application/json",
            "api-key": AZURE_OPENAI_KEY
        }

        prompt = f"""
Convert the following BigQuery JavaScript UDF body into a **Python function**.
- Use Python's re.sub() for regex replacements instead of many .replace() calls.
- If input is null, return None.
- Trim, lowercase, then replace all non-alphanumeric characters with underscores.
- Do NOT include any JavaScript, SQL, markdown fences, or explanations.
- Return only valid Python code, starting with `def `.

JavaScript UDF body:
{js_udf_code}
"""

        payload = {
            "messages": [
                {"role": "system", "content": "You are an expert SQL migration assistant."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 800,
            "temperature": 0.0
        }

        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()

        result = data["choices"][0]["message"]["content"].strip()
        result = re.sub(r"```[\w]*\n|```", "", result).strip()

        if not result:
            return f"-- ERROR: Empty response. Raw LLM output: {data}"

        if re.search(r"\bfunction\s*\(|LANGUAGE\s+js|`.*?`", result, re.IGNORECASE):
            return "-- ERROR: Model returned non-Python content."

        return result

    except requests.HTTPError as http_err:
        return f"-- HTTP ERROR {http_err.response.status_code}: {http_err.response.text}"
    except Exception as e:
        return f"-- ERROR calling LLM for UDF conversion: {e}"


# ===========================
# Refine UDF
# ===========================
@app.post("/refine-udf")
async def refine_udf(request: Request, user_prompt: str = Form(...), current_code: str = Form(...)):
    """
    Refine or modify the converted Python UDF using an additional user prompt.
    """
    try:
        url = f"{AZURE_OPENAI_ENDPOINT}/openai/deployments/{AZURE_OPENAI_DEPLOYMENT}/chat/completions?api-version={AZURE_OPENAI_API_VERSION}"
        headers = {"Content-Type": "application/json", "api-key": AZURE_OPENAI_KEY}

        prompt = f"""
        The following is a Python UDF converted from a BigQuery JavaScript UDF.
        Modify or refine it based on the user request below.

        Current Python UDF:
        ```python
        {current_code}
        ```

        User request:
        {user_prompt}

        Return only the updated Python code, without explanations or markdown fences.
        """

        payload = {
            "messages": [
                {"role": "system", "content": "You are an expert Python and PySpark developer."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 800,
            "temperature": 0.4
        }

        response = requests.post(url, headers=headers, json=payload)
        data = response.json()
        result = data["choices"][0]["message"]["content"].strip()
        result = re.sub(r"```[\w]*\n|```", "", result).strip()

        return templates.TemplateResponse(
            "upload.html",
            {"request": request, "converted_query": result, "user_prompt": user_prompt}
        )

    except Exception as e:
        return {"error": str(e)}


# ===========================
# Validate Query (Mock)
# ===========================
@app.post("/validate-query")
async def validate_query(request: Request):
    """
    Validate a SQL query (mock version for now).
    Later this will use Databricks SQL REST API with Service Principal auth.
    """
    try:
        form = await request.form()
        query = form.get("query", "").strip()

        if not query:
            return JSONResponse({"status": "error", "message": "No SQL query provided."})

        time.sleep(1)

        if "error" in query.lower():
            return JSONResponse({"status": "failed", "message": "Query execution failed (mock)."})

        return JSONResponse({"status": "success", "message": "Query executed successfully (mock)."})

    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)})


# ===========================
# Single File Conversion
# ===========================
@app.post("/convert", response_class=HTMLResponse)
async def convert_sql_file(
    request: Request,
    file: UploadFile,
    mode: str = Form("sql")
):
    """Upload a single BigQuery SQL file and convert it."""
    try:
        content = await file.read()
        query = content.decode("utf-8")

        if mode == "sql":
            converted = convert_sql(query)
        elif mode == "pyspark":
            converted = f"df = spark.sql('''{convert_sql(query)}''')"
        elif mode == "python":
            converted = f"df = duckdb.query('''{convert_sql(query)}''').to_df()"
        elif mode == "udf":
            converted = convert_udf_with_llm(query)
        else:
            converted = "-- ERROR: Unsupported conversion mode"

        return templates.TemplateResponse(
            "upload.html",
            {"request": request, "converted_query": converted}
        )
    except Exception as e:
        return templates.TemplateResponse(
            "upload.html",
            {"request": request, "error": f"Conversion failed: {e}"}
        )


# ===========================
# Batch Conversion
# ===========================
@app.post("/convert-batch")
async def convert_batch(
    files: list[UploadFile] = File(...),
    mode: str = Form("sql")
):
    """Upload multiple BigQuery SQL files and return a ZIP of converted files."""
    if len(files) > 100:
        return {"error": "You can upload a maximum of 100 files."}

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
        for file in files:
            try:
                content = await file.read()
                query = content.decode("utf-8")

                if mode == "sql":
                    converted = convert_sql(query)
                elif mode == "pyspark":
                    converted = f"df = spark.sql('''{convert_sql(query)}''')"
                elif mode == "python":
                    converted = f"df = duckdb.query('''{convert_sql(query)}''').to_df()"
                elif mode == "udf":
                    converted = convert_udf_with_llm(query)
                else:
                    converted = "-- ERROR: Unsupported conversion mode"

                base, ext = os.path.splitext(file.filename)
                if not ext:
                    ext = ".sql" if mode != "udf" else ".py"
                output_name = f"{base}_converted{ext}"
                zipf.writestr(output_name, converted)

            except Exception as e:
                error_name = file.filename.replace(".sql", "_error.txt")
                zipf.writestr(error_name, f"Conversion failed: {e}")

    zip_buffer.seek(0)
    return StreamingResponse(
        zip_buffer,
        media_type="application/x-zip-compressed",
        headers={"Content-Disposition": "attachment; filename=converted_queries.zip"}
    )
