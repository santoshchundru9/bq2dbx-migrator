import sqlglot
import yaml
import re
from pathlib import Path


def convert_sql(query: str, rules_file: str = "bq2dbx/converter/mapping_rules.yaml") -> str:
    """
    Convert BigQuery SQL to Databricks SQL using SQLGlot and custom mappings.

    Handles:
    - ARRAY_AGG(DISTINCT) → COLLECT_SET
    - COUNTIF / SAFE_ functions / IF → CASE WHEN
    - STRUCT → named_struct
    - ARRAY<T>[...] → ARRAY(...)
    - DATE arithmetic with INTERVAL
    - PARTITION BY DATE(col) → PARTITIONED BY (col)
    - CLUSTER BY → CLUSTERED BY
    - STARTS_WITH / ENDS_WITH / REGEXP_CONTAINS / SEARCH
    - UNNEST(GENERATE_ARRAY) → EXPLODE(SEQUENCE(...))
    - Table name mapping project.dataset.table → catalog.schema.table
    - TO_DATE / TO_TIMESTAMP with format
    - JSON path operators (: / [ ]) → get_json_object
    """

    try:
        # Step 1: Core transpilation (BigQuery → Spark SQL)
        transpiled = sqlglot.transpile(query, read="bigquery", write="spark")[0]

        # Step 2: Special handling

        # ARRAY_AGG(DISTINCT ...) → COLLECT_SET(...)
        if "ARRAY_AGG(DISTINCT" in query.upper():
            transpiled = transpiled.replace("COLLECT_LIST(DISTINCT", "COLLECT_SET(")

        # COUNTIF → SUM(CASE WHEN …)
        transpiled = re.sub(
            r"COUNT_IF\s*\((.*?)\)",
            r"SUM(CASE WHEN \1 THEN 1 ELSE 0 END)",
            transpiled,
            flags=re.IGNORECASE,
        )

        # IF(cond, t, f) → CASE WHEN cond THEN t ELSE f END
        transpiled = re.sub(
            r"IF\s*\((.*?),(.*?),(.*?)\)",
            r"CASE WHEN \1 THEN \2 ELSE \3 END",
            transpiled,
            flags=re.IGNORECASE,
        )

        # STRUCT(a AS x, b AS y) → named_struct('x', a, 'y', b)
        transpiled = re.sub(
            r"STRUCT\s*\((.*?)\)",
            lambda m: "named_struct(" + re.sub(
                r"(\w+)\s+AS\s+(\w+)",
                r"'\2', \1",
                m.group(1)
            ) + ")",
            transpiled,
            flags=re.IGNORECASE,
        )

        # ARRAY<T>[...] → ARRAY(...)
        transpiled = re.sub(
            r"ARRAY<\w+>\[(.*?)\]",
            r"ARRAY(\1)",
            transpiled,
            flags=re.IGNORECASE,
        )

        # DATE literal or CAST(... AS DATE) + int → INTERVAL
        transpiled = re.sub(
            r"((?:DATE\s+'[^']+'|CAST\([^)]*AS\s+DATE\)))\s*\+\s*(\d+)",
            r"\1 + INTERVAL \2 DAY",
            transpiled,
        )

        # PARTITION BY DATE(col) → PARTITIONED BY (col)
        transpiled = re.sub(
            r"PARTITION\s+BY\s+DATE\s*\(\s*(\w+)\s*\)",
            r"PARTITIONED BY (\1)",
            transpiled,
            flags=re.IGNORECASE,
        )

        # CLUSTER BY → CLUSTERED BY
        transpiled = re.sub(
            r"\bCLUSTER\s+BY\b",
            "CLUSTERED BY",
            transpiled,
            flags=re.IGNORECASE,
        )

        # UNNEST(GENERATE_ARRAY(...)) → EXPLODE(SEQUENCE(...))
        transpiled = re.sub(
            r"UNNEST\s*\(\s*SEQUENCE\s*\((.*?)\)\s*\)",
            r"EXPLODE(SEQUENCE(\1))",
            transpiled,
            flags=re.IGNORECASE,
        )

        # STARTS_WITH → CASE
        transpiled = re.sub(
            r"STARTS?_?WITH\s*\((.*?),(.*?)\)",
            r"CASE WHEN \1 LIKE CONCAT(\2, '%') THEN TRUE ELSE FALSE END",
            transpiled,
            flags=re.IGNORECASE,
        )

        # ENDS_WITH → CASE
        transpiled = re.sub(
            r"ENDS?_?WITH\s*\((.*?),(.*?)\)",
            r"CASE WHEN \1 LIKE CONCAT('%', \2) THEN TRUE ELSE FALSE END",
            transpiled,
            flags=re.IGNORECASE,
        )

        # JSON operators (: / ['']) → get_json_object
        transpiled = re.sub(
            r"(\w+):(\w+)",
            r"get_json_object(\1, '$.\2')",
            transpiled,
        )
        transpiled = re.sub(
            r"(\w+)\['(\w+)'\]",
            r"get_json_object(\1, '$.\2')",
            transpiled,
        )

        # SEARCH(x,y) → CONTAINS(x,y)
        transpiled = re.sub(
            r"SEARCH\s*\((.*?),(.*?)\)",
            r"CONTAINS(\1, \2)",
            transpiled,
            flags=re.IGNORECASE,
        )

        # Step 3: Apply YAML-based mappings
        rules_path = Path(rules_file)
        rules = {}
        if rules_path.exists():
            with open(rules_path, "r", encoding="utf-8") as f:
                rules = yaml.safe_load(f) or {}

        # Function mappings
        for bq_func, dbx_func in rules.get("functions", {}).items():
            if bq_func in transpiled:
                transpiled = transpiled.replace(bq_func, dbx_func)

        # Table mappings
        if "table_mapping" in rules:
            project_map = rules["table_mapping"].get("projects", {})
            dataset_map = rules["table_mapping"].get("datasets", {})
            table_map = rules["table_mapping"].get("tables", {})

            def replace_table(groups):
                project, dataset, table = groups
                catalog = project_map.get(project, project)
                schema = dataset_map.get(dataset, dataset)
                table_name = table_map.get(table, table)
                return f"{catalog}.{schema}.{table_name}"

            transpiled = re.sub(
                r"`([\w\-]+)`\.`([\w\-]+)`\.`([\w\-]+)`|`([\w\-]+)\.([\w\-]+)\.([\w\-]+)`",
                lambda m: replace_table((
                    m.group(1) or m.group(4),
                    m.group(2) or m.group(5),
                    m.group(3) or m.group(6),
                )),
                transpiled,
            )

        # Step 4: Cleanup
        transpiled = (
            transpiled.replace("TIMESTAMPSTAMP", "TIMESTAMP")
            .replace("( ", "(")
            .replace(" )", ")")
            .replace("CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP")
        )

        return transpiled.strip()

    except Exception as e:
        return f"-- ERROR: {e}"
