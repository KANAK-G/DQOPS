import streamlit as st
import pandas as pd
import requests
from trino.auth import BasicAuthentication
from trino.dbapi import connect
import datetime

############################
# HELPER FUNCTIONS
############################

def get_checks_df(api_key, env_name):
    """
    Connects to Trino and retrieves the checks data from 'coke_dummy.checks'
    joined with 'coke_dummy.runs' and also pulls in labels via LEFT JOIN tags.
    """
    host_name = "tcp.unique-haddock.dataos.app"
    user_name = "kanakgupta"
    cluster_name = "miniature"

    conn = connect(
        host=host_name,
        port=7432,
        auth=BasicAuthentication(user_name, api_key),
        http_scheme="https",
        http_headers={"cluster-name": cluster_name}
    )
    cur = conn.cursor()
    cur.execute(
        """
        WITH checks AS (
            SELECT
                c.id,
                c.run_id,
                c.created_at,
                c.name AS check_name,
                c.identity,
                c.definition,
                r.schema_name,
                c.table_name,
                c.outcome,
                CAST(json_extract_scalar(c.diagnostics, '$.value') AS double) AS current_value
            FROM icebase.coke_dummy.checks c
            JOIN icebase.coke_dummy.runs r
              ON c.run_id = r.run_id
        ),
        tags AS (
            SELECT
                element_at(split(targetfqn, '.'), 3) AS schema,
                element_at(split(targetfqn, '.'), 4) AS table_name,
                array_join(array_agg(DISTINCT trim(tagfqn)), ', ') AS labels
            FROM "metisdb"."public".tag_usage
            WHERE (
                targetfqn LIKE 'icebase.icebase.retail%'
                OR targetfqn LIKE 'icebase.icebase.customer_relationship%'
            )
            AND labeltype = 2
            GROUP BY 1, 2
        )
        SELECT
            c.*,
            labels
        FROM checks c
        LEFT JOIN tags t
          ON c.table_name = t.table_name
         AND c.schema_name = t.schema
        """
    )
    data = cur.fetchall()

    # NOTE: We have 11 columns now because of "c.*" (10 columns) + "labels" (1 column).
    df_schema = pd.DataFrame(
        data,
        columns=[
            "id", "run_id", "created_at", "check_name", "identity",
            "definition", "schema_name", "table_name", "outcome",
            "current_value", "labels"
        ]
    )
    return df_schema

def get_schemas(api_key, env_name, catalog="icebase"):
    """
    Returns a list of schema names for a given catalog.
    """
    url = f"{env_name}/workbench/api/meta-info/presto/{catalog}?&routingName=miniature"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    return data.get("schemas", [])

def get_tables(api_key, env_name, schema_name, catalog="icebase"):
    """
    Returns a list of table names for a given schema in a catalog.
    """
    url = f"{env_name}/workbench/api/meta-info/presto/{catalog}/{schema_name}?&routingName=miniature"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    return data.get("tables", [])

def split_labels_to_list(labels_str):
    """
    Splits a comma-separated 'labels' string into a list of individual labels.
    Example: "LabelA, LabelB" -> ["LabelA", "LabelB"].
    Returns an empty list if labels_str is None or empty.
    """
    if not labels_str or pd.isna(labels_str):
        return []
    return [lbl.strip() for lbl in labels_str.split(",")]

def has_any_selected_labels(labels_str, selected_labels):
    """
    Given a row's 'labels_str' (e.g., "LabelA, LabelB") and a list of
    selected labels, return True if there's any overlap.
    This is "OR" logic.
    """
    if not selected_labels:
        return True  # No label filter means keep everything
    row_labels = split_labels_to_list(labels_str)
    # If any selected label is in row_labels -> keep
    return any(lbl in row_labels for lbl in selected_labels)

############################
# STREAMLIT APP
############################

def main():
    st.title("DQOPS")

    # Replace these with your actual environment URL and API key
    env_name = "https://unique-haddock.dataos.app"
    api_key = "cHl0aG9uX2FwaS41ZWNmNzZjZC04MWI5LTQ2YjktOTI0OC04NjJkZmE1YmY2NmU="
    catalog = "icebase"

    # 1) Load checks DataFrame
    checks_df = get_checks_df(api_key, env_name)

    # 2) Convert created_at to datetime
    checks_df["created_at"] = pd.to_datetime(checks_df["created_at"])

    # 3) Load schemas
    all_schemas = get_schemas(api_key, env_name, catalog=catalog)

    # 4) Build a list of unique labels from the 'labels' column
    all_label_set = set()
    for val in checks_df["labels"].dropna().unique():
        # val might be something like "LabelA, LabelB"
        for label_item in val.split(","):
            label_item = label_item.strip()
            if label_item:
                all_label_set.add(label_item)
    all_labels = sorted(list(all_label_set))

    # ================
    # FILTERS
    # ================
    selected_schemas = st.multiselect(
        "Filter by Schema(s):",
        all_schemas,
        default=[],
        help="Leave empty to see data across all schemas."
    )

    selected_labels = st.multiselect(
        "Filter by Label(s):",
        all_labels,
        default=[],
        help="Leave empty to see data with any or no labels."
    )

    # Filter by selected schemas
    if selected_schemas:
        checks_df = checks_df[checks_df["schema_name"].isin(selected_schemas)]

    # Filter by selected labels (using OR logic)
    # Keep only rows that have at least one of the selected labels
    # (or keep all if none are selected)
    if selected_labels:
        checks_df = checks_df[checks_df["labels"].apply(lambda x: has_any_selected_labels(x, selected_labels))]

    # ================
    # CREATE TABS
    # ================
    tab_summary, tab_datasets, tab_checks = st.tabs(["Summary", "Datasets", "Checks"])

    # ================
    # TAB 1: SUMMARY
    # ================
    with tab_summary:
        # 1) Total Datasets
        def get_total_datasets(selected_schemas_list):
            if not selected_schemas_list:
                # Sum across ALL schemas
                total_tables = 0
                for sch in all_schemas:
                    tables = get_tables(api_key, env_name, sch, catalog=catalog)
                    total_tables += len(tables)
                return total_tables
            else:
                # Sum for only the selected schemas
                total_tables = 0
                for sch in selected_schemas_list:
                    tables = get_tables(api_key, env_name, sch, catalog=catalog)
                    total_tables += len(tables)
                return total_tables

        total_datasets = get_total_datasets(selected_schemas)

        # 2) Total Fail Checks
        total_fail_checks = len(checks_df[checks_df["outcome"] == "fail"])

        # 3) Health Score = (# pass) / (total checks)
        total_pass_checks = len(checks_df[checks_df["outcome"] == "pass"])
        total_checks = len(checks_df)
        health_score = total_pass_checks / total_checks if total_checks > 0 else 0.0

        # 4) Coverage
        distinct_table_count = checks_df[["schema_name","table_name"]].drop_duplicates()
        used_tables = len(distinct_table_count)
        coverage = (used_tables / total_datasets) * 100 if total_datasets > 0 else 0.0

        # Display KPI columns
        kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
        kpi_col1.metric("Health Score", f"{health_score:.2%}")
        kpi_col2.metric("Total Datasets", f"{total_datasets}")
        kpi_col3.metric("Coverage (%)", f"{coverage:.2f}%")
        kpi_col4.metric("Total Fail Checks", f"{total_fail_checks}")

        # Chart: Pass/Fail in last 14 days
        checks_df["created_at"] = pd.to_datetime(checks_df["created_at"], utc=True).dt.tz_localize(None)
        now = pd.Timestamp.now()
        start_date = now - pd.Timedelta(days=14)
        df_14 = checks_df[checks_df["created_at"] >= start_date].copy()

        df_14["created_date"] = df_14["created_at"].dt.date
        grouped = df_14.groupby(["created_date", "outcome"])["id"].count().reset_index()
        grouped.rename(columns={"id": "check_count"}, inplace=True)

        pivoted = grouped.pivot_table(
            index="created_date",
            columns="outcome",
            values="check_count",
            aggfunc="sum",
            fill_value=0
        ).reset_index()

        for outcome_col in ["pass", "fail"]:
            if outcome_col not in pivoted.columns:
                pivoted[outcome_col] = 0

        pivoted.sort_values("created_date", inplace=True)

        st.write("#### Checks (Pass/Fail) in the Last 14 Days")
        st.bar_chart(pivoted.set_index("created_date")[["pass", "fail"]])

    # ================
    # TAB 2: DATASETS
    # ================
    with tab_datasets:
        st.subheader("Datasets Overview")

        # Group to get table-level metrics
        grouped_df = checks_df.groupby(["schema_name", "table_name"], dropna=False).agg(
            pass_count=("outcome", lambda x: (x == "pass").sum()),
            fail_count=("outcome", lambda x: (x == "fail").sum()),
            total_checks=("outcome", "size"),
            last_scan=("created_at", "max")
        ).reset_index()

        # Calculate Health Score
        grouped_df["Health Score"] = grouped_df["pass_count"] / grouped_df["total_checks"]

        # Create dataset column
        grouped_df["dataset"] = grouped_df.apply(
            lambda row: f"{catalog}/{row['schema_name']}/{row['table_name']}",
            axis=1
        )

        # Keep relevant columns
        grouped_df = grouped_df[
            ["dataset", "Health Score", "fail_count", "last_scan"]
        ].sort_values("dataset")

        grouped_df["Health Score"] = grouped_df["Health Score"].fillna(0.0) * 100

        grouped_df.rename(
            columns={
                "dataset": "Dataset",
                "fail_count": "Check Fails",
                "last_scan": "Last Scan"
            },
            inplace=True
        )

        st.dataframe(grouped_df)

    # ================
    # TAB 3: CHECKS
    # ================
    with tab_checks:
        st.subheader("All Checks Overview)")

        # Copy the filtered DataFrame
        df_checks = checks_df.copy()

        # Create a "Dataset" column
        df_checks["Dataset"] = df_checks.apply(
            lambda row: f"{catalog}/{row['schema_name']}/{row['table_name']}",
            axis=1
        )

        # Rename columns for clarity
        df_checks.rename(
            columns={
                "check_name": "Check",
                "definition": "Definition"
            },
            inplace=True
        )

        # Identify unique (Check, Definition, Dataset)
        unique_check_records = df_checks[["Check", "Definition", "Dataset"]].drop_duplicates()

        # For each unique record, show an expander with pass/fail chart by run_id
        for idx, record_row in unique_check_records.iterrows():
            check_name = record_row["Check"]
            definition = record_row["Definition"]
            dataset_str = record_row["Dataset"]

            with st.expander(f"Check: {check_name} | Dataset: {dataset_str}"):
                st.markdown(f"**Definition:** {definition}")

                # Filter for that check & dataset
                filtered_df = df_checks[
                    (df_checks["Check"] == check_name) &
                    (df_checks["Dataset"] == dataset_str)
                ].copy()

                # Group by run_id + outcome -> count pass/fail
                grouped_run = filtered_df.groupby(["run_id", "outcome"])["id"].count().reset_index()
                grouped_run.rename(columns={"id": "check_count"}, inplace=True)

                # Pivot to get pass/fail columns
                pivot_run = grouped_run.pivot_table(
                    index="run_id",
                    columns="outcome",
                    values="check_count",
                    aggfunc="sum",
                    fill_value=0
                ).reset_index()

                # Make sure columns exist
                for ocol in ["pass", "fail"]:
                    if ocol not in pivot_run.columns:
                        pivot_run[ocol] = 0

                pivot_run.sort_values("run_id", inplace=True)

                # Show pass/fail chart
                st.bar_chart(pivot_run.set_index("run_id")[["pass", "fail"]])

                # Optionally, show the raw data
                # st.dataframe(pivot_run)

if __name__ == "__main__":
    main()
