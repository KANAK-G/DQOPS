import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, datetime, timedelta

def load_data():
    # Your DB credentials
    db_username = 'kanakgupta'
    db_password = 'c3RyZWFtYXBwLjU0N2I0NTg4LTBkZDgtNDZkMC05Y2MwLTUyNjU4NDcxOWIwNg==' 
    db_host = 'tcp.unique-haddock.dataos.app'
    db_port = '6432'
    db_name = 'lens:public:coke360'
    
    conn = psycopg2.connect(
        host=db_host,
        database=db_name,
        user=db_username,
        password=db_password,
        port=db_port
    )

    query = """
    SELECT
      *
    FROM
      (SELECT
         *,
         row_number() over (partition by table_name, name order by created_at desc) AS rn
       FROM (
         SELECT
           datasets.labels,
           datasets.owner,
           datasets.description,
           checks.check_value,
           checks.check_type,
           checks.run_id,
           checks.created_at,
           checks.name,
           checks.table_name,
           checks.outcome,
           datasets.schema_name,
           datasets.catalog,
           identity,
           checks.column_name
         FROM
           datasets
           CROSS JOIN checks
       ) as a
      ) as b;
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Clean up the created_at column
    df['created_at'] = pd.to_datetime(df['created_at']).dt.normalize()
    return df

def create_kpis(df: pd.DataFrame):
    """Return a dict with the KPI values."""
    total_datasets = df['table_name'].nunique()
    total_fails = (df['outcome'] == 'fail').sum()
    pass_rate = (df['outcome'] == 'pass').sum() / len(df) * 100 if len(df) else 0.0
    
    return {
        "Number of Datasets Monitored": total_datasets,
        "Total Fails": total_fails,
        "Check Pass Rate": pass_rate  # 0-100
    }

def aggregated_by_table(df: pd.DataFrame):
    """
    Aggregate stats by table_name.
    We'll rename columns after we compute the basic aggregations.
    """
    grouped = df.groupby("table_name", as_index=False).agg(
        CheckFails=('outcome', lambda x: (x == 'fail').sum()),
        Pass=('outcome', lambda x: (x == 'pass').sum()),
        Total=('outcome', 'size'),
        LastScan=('created_at', 'max')
    )
    grouped['Health Score'] = grouped['Pass'] / grouped['Total'] * 100 if len(grouped) else 0
    return grouped

def pass_fail_by_day(df: pd.DataFrame, table_name: str):
    """
    Shows two lines (Pass and Fail counts) aggregated by day
    for the specified table_name.
    """
    # 1) Filter to the desired table
    subset = df[df["table_name"] == table_name].copy()
    if subset.empty:
        return None

    # 2) Convert created_at to a pure date (no time)
    subset["date"] = subset["created_at"].dt.date

    # 3) Group by 'date' to get daily pass/fail counts
    daily = subset.groupby("date", as_index=False).agg(
        pass_count=("outcome", lambda x: (x == "pass").sum()),
        fail_count=("outcome", lambda x: (x == "fail").sum())
    )

    # 4) Reshape (melt) so we can plot pass/fail as separate lines
    daily_melted = daily.melt(
        id_vars="date",
        value_vars=["pass_count", "fail_count"],
        var_name="Outcome",
        value_name="Count"
    )
    # Rename the outcome labels for clarity
    daily_melted["Outcome"] = daily_melted["Outcome"].replace({
        "pass_count": "Pass",
        "fail_count": "Fail"
    })

    # 5) Plot two lines: one for Pass, one for Fail
    fig = px.line(
        daily_melted,
        x="date",
        y="Count",
        color="Outcome",
        markers=True,
        title=f"Daily Pass/Fail Counts: {table_name}"
    )
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Count"
    )
    return fig

############################################
# Time Range Filter (For Page 1 Only)
############################################

def filter_page1_time_range(df: pd.DataFrame, option: str):
    if df.empty:
        return df
    
    today = pd.to_datetime("today").normalize()
    
    if option == "Last 7 Days":
        cutoff = today - pd.Timedelta(days=7)
        return df[df["created_at"] >= cutoff]
    elif option == "Last 10 Days":
        cutoff = today - pd.Timedelta(days=10)
        return df[df["created_at"] >= cutoff]
    elif option == "Last 15 Days":
        cutoff = today - pd.Timedelta(days=15)
        return df[df["created_at"] >= cutoff]
    elif option == "Current Month":
        return df[(df["created_at"].dt.year == today.year) & (df["created_at"].dt.month == today.month)]
    elif option == "Last Month":
        # if current month is january, last month is dec last year
        year, month = today.year, today.month
        if month == 1:
            month = 12
            year -= 1
        else:
            month -= 1
        return df[(df["created_at"].dt.year == year) & (df["created_at"].dt.month == month)]
    else:
        # "All" or none recognized
        return df

############################################
# Streamlit App
############################################

def run_app():
    st.set_page_config(page_title="DQOPS", layout="wide")
    
    # 1) Center the Title
    st.markdown("<h1 style='text-align: center;'>DQOPS</h1>", unsafe_allow_html=True)

    # 2) Load & Cache Data
    @st.cache_data
    def get_data():
        return load_data()
    df = get_data()

    # 3) Sidebar Navigation + Filters
    page = st.sidebar.radio("Go to page:", ["Overview", "Datasets"])
    st.sidebar.write("### Filters By")

    # schema_name filter (multiselect)
    all_schemas = sorted(df["schema_name"].dropna().unique())
    selected_schemas = st.sidebar.multiselect("Schema(s)", all_schemas, default=[])
    if selected_schemas:
        df = df[df["schema_name"].isin(selected_schemas)]
    
    # catalog filter
    all_catalogs = sorted(df["catalog"].dropna().unique())
    selected_catalogs = st.sidebar.multiselect("Catalog(s)", all_catalogs, default=[])
    if selected_catalogs:
        df = df[df["catalog"].isin(selected_catalogs)]
    
    # labels filter
    all_labels = sorted(df["labels"].dropna().unique())
    selected_labels = st.sidebar.multiselect("Labels", all_labels, default=[])
    if selected_labels:
        df = df[df["labels"].isin(selected_labels)]
    
    # 4) Navigate Pages
    if page == "Overview":
        page_overview(df)
    else:
        page_datasets(df)

def page_overview(df: pd.DataFrame):
    """
    The time range filter is *only* applied here, for Page 1.
    """
    # Let user pick time range for Page 1
    time_options = [
        "All",
        "Last 7 Days",
        "Last 10 Days",
        "Last 15 Days",
        "Current Month",
        "Last Month"
    ]
    selected_time = st.selectbox("Select Time Window", time_options, index=0)
    df_overview = filter_page1_time_range(df, selected_time)
    
    # ---- KPIs ----
    kpis = create_kpis(df_overview)
    col3, col1, col2 = st.columns(3)
    col3.metric("Check Pass Rate (%)", f"{kpis['Check Pass Rate']:.1f}%")
    col1.metric("Number of Datasets Monitored", f"{kpis['Number of Datasets Monitored']}")
    col2.metric("Fails", f"{kpis['Total Fails']}")
    
    st.divider()

    # ---- Aggregations ----
    cat_agg = (
        df_overview.groupby("catalog")["outcome"]
                    .value_counts()
                    .unstack(fill_value=0)
                    .reset_index()
    )
    if "fail" not in cat_agg.columns:
        cat_agg["fail"] = 0
    if "pass" not in cat_agg.columns:
        cat_agg["pass"] = 0
    cat_agg.rename(columns={
        'catalog': 'Catalog',
        'fail': 'Fail',
        'pass': 'Pass'
    }, inplace=True)
    cat_agg['Status'] = cat_agg.apply(
        lambda row: 'Green' if row['Pass'] > row['Fail'] else 'Red',
        axis=1
    )
    styled_cat_agg = cat_agg.style.apply(highlight_pass_fail_status, axis=1)
    st.write("#### By Catalog")
    st.dataframe(styled_cat_agg, use_container_width=True)

    check_agg = (
        df_overview.groupby("check_type")["outcome"]
                    .value_counts()
                    .unstack(fill_value=0)
                    .reset_index()
    )
    if "fail" not in check_agg.columns:
        check_agg["fail"] = 0
    if "pass" not in check_agg.columns:
        check_agg["pass"] = 0
    check_agg.rename(columns={
        'check_type': 'Check Category',
        'fail': 'Fail',
        'pass': 'Pass'
    }, inplace=True)
    check_agg['Status'] = check_agg.apply(
        lambda row: 'Green' if row['Pass'] > row['Fail'] else 'Red',
        axis=1
    )
    styled_check_agg = check_agg.style.apply(highlight_pass_fail_status, axis=1)
    st.write("#### By Check Category")
    st.dataframe(styled_check_agg, use_container_width=True)

    schema_agg = (
        df_overview.groupby("schema_name")["outcome"]
                    .value_counts()
                    .unstack(fill_value=0)
                    .reset_index()
    )
    if "fail" not in schema_agg.columns:
        schema_agg["fail"] = 0
    if "pass" not in schema_agg.columns:
        schema_agg["pass"] = 0
    schema_agg.rename(columns={
        'schema_name': 'Schema',
        'fail': 'Fail',
        'pass': 'Pass'
    }, inplace=True)
    schema_agg['Status'] = schema_agg.apply(
        lambda row: 'Green' if row['Pass'] > row['Fail'] else 'Red',
        axis=1
    )
    styled_schema_agg = schema_agg.style.apply(highlight_pass_fail_status, axis=1)
    st.write("#### By Schema")
    st.dataframe(styled_schema_agg, use_container_width=True)

def highlight_pass_fail_status(row):
    """
    Pandas Styler function to color Pass in green, Fail in red,
    and 'Status' in green/red depending on the value.
    """
    styles = [''] * len(row)
    cols = list(row.index)
    
    color_green = 'background-color: #c5f3c5; color: #000;'
    color_red = 'background-color: #f9c5c5; color: #000;'
    
    if 'Pass' in cols:
        pass_idx = cols.index('Pass')
        styles[pass_idx] = color_green
    
    if 'Fail' in cols:
        fail_idx = cols.index('Fail')
        styles[fail_idx] = color_red
    
    if 'Status' in cols:
        status_idx = cols.index('Status')
        if row['Status'] == 'Green':
            styles[status_idx] = color_green
        else:
            styles[status_idx] = color_red
    
    return styles

def page_datasets(df: pd.DataFrame):
    """
    Displays an aggregated table at the dataset level,
    plus a detail section for the selected dataset (row).
    """
    agg_df = aggregated_by_table(df)
    table_display = agg_df[["table_name", "CheckFails", "LastScan", "Health Score"]].copy()
    table_display.rename(columns={
        "table_name": "Dataset",
        "CheckFails": "Check Fails",
        "LastScan": "Last Scan",
        "Health Score": "Health Score"
    }, inplace=True)

    st.subheader("Aggregated Stats by Dataset")
    st.dataframe(table_display, use_container_width=True)
    
    table_names = agg_df["table_name"].unique()
    selected_table = st.selectbox("Select a Dataset:", options=["(None)"] + list(table_names))
    
    if selected_table != "(None)":
        st.markdown(f"### Details for **{selected_table}**")

        show_cols = st.button("View columns", key="show_columns_button")
        if show_cols:
            subset_cols = df[(df["table_name"] == selected_table) & (df["column_name"].notna())]
            column_list = sorted(subset_cols["column_name"].unique())
            if column_list:
                st.write("**Columns in this dataset:**")
                st.write(column_list)
            else:
                st.write("_No columns found for this dataset._")

        meta_cols_raw = ["table_name", "owner", "description", "labels"]
        subset_meta = df[df["table_name"] == selected_table].drop_duplicates(subset=meta_cols_raw).copy()
        subset_meta.rename(columns={
            "table_name": "Dataset",
            "owner": "Owner",
            "description": "Description",
            "labels": "Labels"
        }, inplace=True)

        st.write("**Metadata**:")
        st.dataframe(subset_meta[["Dataset", "Owner", "Description", "Labels"]], use_container_width=True)
        
        fig = pass_fail_by_day(df, selected_table)
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info(f"No data for {selected_table} (Chart).")

        subset_checks = df[(df["table_name"] == selected_table) & (df["rn"] == 1)].copy()
        if subset_checks.empty:
            st.info("No check data found for this dataset.")
        else:
            subset_checks.rename(columns={
                "name": "Check",
                "check_value": "Last Scan Value",
                "outcome": "Recent Result",
                "created_at": "Last Evaluated"
            }, inplace=True)

            display_checks = subset_checks[["Check", "Last Scan Value", "Recent Result", "Last Evaluated"]]
            st.write("**Latest Checks:**")
            st.dataframe(display_checks, use_container_width=True)
    else:
        st.info("Select a dataset from the dropdown above to view details.")

def main():
    run_app()

if __name__ == "__main__":
    main()
