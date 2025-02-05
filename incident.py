import streamlit as st
import pandas as pd
import requests
from trino.auth import BasicAuthentication
from trino.dbapi import connect

############################
# HELPER FUNCTIONS
############################

def get_checks_df(api_key, env_name):
    """
    Connects to Trino using:
      - host = tcp.<env_name>
      - user_name = "kanakgupta"
      - cluster_name = "miniature"
      - password = api_key
    Returns a DataFrame of checks with columns:
      check_name, definition, outcome, table_name
    """
    host_name = f"tcp.{env_name}"
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
    cur.execute("""
            SELECT
            check_name,
            definition,
            outcome,
            table_name
            FROM
            (
                SELECT
                name AS check_name,
                definition,
                outcome,
                table_name,
                created_at,
                row_number() over (PARTITION by name ORDER BY created_at DESC) AS rn
                FROM
                "icebase"."coke_dummy".checks
            )
            WHERE
            rn = 1
    """)
    data = cur.fetchall()
    df = pd.DataFrame(data, columns=["check_name", "definition", "outcome", "table_name"])
    return df

def get_schemas(api_key, catalog):
    """
    Returns a list of schema names for a given catalog
    using the Workbench Meta-Info API.
    """
    url = f"https://unique-haddock.dataos.app/workbench/api/meta-info/presto/{catalog}?&routingName=miniature"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    return data.get("schemas", [])

def get_tables(api_key, catalog, schema_name):
    """
    Returns a list of tables for a given catalog & schema.
    """
    url = f"https://unique-haddock.dataos.app/workbench/api/meta-info/presto/{catalog}/{schema_name}?&routingName=miniature"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    return data.get("tables", [])

def get_table_lineage(api_key, env_name, catalog, schema_name, table_name):
    """
    Calls the Metis lineage API for the given table.
    """
    fqn = f"{catalog}.{catalog}.{schema_name}.{table_name}"
    url = f"https://{env_name}/metis/api/v1/lineage/table/name/{fqn}?upstreamDepth=2&downstreamDepth=2"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def parse_downstream_lineage(lineage_json):
    """
    Parse lineage data into a list of {downstream_workflow_name, downstream_datasets}.
    """
    nodes = lineage_json.get("nodes", [])
    node_lookup = {node["id"]: node for node in nodes}
    edges = lineage_json.get("downstreamEdges", [])

    results = []
    for edge in edges:
        from_node = node_lookup.get(edge["fromEntity"])
        to_node = node_lookup.get(edge["toEntity"])

        workflow_name = None
        downstream_tbl = None

        if from_node and from_node["type"] == "dataosJob":
            wf_fqn = from_node.get("fullyQualifiedName", "")
            parts = wf_fqn.split(".")
            if len(parts) > 2:
                workflow_name = parts[2]
        if to_node and to_node["type"] == "table":
            downstream_tbl = to_node.get("fullyQualifiedName", "")

        if workflow_name and downstream_tbl:
            results.append({
                "downstream_workflow_name": workflow_name,
                "downstream_datasets": downstream_tbl
            })
    return results

def delete_workflow(api_key, workflow_name):
    """
    Delete a workflow by name. Returns (success_bool, message).
    """
    url = f"https://unique-haddock.dataos.app/poros/api/v1/workspaces/public/resources/workflow/{workflow_name}"
    headers = {"Authorization": f"Bearer {api_key}"}
    resp = requests.delete(url, headers=headers)
    if resp.status_code in [200, 204]:
        return True, f"Successfully deleted workflow: {workflow_name}"
    else:
        return False, (
            f"Failed to delete workflow '{workflow_name}'. "
            f"Status: {resp.status_code}, Details: {resp.text}"
        )
        
############################
# ALERT / NOTIFICATION FUNCTIONS
############################

def post_to_teams(teams_webhook_url: str, message: str):
    payload = {"text": message}
    response = requests.post(teams_webhook_url, json=payload)
    response.raise_for_status()

def send_alert(teams_webhook_url: str, message: str):
    """
    For demo, we only do a Teams webhook if provided.
    """
    if not teams_webhook_url:
        st.write("No Teams webhook URL provided. Skipping alert.")
        return
    try:
        post_to_teams(teams_webhook_url, message)
        st.write(f"Alert posted to Teams: {teams_webhook_url}")
    except Exception as e:
        st.error(f"Error posting to Teams: {e}")


############################
# STREAMLIT APP
############################

def main():
    st.set_page_config(page_title="incident_management", layout="wide")

    # --- Constants (hardcoded) ---
    # These values are no longer asked from the user.
    st.session_state["api_key"] = "c3RyZWFtYXBwLjU0N2I0NTg4LTBkZDgtNDZkMC05Y2MwLTUyNjU4NDcxOWIwNg=="
    env_name = "unique-haddock.dataos.app"
    catalog = "icebase"

    # Initialize session_state variables if not present
    if "workflow_names" not in st.session_state:
        st.session_state["workflow_names"] = []
    if "schemas" not in st.session_state:
        st.session_state["schemas"] = []
    if "tables" not in st.session_state:
        st.session_state["tables"] = []
    if "downstream_workflows" not in st.session_state:
        st.session_state.downstream_workflows = pd.DataFrame()
    if "checks_df" not in st.session_state:
        st.session_state.checks_df = pd.DataFrame()

    # Automatically load schemas if we don't have them yet
    if not st.session_state["schemas"]:
        try:
            schemas_list = get_schemas(st.session_state["api_key"], catalog)
            st.session_state["schemas"] = schemas_list
        except Exception as e:
            st.error(f"Error fetching schemas: {e}")
    if "rules" not in st.session_state:
        st.session_state.rules = []


    st.header("Incident App")

    catalog = st.text_input("Catalog Name", value="icebase")
    # 1) Dropdown to select schema (auto-loaded above)
    selected_schema = st.selectbox(
        "Select a Schema:",
        ["(None)"] + st.session_state["schemas"]
    )

    # 2) Once a schema is selected, auto-load tables

    if selected_schema and selected_schema != "(None)":
        try:
            tables_list = get_tables(st.session_state["api_key"], catalog, selected_schema)
            st.session_state["tables"] = tables_list
        except Exception as e:
            st.error(f"Error fetching tables: {e}")
    else:
        st.session_state["tables"] = []

    # 3) Dropdown for tables

    selected_table = st.selectbox(
        "Select a Table:",
        ["(None)"] + st.session_state["tables"] if st.session_state["tables"] else ["(None)"]
    )


    if st.button("Fetch Checks and Downstream Workflows defined on this Dataset"):
        if  not selected_schema or not selected_table:
            st.warning("Please provide all required fields.")
        else:
            try:
                st.session_state.api_key = st.session_state["api_key"],
                st.session_state.env_name = env_name
                st.session_state.catalog = catalog
                st.session_state.schema = selected_schema
                st.session_state.table_name = selected_table

                # Downstream Workflows
                lineage_json = get_table_lineage(api_key, env_name, catalog, selected_schema, selected_table)
                results = parse_downstream_lineage(lineage_json)
                wf_df = pd.DataFrame(results)
                st.session_state.downstream_workflows = wf_df

                # Checks for this table
                all_checks = get_checks_df(api_key, env_name)
                filtered_checks = all_checks[all_checks["table_name"] == selected_table]
                st.session_state.checks_df = filtered_checks

                if wf_df.empty:
                    st.info("No downstream workflows found for this table.")
                else:
                    st.success("Downstream workflows fetched and stored.")

                if filtered_checks.empty:
                    st.info("No checks found for this table.")
                else:
                    st.success("Relevant checks fetched and stored.")
            except Exception as e:
                st.error(f"Error fetching data: {str(e)}")
    if not st.session_state.downstream_workflows.empty:
        st.dataframe(st.session_state.downstream_workflows, use_container_width=True)



    # Quick checks if we have data
    if st.session_state.downstream_workflows.empty:
        st.info("No downstream workflows found. Please fetch them.")
    if st.session_state.checks_df.empty:
        st.info("No checks found. Please fetch them.")

    if st.session_state.checks_df.empty:
        return  # Stop if no checks at all.
    
    st.subheader("Add a New Rule")
    with st.form("add_rule_form", clear_on_submit=True):
        action_type = st.selectbox("Action Type", ["Alert Only", "Alert + Pipeline Break"])

        # Let user pick a check from checks_df
        check_options = st.session_state.checks_df["check_name"].unique().tolist()
        selected_check = st.selectbox("Select a Check Name", check_options)

        # The user chooses the outcome they'd like to trigger on
        desired_outcome = st.selectbox("Desired Check Outcome for Trigger", ["pass", "fail"])

        # We set a default workflow_name to an empty string
        workflow_name = ""

        # Show the workflow dropdown ONLY if the user selected "Alert + Pipeline Break"
        if action_type == "Alert + Pipeline Break":
            if st.session_state.downstream_workflows.empty:
                st.warning(
                    "No downstream workflows have been fetched yet. "
                    "Click 'Fetch Checks defined on this Dataset' above."
                )
                wf_names = []
            else:
                wf_names = (
                    st.session_state.downstream_workflows["downstream_workflow_name"]
                    .unique()
                    .tolist()
                )

            # Now show the dropdown to pick which workflow to delete
            workflow_name = st.selectbox("Workflow to Delete", ["(None)"] + wf_names, index=0)
            
            # If user leaves it on "(None)", treat it as empty
            if workflow_name == "(None)":
                workflow_name = ""

        teams_webhook_url = st.text_input("Teams Webhook URL")

        if st.form_submit_button("Add Rule"):
            # If we are in "Pipeline Break" mode but have no actual workflow, warn the user
            if action_type == "Alert + Pipeline Break" and not workflow_name:
                st.warning("Please select a workflow to delete.")
                st.stop()

            new_rule = {
                "action_type": action_type,
                "check_name": selected_check,
                "desired_outcome": desired_outcome,
                "workflow_name": workflow_name,
                "teams_webhook": teams_webhook_url
            }
            st.session_state.rules.append(new_rule)
            st.success("Rule added!")

    st.divider()

    # Show existing rules
    st.subheader("Current Rules")

    rules_df = pd.DataFrame(st.session_state.rules)
    st.dataframe(rules_df, use_container_width=True)

    # Add Delete buttons
    for i, rule in enumerate(st.session_state.rules):
        # A unique key for each button to avoid collisions
        if st.button(f"Delete Rule #{i+1}", key=f"delete_rule_{i}"):
            # Remove that rule by index
            st.session_state.rules.pop(i)
            st.experimental_rerun()
            
# Additional UI to choose a rule to trigger
    st.markdown("### Trigger the Rules Now?")

    rule_names = [f"Rule #{i+1}: {rule['check_name']}" for i, rule in enumerate(st.session_state.rules)]
    selected_rule_index = st.selectbox(
        "Select a Rule to Trigger (or skip to trigger all)", 
        options=["(None)"] + rule_names
    )

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Trigger Selected Rule"):
            if selected_rule_index == "(None)":
                st.warning("Please select a rule or use 'Trigger All Rules'.")
            else:
                # Figure out which index was chosen
                idx = rule_names.index(selected_rule_index)
                selected_rule = st.session_state.rules[idx]
                trigger_rule(selected_rule, idx+1)

    with col2:
        if st.button("Trigger All Rules"):
            trigger_all_rules()

def trigger_rule(rule, rule_index=None):
    """
    Trigger a single rule (alert + optional workflow deletion).
    rule_index is optional, just for display messages.
    """
    checks_df = st.session_state.checks_df
    api_key   = st.session_state.api_key

    check_name      = rule["check_name"]
    desired_outcome = rule["desired_outcome"]
    action_type     = rule["action_type"]
    workflow_name   = rule["workflow_name"]
    teams_webhook   = rule["teams_webhook"]

    # Find the current outcome in checks_df
    row = checks_df[checks_df["check_name"] == check_name]
    if row.empty:
        st.warning(f"[Rule {rule_index}] Check '{check_name}' not found in checks_df.")
        return

    current_outcome = row["outcome"].iloc[0]

    if current_outcome == desired_outcome:
        # This rule is triggered
        st.write(f"**[Rule {rule_index}]** TRIGGERED for check: {check_name} (current outcome={current_outcome}).")

        # Alert
        alert_msg = f"Check '{check_name}' has outcome '{current_outcome}', rule triggered."
        send_alert(teams_webhook, alert_msg)

        # If "Alert + Pipeline Break," also delete the workflow
        if action_type == "Alert + Pipeline Break" and workflow_name:
            success, msg = delete_workflow(api_key, workflow_name)
            if success:
                st.success(msg)
            else:
                st.error(msg)
    else:
        # Not triggered
        st.write(f"[Rule {rule_index}] NOT triggered. Current outcome='{current_outcome}', desired='{desired_outcome}'.")

def trigger_all_rules():
    """
    For each rule, check if the *current outcome* in checks_df 
    matches the rule's desired outcome. If yes, do the action.
    """
    for i, rule in enumerate(st.session_state.rules, start=1):
        trigger_rule(rule, i)

api_key="c3RyZWFtYXBwLjU0N2I0NTg4LTBkZDgtNDZkMC05Y2MwLTUyNjU4NDcxOWIwNg=="
#############################
# Run the app
#############################
if __name__ == "__main__":
    main()
