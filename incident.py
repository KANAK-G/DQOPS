import streamlit as st
import pandas as pd
import requests
from trino.auth import BasicAuthentication
from trino.dbapi import connect

#########################
# BACKEND HELPER CODE
#########################

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
        SELECT DISTINCT
            name as check_name,
            definition,
            outcome,
            table_name
        FROM "icebase"."coke_dummy".checks
    """)
    data = cur.fetchall()
    df = pd.DataFrame(data, columns=["check_name", "definition", "outcome", "table_name"])
    return df

def get_table_lineage(api_key, env_name, catalog, schema, table_name):
    # Construct the fully qualified name
    fully_qualified_name = f"{catalog}.{catalog}.{schema}.{table_name}"

    # Construct the URL
    url = f"https://{env_name}/metis/api/v1/lineage/table/name/{fully_qualified_name}?upstreamDepth=2&downstreamDepth=2"
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    return response.json()

def parse_downstream_lineage(lineage_json):
    nodes = lineage_json.get("nodes", [])
    node_lookup = { node["id"]: node for node in nodes }
    downstream_edges = lineage_json.get("downstreamEdges", [])

    results = []
    for edge in downstream_edges:
        from_id = edge["fromEntity"]
        to_id   = edge["toEntity"]

        from_node = node_lookup.get(from_id)
        to_node   = node_lookup.get(to_id)

        workflow_name  = None
        downstream_tbl = None

        # Identify dataosJob node (workflow)
        if from_node and from_node["type"] == "dataosJob":
            full_wf_fqn = from_node.get("fullyQualifiedName", "")
            parts = full_wf_fqn.split(".")
            if len(parts) > 2:
                workflow_name = parts[2]

        # Identify downstream table node
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

#########################
# STREAMLIT APP
#########################

def main():
    st.set_page_config(page_title="incident_management", layout="wide")

    # Initialize session state items if they don't exist
    if "downstream_workflows" not in st.session_state:
        st.session_state.downstream_workflows = pd.DataFrame()
    if "checks_df" not in st.session_state:
        st.session_state.checks_df = pd.DataFrame()
    if "api_key" not in st.session_state:
        st.session_state.api_key = ""
    if "env_name" not in st.session_state:
        st.session_state.env_name = ""
    if "catalog" not in st.session_state:
        st.session_state.catalog = ""
    if "schema" not in st.session_state:
        st.session_state.schema = ""
    if "table_name" not in st.session_state:
        st.session_state.table_name = ""
    # Each rule looks like:
    # {
    #   "action_type": "Alert Only" or "Alert + Delete",
    #   "check_name": ...,
    #   "desired_outcome": "Pass" or "Fail",
    #   "workflow_name": (optional),
    #   "teams_webhook": ...
    # }
    if "rules" not in st.session_state:
        st.session_state.rules = []

    tab_home, tab_manage = st.tabs(["Home", "Manage"])

    ######## HOME TAB ########
    with tab_home:
        st.header("Incident Management - Home")

        api_key = st.text_input("API Key", type="password")
        env_name = st.text_input("Env Name", value="unique-haddock.dataos.app")
        catalog = st.text_input("Catalog Name", value="icebase")
        schema = st.text_input("Schema Name", value="customer_relationship_management")
        table_name = st.text_input("Table Name", value="crm_raw_data")
        
        # Button to get lineage
        if st.button("Get Downstream Workflows"):
            if not api_key or not catalog or not schema or not table_name:
                st.warning("Please provide all required fields.")
            else:
                try:
                    lineage_json = get_table_lineage(api_key, env_name, catalog, schema, table_name)
                    results = parse_downstream_lineage(lineage_json)

                    if not results:
                        st.info("No downstream workflows/datasets found.")
                    else:
                        # Optional search filter
                        search_str = st.text_input("Search (workflow or dataset name)")
                        if search_str:
                            filtered = [
                                r for r in results 
                                if search_str.lower() in r["downstream_workflow_name"].lower()
                                or search_str.lower() in r["downstream_datasets"].lower()
                            ]
                        else:
                            filtered = results

                        # Display table
                        df = pd.DataFrame(filtered)
                        st.dataframe(df, use_container_width=True)

                        st.write("#### Switch to the Actions tab.")
                except Exception as e:
                    st.error(f"Error retrieving lineage: {str(e)}")

        if st.button("Fetch Table's Downstream and Checks"):
            if not api_key or not catalog or not schema or not table_name:
                st.warning("Please provide all required fields.")
            else:
                try:
                    st.session_state.api_key = api_key
                    st.session_state.env_name = env_name
                    st.session_state.catalog = catalog
                    st.session_state.schema = schema
                    st.session_state.table_name = table_name

                    # Downstream Workflows
                    lineage_json = get_table_lineage(api_key, env_name, catalog, schema, table_name)
                    results = parse_downstream_lineage(lineage_json)
                    wf_df = pd.DataFrame(results)
                    st.session_state.downstream_workflows = wf_df

                    # Checks for this table
                    all_checks = get_checks_df(api_key, env_name)
                    filtered_checks = all_checks[all_checks["table_name"] == table_name]
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


    ######## MANAGE TAB ########
    with tab_manage:
        st.header("Manage Tab")

        # Quick checks if we have data
        if st.session_state.downstream_workflows.empty:
            st.info("No downstream workflows found. Please fetch them in the Home tab.")
        if st.session_state.checks_df.empty:
            st.info("No checks found. Please fetch them in the Home tab.")

        if st.session_state.checks_df.empty:
            return  # Stop if no checks at all.

        st.subheader("Add a New Rule")
        with st.form("add_rule_form", clear_on_submit=True):
            action_type = st.selectbox("Action Type", ["Alert Only", "Alert + Delete"])

            # Let user pick a check from checks_df
            check_options = st.session_state.checks_df["check_name"].unique().tolist()
            selected_check = st.selectbox("Select a Check Name", check_options)

            # The user chooses the outcome they'd like to trigger on
            desired_outcome = st.selectbox("Desired Outcome to Trigger", ["pass", "fail"])

            workflow_name = ""
            if action_type == "Alert + Delete":
                if st.session_state.downstream_workflows.empty:
                    st.warning("No workflows available for deletion.")
                else:
                    wf_names = (
                        st.session_state.downstream_workflows["downstream_workflow_name"]
                        .unique()
                        .tolist()
                    )
                    workflow_name = st.selectbox("Workflow to Delete", wf_names)

            teams_webhook_url = st.text_input("Teams Webhook URL (optional)")

            if st.form_submit_button("Add Rule"):
                # Validate if needed
                if action_type == "Alert + Delete" and not workflow_name:
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
        if not st.session_state.rules:
            st.write("No rules defined yet.")
        else:
            rules_df = pd.DataFrame(st.session_state.rules)
            st.dataframe(rules_df, use_container_width=True)

            # Button to "Trigger" all rules against the *current* outcomes
            st.markdown("### Trigger the Rules Now?")
            if st.button("Trigger All Rules"):
                trigger_all_rules()

def trigger_all_rules():
    """
    For each rule, check if the *current outcome* in checks_df 
    matches the rule's desired outcome. If yes, do the action.
    """
    checks_df = st.session_state.checks_df
    api_key   = st.session_state.api_key

    for i, rule in enumerate(st.session_state.rules, start=1):
        check_name      = rule["check_name"]
        desired_outcome = rule["desired_outcome"]
        action_type     = rule["action_type"]
        workflow_name   = rule["workflow_name"]
        teams_webhook   = rule["teams_webhook"]

        # Find the current outcome in checks_df
        row = checks_df[checks_df["check_name"] == check_name]
        if row.empty:
            st.warning(f"[Rule {i}] Check '{check_name}' not found in checks_df.")
            continue

        current_outcome = row["outcome"].iloc[0]

        if current_outcome == desired_outcome:
            # This rule is triggered
            st.write(f"**[Rule {i}]** TRIGGERED for check: {check_name} (current outcome={current_outcome}).")

            # Alert
            alert_msg = f"Check '{check_name}' has outcome '{current_outcome}', rule triggered."
            send_alert(teams_webhook, alert_msg)

            # If "Alert + Delete," also delete the workflow
            if action_type == "Alert + Delete" and workflow_name:
                success, msg = delete_workflow(api_key, workflow_name)
                if success:
                    st.success(msg)
                else:
                    st.error(msg)
        else:
            # Not triggered
            st.write(f"[Rule {i}] NOT triggered. Current outcome='{current_outcome}', desired='{desired_outcome}'.")

#############################
# Run the app
#############################
if __name__ == "__main__":
    main()
