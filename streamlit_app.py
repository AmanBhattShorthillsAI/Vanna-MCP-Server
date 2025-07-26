import streamlit as st
import requests

API_URL = "http://localhost:8000"

# Initialize session state for recent queries (as lists for last 3)
if 'recent_questions' not in st.session_state:
    st.session_state['recent_questions'] = []
if 'recent_sqls' not in st.session_state:
    st.session_state['recent_sqls'] = []

st.title("Vanna AI MCP Streamlit UI")

# Display last three recent queries at the top
with st.expander("Recent Activity", expanded=True):
    st.markdown("**Last 3 Natural Language Questions:**")
    if st.session_state['recent_questions']:
        for q in st.session_state['recent_questions'][-1:-4:-1]:
            st.markdown(f"- {q}")
    else:
        st.markdown('_None yet_')
    st.markdown("**Last 3 Generated SQL Queries:**")
    if st.session_state['recent_sqls']:
        for sql in st.session_state['recent_sqls'][-1:-4:-1]:
            st.code(sql, language="sql")
    else:
        st.code('None yet', language="sql")

st.sidebar.header("API Endpoints")
endpoint = st.sidebar.selectbox("Choose endpoint", ["/context", "/generate", "/execute"])

if endpoint == "/context":
    st.header("View Model Context (/context)")
    if st.button("Fetch Context"):
        try:
            resp = requests.get(f"{API_URL}/context")
            resp.raise_for_status()
            data = resp.json()
            st.subheader("Schema (DDL)")
            for ddl in data.get("schema", []):
                st.code(ddl, language="sql")
            st.subheader("Documentation")
            for doc in data.get("documentation", []):
                st.markdown(doc)
        except Exception as e:
            st.error(f"Error fetching context: {e}")

elif endpoint == "/generate":
    st.header("Generate SQL from Question (/generate)")
    question = st.text_area("Enter your natural language question:")
    if st.button("Generate SQL"):
        if not question.strip():
            st.warning("Please enter a question.")
        else:
            with st.spinner("Generating SQL, please wait..."):
                try:
                    resp = requests.post(f"{API_URL}/generate", json={"question": question})
                    resp.raise_for_status()
                    data = resp.json()
                    st.subheader("Generated SQL")
                    st.code(data.get("sql", "No SQL returned."), language="sql")
                    st.write("Question:", data.get("question", ""))
                    if "reasoning" in data:
                        st.write("Reasoning:", data["reasoning"])
                    # Store last 3 recent queries
                    st.session_state['recent_questions'].append(question)
                    st.session_state['recent_questions'] = st.session_state['recent_questions'][-3:]
                    st.session_state['recent_sqls'].append(data.get("sql", "No SQL returned."))
                    st.session_state['recent_sqls'] = st.session_state['recent_sqls'][-3:]
                except Exception as e:
                    st.error(f"Error generating SQL: {e}")

elif endpoint == "/execute":
    st.header("Execute SQL Query (/execute)")
    sql = st.text_area("Enter SQL to execute:")
    if st.button("Execute SQL"):
        if not sql.strip():
            st.warning("Please enter a SQL query.")
        else:
            try:
                resp = requests.post(f"{API_URL}/execute", json={"sql": sql})
                resp.raise_for_status()
                data = resp.json()
                if "result" in data:
                    st.subheader("Query Result")
                    st.write(data["result"])
                else:
                    st.write(data)
            except Exception as e:
                st.error(f"Error executing SQL: {e}") 
