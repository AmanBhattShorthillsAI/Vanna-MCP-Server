# Vanna AI MCP Server

This project implements a Model Context Protocol (MCP) server using Vanna AI for natural language to SQL translation and SQL execution over a financial database. It features:

- **Natural language to SQL generation** using Vanna AI and Azure OpenAI
- **SQL execution** against a SQLite database
- **Full schema and documentation context** provided to the LLM for accurate SQL
- **Excel logging** of all queries, prompts, LLM token usage, cost, timing, and results
- **Hot reload workflow** for rapid development
- **Graceful shutdown** and robust error handling

## Features

- **ask_sql**: Converts a natural language question to a SQL query using the LLM, logs all details to `query_log.xlsx`.
- **run_sql**: Executes a SQL query and logs execution time and results to Excel.
- **LLM token/cost tracking**: Logs input/output tokens and estimated cost for each LLM call.
- **Signal handling**: Clean shutdown on Ctrl+C or kill.
- **Hot reload**: Easily restart both server and Inspector for rapid iteration.

## Setup

### 1. Clone the repository
```sh
git clone <your-repo-url>
cd <your-repo-directory>
```

### 2. Install Python dependencies
```sh
pip install -r requirements.txt
```

### 3. Install Node.js Inspector (optional, for UI)
```sh
npm install -g @modelcontextprotocol/inspector
```

### 4. Set up environment variables
Create a `.env` file with your credentials:
```
OPENAI_API_KEY=your-openai-key
AZURE_OPENAI_ENDPOINT=your-azure-endpoint
WEAVIATE_URL=your-weaviate-url
WEAVIATE_API_KEY=your-weaviate-key
```

### 5. Prepare the SQLite database
Place your `financial.sqlite` database in the project root.

### 6. (Optional) Train Vanna AI
Run your training script (e.g., `train.py`) once to populate the vector store.

## Usage

### Start the MCP server (with hot reload)
```sh
pip install watchfiles
watchfiles "uv run mcp dev app.py" .
```

### Start the MCP Inspector (UI)
```sh
mcp-inspector
```

### (Optional) Open the Inspector in your browser
```sh
open http://localhost:6277  # macOS
# or
xdg-open http://localhost:6277  # Linux
```

## Logging
- All queries, prompts, LLM token usage, cost, timing, and results are logged to `query_log.xlsx`.
- Each new query appends a row; SQL execution updates the last row with fetch time and result.

## Graceful Shutdown
- The server handles SIGINT/SIGTERM for clean shutdown and port release.

## Customization
- Adjust LLM cost calculation in `calculate_cost()` as needed.
- Update schema, documentation, and training data in `app.py` as your database evolves.

## Troubleshooting
- If you see "Not connected" errors in the Inspector, restart both the server and Inspector.
- Ensure all environment variables are set and the database is present.

## License
MIT 