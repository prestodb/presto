# Presto MCP Server

A lightweight server that enables AI applications like Claude Desktop to interact with Presto using the Model Context Protocol (MCP).

## Features

- **5 MCP Tools**: List catalogs, schemas, tables, columns, and execute read-only SQL queries
- **Read-Only Enforcement**: Blocks INSERT, UPDATE, DELETE, CREATE, DROP operations
- **Auto Query Limits**: Applies 10-row limit to prevent large result sets by default
- **Dual Transport**: STDIO and HTTP
- **Authentication**: Basic Auth or Bearer Token

## Quick Start

### 1. Build

```bash
cd presto-mcp
mvn clean package -DskipTests
```

### 2. Configure

Edit `etc/mcp-server-config.properties`:

```properties
presto.server.uri=http://localhost:8080
presto.server.user=your-username
```

### 3. Setup Claude Desktop

**macOS Config Location:**
```bash
~/Library/Application Support/Claude/claude_desktop_config.json
```

**Edit Config:**
```json
{
  "mcpServers": {
    "presto": {
      "command": "java",
      "args": [
        "-Dconfig=/absolute/path/to/presto-mcp/etc/mcp-server-config.properties",
        "-jar",
        "/absolute/path/to/presto-mcp/target/presto-mcp-0.297-SNAPSHOT-mcp.jar"
      ]
    }
  }
}
```

### 4. Restart Claude Desktop

Close and reopen Claude Desktop. Look for 🔌 icon indicating connection.

### 5. Check Logs (if needed)

```bash
# macOS
tail -f ~/Library/Logs/Claude/mcp-server-presto.log
```

## Sample Queries for Claude

### Basic Queries

**List available tools:**
```
What tools are available in Presto?
```

**List catalogs:**
```
Which catalogs are available in Presto?
```

**List schemas:**
```
What schemas are in the tpch catalog?
```

**List tables:**
```
Show me all tables in the tpch.sf1 schema
```

**Get columns:**
```
What columns does the customer table have in tpch.sf1?
```

### Exploration Query

**Multi-step exploration:**
```
Can you explore the tpch catalog, find out what tables are in the sf1 schema, 
and tell me what columns exist in the customer table?
```

**Expected Response:**
1. Lists schemas in `tpch` catalog
2. Lists tables in `tpch.sf1` schema  
3. Shows columns in `tpch.sf1.customer` table

### Data Queries

**Simple SELECT:**
```
Show me the first 10 customers from tpch.sf1.customer
```

**With filtering:**
```
Show me customers from nation 5 in tpch.sf1.customer, limit to 5 results
```

**Aggregation:**
```
How many customers are in each market segment in tpch.sf1.customer?
```

**Complex analysis:**
```
What's the structure of the orders table in tpch.sf1? 
Then show me the top 5 orders by total price.
```

## Available Tools

| Tool | Description |
|------|-------------|
| `presto_metadata_list_catalogs` | List all Presto catalogs |
| `presto_metadata_list_schemas` | List schemas in a catalog |
| `presto_metadata_list_tables` | List tables in a schema |
| `presto_metadata_get_columns` | Get column information |
| `presto_query_run` | Execute read-only SQL queries |

## Security

### Read-Only Operations
✅ **Allowed:** SELECT, SHOW, DESCRIBE, EXPLAIN, WITH  
❌ **Blocked:** INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, TRUNCATE, GRANT, REVOKE

### Query Limits
- Automatic 10-row limit on queries without explicit LIMIT
- Can be overridden by specifying LIMIT in query

### Authentication
- Credentials forwarded to Presto
- All authorization handled by Presto coordinator

## Testing HTTP Mode (Optional)

### Start HTTP Server
```bash
java -Dconfig=etc/mcp-server-http-config.properties \
     -jar target/presto-mcp-0.297-SNAPSHOT-mcp.jar
```

### Test with MCP Inspector
```bash
npx @modelcontextprotocol/inspector http://localhost:18080/mcp
```

Open browser at `http://localhost:5173`

## Configuration Options

| Property | Required | Default | Description                |
|----------|----------|---------|----------------------------|
| `presto.server.uri` | Yes | -       | Presto coordinator URL     |
| `presto.server.user` | No* | -       | Username (Basic Auth)      |
| `presto.server.password` | No* | -       | Password (Basic Auth)      |
| `presto.server.token` | No* | -       | Bearer token (OAuth/JWT)   |
| `http-server.http.port` | No | 18080   | HTTP port (HTTP mode only) |
| `mcp.default-limit` | No | 10      | Default result set limit   |

*Either `user` OR `user`+`password` OR `token` required

## Troubleshooting

### Claude Desktop Not Connecting

1. **Check paths are absolute** (no `~` or relative paths)
2. **View logs:**
   ```bash
   tail -f ~/Library/Logs/Claude/mcp*.log
   ```
3. **Test manually:**
   ```bash
   java -Dconfig=/path/to/config.properties -jar /path/to/presto-mcp.jar
   ```
4. **Verify Presto is running:**
   ```bash
   curl http://localhost:8080/v1/info
   ```

### Query Failures

- **Read-only error:** Ensure query is SELECT/SHOW/DESCRIBE/EXPLAIN
- **Permission error:** Check Presto user permissions
- **Syntax error:** Verify SQL syntax

## Architecture

```
Claude Desktop
    ↓ (JSON-RPC over STDIO)
MCP Server
    ↓ (HTTP + Auth)
Presto Coordinator
    ↓
Data Sources
```

## Example Session

**User:** "List available tools in Presto"

**Claude:** Shows 5 MCP tools with descriptions

---

**User:** "Which catalogs are available in Presto?"

**Claude:** Lists catalogs (e.g., tpch, hive, system)

---

**User:** "Can you explore the tpch catalog, find out what tables are in the sf1 schema, and tell me what columns exist in the customer table?"

**Claude:** 
1. Lists schemas: sf1, sf100, sf1000, tiny
2. Lists tables: customer, lineitem, nation, orders, part, partsupp, region, supplier
3. Shows customer columns: custkey, name, address, nationkey, phone, acctbal, mktsegment, comment

---

**User:** "Show me the first 10 customers from tpch.sf1.customer"

**Claude:** Executes query and displays results in table format

## Related Links

- [Model Context Protocol](https://modelcontextprotocol.io/)
- [Presto Documentation](https://prestodb.io/docs/current/)
- [Claude Desktop](https://claude.ai/)