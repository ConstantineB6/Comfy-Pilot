-- DuckLake of Claude Code Interactions
-- Run with: duckdb < analytics/ducklake_init.sql
-- Or interactively: duckdb -c ".read analytics/ducklake_init.sql"

INSTALL ducklake;
LOAD ducklake;

-- Attach DuckLake catalog (creates if not exists)
ATTACH 'ducklake:~/.claude/interactions.ducklake' AS lake;

-------------------------------------------------------
-- 1. Command History (from history.jsonl)
-------------------------------------------------------
CREATE OR REPLACE TABLE lake.command_history AS
SELECT
  display AS command,
  epoch_ms(timestamp) AS ts,
  project,
  sessionId AS session_id,
  -- derived fields
  CASE
    WHEN display LIKE '/%' THEN 'slash_command'
    WHEN display LIKE 'git %' THEN 'git'
    WHEN display = 'confirm' THEN 'confirm'
    ELSE 'prompt'
  END AS command_type,
  regexp_extract(project, '/([^/]+)$') AS project_name
FROM read_json_auto(
  '~/.claude/history.jsonl'
);

-------------------------------------------------------
-- 2. Conversations (from project .jsonl files)
--    Reads ALL project conversations into one table
-------------------------------------------------------
CREATE OR REPLACE TABLE lake.conversations AS
SELECT
  type AS event_type,
  CASE WHEN timestamp IS NOT NULL
    THEN timestamp::TIMESTAMP
    ELSE NULL
  END AS ts,
  sessionId AS session_id,
  uuid AS event_id,
  parentUuid AS parent_event_id,
  cwd AS working_dir,
  version AS claude_version,
  coalesce(isSidechain, false) AS is_sidechain,
  -- extract project name from file path
  regexp_extract(filename, 'projects/([^/]+)/', 1) AS project_key,
  filename AS source_file
FROM read_json(
  '~/.claude/projects/*/*.jsonl',
  columns={
    type: 'VARCHAR',
    timestamp: 'VARCHAR',
    sessionId: 'VARCHAR',
    uuid: 'VARCHAR',
    parentUuid: 'VARCHAR',
    cwd: 'VARCHAR',
    version: 'VARCHAR',
    isSidechain: 'BOOLEAN'
  },
  maximum_object_size=50000000,
  ignore_errors=true
)
WHERE type IS NOT NULL;

-------------------------------------------------------
-- 3. Materialized views for common queries
-------------------------------------------------------

-- Sessions: one row per session with start/end times
CREATE OR REPLACE TABLE lake.sessions AS
SELECT
  session_id,
  project_key,
  working_dir,
  claude_version,
  min(ts) AS started_at,
  max(ts) AS ended_at,
  age(max(ts), min(ts)) AS duration,
  count(*) FILTER (WHERE event_type = 'user') AS user_messages,
  count(*) FILTER (WHERE event_type = 'assistant') AS assistant_messages,
  count(*) FILTER (WHERE event_type = 'progress') AS tool_calls,
  count(*) AS total_events
FROM lake.conversations
WHERE session_id IS NOT NULL
GROUP BY session_id, project_key, working_dir, claude_version;

-- Daily activity: interaction counts per day
CREATE OR REPLACE TABLE lake.daily_activity AS
SELECT
  ts::DATE AS day,
  project_key,
  count(*) FILTER (WHERE event_type = 'user') AS user_msgs,
  count(*) FILTER (WHERE event_type = 'assistant') AS assistant_msgs,
  count(*) FILTER (WHERE event_type = 'progress') AS tool_uses,
  count(DISTINCT session_id) AS sessions,
  count(*) AS total_events
FROM lake.conversations
WHERE ts IS NOT NULL
GROUP BY day, project_key
ORDER BY day DESC;

-- Hourly heatmap: when do you use Claude Code?
CREATE OR REPLACE TABLE lake.hourly_heatmap AS
SELECT
  extract(dow FROM ts) AS day_of_week,
  extract(hour FROM ts) AS hour,
  count(*) AS interactions,
  count(DISTINCT session_id) AS sessions
FROM lake.conversations
WHERE ts IS NOT NULL AND event_type = 'user'
GROUP BY day_of_week, hour
ORDER BY day_of_week, hour;

-- Project stats
CREATE OR REPLACE TABLE lake.project_stats AS
SELECT
  project_key,
  count(DISTINCT session_id) AS total_sessions,
  count(*) FILTER (WHERE event_type = 'user') AS total_user_msgs,
  count(*) FILTER (WHERE event_type = 'assistant') AS total_assistant_msgs,
  count(*) FILTER (WHERE event_type = 'progress') AS total_tool_calls,
  min(ts) AS first_interaction,
  max(ts) AS last_interaction,
  count(DISTINCT ts::DATE) AS active_days
FROM lake.conversations
WHERE session_id IS NOT NULL
GROUP BY project_key
ORDER BY total_sessions DESC;

-------------------------------------------------------
-- 4. Tool Usage (extracted from assistant messages)
--    Every tool_use invocation across all sessions
-------------------------------------------------------
CREATE OR REPLACE TABLE lake.tool_usage AS
WITH raw_tools AS (
  SELECT
    sessionId AS session_id,
    CASE WHEN timestamp IS NOT NULL THEN timestamp::TIMESTAMP ELSE NULL END AS ts,
    regexp_extract(CAST(message AS VARCHAR), '"name":"([^"]+)"', 1) AS tool_name,
    regexp_extract(filename, 'projects/([^/]+)/', 1) AS project_key,
    CAST(message AS VARCHAR) AS raw_msg
  FROM read_json(
    '~/.claude/projects/*/*.jsonl',
    columns={
      type: 'VARCHAR', timestamp: 'VARCHAR', sessionId: 'VARCHAR',
      message: 'JSON'
    },
    maximum_object_size=50000000,
    ignore_errors=true,
    filename=true
  )
  WHERE type = 'assistant'
    AND CAST(message AS VARCHAR) LIKE '%"type":"tool_use"%'
)
SELECT
  session_id,
  ts,
  tool_name,
  project_key,
  CASE
    WHEN tool_name LIKE 'mcp__%' THEN split_part(tool_name, '__', 2)
    ELSE NULL
  END AS mcp_server,
  CASE
    WHEN tool_name LIKE 'mcp__%' THEN 'mcp'
    WHEN tool_name IN ('Bash','Read','Write','Edit','Glob','Grep','WebFetch','WebSearch') THEN 'builtin'
    WHEN tool_name IN ('Task','Skill','EnterPlanMode','ExitPlanMode') THEN 'agent'
    WHEN tool_name IN ('TodoWrite','TodoRead') THEN 'todo'
    ELSE 'other'
  END AS tool_category
FROM raw_tools
WHERE tool_name IS NOT NULL;

-- Aggregated tool stats
CREATE OR REPLACE TABLE lake.tool_stats AS
SELECT
  tool_name,
  tool_category,
  mcp_server,
  count(*) AS total_calls,
  count(DISTINCT session_id) AS sessions_used,
  count(DISTINCT project_key) AS projects_used,
  min(ts) AS first_used,
  max(ts) AS last_used
FROM lake.tool_usage
GROUP BY tool_name, tool_category, mcp_server
ORDER BY total_calls DESC;

-------------------------------------------------------
-- 5. Babashka Usage (MCP tool deep-dive)
--    Categorized Clojure scripting patterns
-------------------------------------------------------
CREATE OR REPLACE TABLE lake.babashka_usage AS
SELECT
  sessionId AS session_id,
  CASE WHEN timestamp IS NOT NULL THEN timestamp::TIMESTAMP ELSE NULL END AS ts,
  regexp_extract(filename, 'projects/([^/]+)/', 1) AS project_key,
  CASE
    WHEN CAST(message AS VARCHAR) LIKE '%container %'
      AND CAST(message AS VARCHAR) LIKE '%babashka.process%' THEN 'apple-containers'
    WHEN CAST(message AS VARCHAR) LIKE '%babashka.process%'
      OR CAST(message AS VARCHAR) LIKE '%shell {:out%' THEN 'shell-exec'
    WHEN CAST(message AS VARCHAR) LIKE '%babashka.fs%' THEN 'filesystem'
    WHEN CAST(message AS VARCHAR) LIKE '%slurp%' THEN 'file-read'
    WHEN CAST(message AS VARCHAR) LIKE '%spit%'
      OR CAST(message AS VARCHAR) LIKE '%io/writer%' THEN 'file-write'
    WHEN CAST(message AS VARCHAR) LIKE '%cheshire%'
      OR CAST(message AS VARCHAR) LIKE '%data.json%' THEN 'json-processing'
    WHEN CAST(message AS VARCHAR) LIKE '%Base64%' THEN 'encoding'
    WHEN CAST(message AS VARCHAR) LIKE '%http%'
      OR CAST(message AS VARCHAR) LIKE '%curl%' THEN 'network'
    ELSE 'other-clojure'
  END AS usage_category,
  substr(
    regexp_extract(CAST(message AS VARCHAR),
      '"code":"((?:[^"\\\\]|\\\\.)*)"', 1),
    1, 500
  ) AS code_snippet
FROM read_json(
  '~/.claude/projects/*/*.jsonl',
  columns={
    type: 'VARCHAR', timestamp: 'VARCHAR', sessionId: 'VARCHAR',
    message: 'JSON'
  },
  maximum_object_size=50000000,
  ignore_errors=true,
  filename=true
)
WHERE type = 'assistant'
  AND CAST(message AS VARCHAR) LIKE '%"type":"tool_use"%'
  AND CAST(message AS VARCHAR) LIKE '%mcp__babashka__execute%';

-------------------------------------------------------
-- 6. Token Usage (from assistant message metadata)
--    Tracks input/output/cache token consumption
-------------------------------------------------------
CREATE OR REPLACE TABLE lake.token_usage AS
SELECT
  sessionId AS session_id,
  CASE WHEN timestamp IS NOT NULL THEN timestamp::TIMESTAMP ELSE NULL END AS ts,
  regexp_extract(filename, 'projects/([^/]+)/', 1) AS project_key,
  json_extract_string(message, '$.model')::VARCHAR AS model,
  CAST(json_extract(message, '$.usage.input_tokens') AS BIGINT) AS input_tokens,
  CAST(json_extract(message, '$.usage.output_tokens') AS BIGINT) AS output_tokens,
  CAST(json_extract(message, '$.usage.cache_read_input_tokens') AS BIGINT) AS cache_read_tokens,
  CAST(json_extract(message, '$.usage.cache_creation_input_tokens') AS BIGINT) AS cache_creation_tokens
FROM read_json(
  '~/.claude/projects/*/*.jsonl',
  columns={
    type: 'VARCHAR', timestamp: 'VARCHAR', sessionId: 'VARCHAR',
    message: 'JSON'
  },
  maximum_object_size=50000000,
  ignore_errors=true,
  filename=true
)
WHERE type = 'assistant'
  AND json_extract(message, '$.usage') IS NOT NULL
  AND CAST(json_extract(message, '$.usage.input_tokens') AS BIGINT) IS NOT NULL;

-------------------------------------------------------
-- 7. Telemetry Events (from ~/.claude/telemetry/)
-------------------------------------------------------
CREATE OR REPLACE TABLE lake.telemetry AS
SELECT
  json_extract_string(event_data, '$.event_name') AS event_name,
  json_extract_string(event_data, '$.client_timestamp')::TIMESTAMP AS ts,
  json_extract_string(event_data, '$.model') AS model,
  json_extract_string(event_data, '$.session_id') AS session_id,
  json_extract_string(event_data, '$.env.version') AS cli_version,
  json_extract_string(event_data, '$.env.terminal') AS terminal,
  json_extract_string(event_data, '$.env.platform') AS platform,
  json_extract_string(event_data, '$.entrypoint') AS entrypoint,
  json_extract_string(event_data, '$.additional_metadata') AS metadata_json
FROM read_json_auto(
  '~/.claude/telemetry/*.json',
  maximum_object_size=10000000,
  ignore_errors=true
)
WHERE event_type = 'ClaudeCodeInternalEvent';

-------------------------------------------------------
-- 8. Stats Cache (daily activity from Claude Code)
-------------------------------------------------------
CREATE OR REPLACE TABLE lake.stats_cache AS
WITH raw AS (
  SELECT unnest(dailyActivity) AS entry
  FROM read_json_auto('~/.claude/stats-cache.json')
)
SELECT
  (entry->>'date')::DATE AS day,
  (entry->>'messageCount')::INT AS message_count,
  (entry->>'sessionCount')::INT AS session_count,
  (entry->>'toolCallCount')::INT AS tool_call_count
FROM raw;

-------------------------------------------------------
-- 9. Cognitive Science Tables (from history.duckdb)
--    Pre-existing research data: phase transitions,
--    Markov blankets, reafference loops, strange loops
-------------------------------------------------------
ATTACH '~/.claude/history.duckdb' AS cogdb (READ_ONLY);

CREATE OR REPLACE TABLE lake.cognitive_glue AS
SELECT * FROM cogdb.cognitive_glue_unified;

CREATE OR REPLACE TABLE lake.markov_blanket AS
SELECT * FROM cogdb.markov_blanket;

CREATE OR REPLACE TABLE lake.phase_transitions AS
SELECT * FROM cogdb.phase_transitions;

CREATE OR REPLACE TABLE lake.reafference_loop AS
SELECT * FROM cogdb.reafference_loop;

CREATE OR REPLACE TABLE lake.rec2020_gamut AS
SELECT * FROM cogdb.rec2020_gamut;

CREATE OR REPLACE TABLE lake.stigmergy_evolution AS
SELECT * FROM cogdb.stigmergy_evolution;

CREATE OR REPLACE TABLE lake.strange_loop_cycle AS
SELECT * FROM cogdb.strange_loop_cycle;

DETACH cogdb;

-------------------------------------------------------
-- Summary
-------------------------------------------------------
SELECT '=== DuckLake Initialized ===' AS status;
SELECT count(*) AS command_history_rows FROM lake.command_history;
SELECT count(*) AS conversation_rows FROM lake.conversations;
SELECT count(*) AS session_rows FROM lake.sessions;
SELECT count(*) AS tool_usage_rows FROM lake.tool_usage;
SELECT count(*) AS babashka_rows FROM lake.babashka_usage;
SELECT count(*) AS token_usage_rows FROM lake.token_usage;
SELECT count(*) AS telemetry_rows FROM lake.telemetry;
SELECT count(*) AS stats_cache_rows FROM lake.stats_cache;
SELECT * FROM lake.tool_stats LIMIT 25;
SELECT usage_category, count(*) AS cnt FROM lake.babashka_usage GROUP BY usage_category ORDER BY cnt DESC;
SELECT * FROM lake.project_stats;
