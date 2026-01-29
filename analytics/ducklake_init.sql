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
  maximum_object_size=20000000,
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

-- Summary
SELECT '=== DuckLake Initialized ===' AS status;
SELECT count(*) AS command_history_rows FROM lake.command_history;
SELECT count(*) AS conversation_rows FROM lake.conversations;
SELECT count(*) AS session_rows FROM lake.sessions;
SELECT * FROM lake.project_stats;
