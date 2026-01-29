-- DuckLake Query Cookbook for Claude Code Interactions
-- Attach first: ATTACH 'ducklake:~/.claude/interactions.ducklake' AS lake;

-------------------------------------------------------
-- SELF-ANALYSIS: How you use Claude Code
-------------------------------------------------------

-- When are you most active? (hourly heatmap)
SELECT
  CASE day_of_week
    WHEN 0 THEN 'Sun' WHEN 1 THEN 'Mon' WHEN 2 THEN 'Tue'
    WHEN 3 THEN 'Wed' WHEN 4 THEN 'Thu' WHEN 5 THEN 'Fri'
    WHEN 6 THEN 'Sat'
  END AS day,
  hour,
  interactions,
  repeat('█', (interactions * 20 / max(interactions) OVER ())::INT) AS bar
FROM lake.hourly_heatmap
WHERE interactions > 0
ORDER BY day_of_week, hour;

-- Session duration distribution
SELECT
  CASE
    WHEN duration < INTERVAL '5 minutes' THEN '< 5 min'
    WHEN duration < INTERVAL '30 minutes' THEN '5-30 min'
    WHEN duration < INTERVAL '1 hour' THEN '30-60 min'
    WHEN duration < INTERVAL '2 hours' THEN '1-2 hours'
    ELSE '2+ hours'
  END AS session_length,
  count(*) AS sessions,
  avg(user_messages)::INT AS avg_user_msgs,
  avg(tool_calls)::INT AS avg_tool_calls
FROM lake.sessions
WHERE duration IS NOT NULL
GROUP BY session_length
ORDER BY min(duration);

-- Most active projects by engagement depth
SELECT
  project_key,
  total_sessions,
  total_user_msgs,
  total_tool_calls,
  (total_tool_calls * 1.0 / NULLIF(total_user_msgs, 0))::DECIMAL(5,1) AS tools_per_msg,
  active_days
FROM lake.project_stats
ORDER BY total_sessions DESC;

-- Daily streak: consecutive days of Claude Code usage
WITH days AS (
  SELECT DISTINCT ts::DATE AS day FROM lake.conversations WHERE ts IS NOT NULL
),
streaks AS (
  SELECT
    day,
    day - (ROW_NUMBER() OVER (ORDER BY day))::INT * INTERVAL '1 day' AS grp
  FROM days
)
SELECT
  min(day) AS streak_start,
  max(day) AS streak_end,
  count(*) AS streak_days
FROM streaks
GROUP BY grp
ORDER BY streak_days DESC
LIMIT 5;

-------------------------------------------------------
-- COMMAND PATTERNS
-------------------------------------------------------

-- Most common commands/prompts
SELECT
  command_type,
  count(*) AS cnt,
  array_agg(DISTINCT command ORDER BY command LIMIT 5) AS examples
FROM lake.command_history
GROUP BY command_type
ORDER BY cnt DESC;

-- Slash commands frequency
SELECT
  command,
  count(*) AS uses
FROM lake.command_history
WHERE command_type = 'slash_command'
GROUP BY command
ORDER BY uses DESC
LIMIT 20;

-------------------------------------------------------
-- CONVERSATION THREADING
-------------------------------------------------------

-- Longest conversation chains (user -> assistant -> user -> ...)
WITH RECURSIVE chain AS (
  SELECT event_id, parent_event_id, event_type, session_id, 1 AS depth
  FROM lake.conversations
  WHERE parent_event_id IS NULL AND event_type = 'user'

  UNION ALL

  SELECT c.event_id, c.parent_event_id, c.event_type, c.session_id, ch.depth + 1
  FROM lake.conversations c
  JOIN chain ch ON c.parent_event_id = ch.event_id
  WHERE ch.depth < 100
)
SELECT
  session_id,
  max(depth) AS chain_depth,
  count(*) FILTER (WHERE event_type = 'user') AS user_turns,
  count(*) FILTER (WHERE event_type = 'assistant') AS assistant_turns
FROM chain
GROUP BY session_id
ORDER BY chain_depth DESC
LIMIT 10;

-------------------------------------------------------
-- TIME TRAVEL (DuckLake snapshots)
-------------------------------------------------------

-- View all snapshots (each re-run of ducklake_init.sql creates one)
-- FROM ducklake_snapshots('lake');

-- Compare today's stats vs yesterday's snapshot
-- FROM lake.project_stats AT (VERSION => 1);

-------------------------------------------------------
-- TOOL USAGE ANALYTICS
-------------------------------------------------------

-- Top 25 tools by invocation count
SELECT
  tool_name,
  tool_category,
  mcp_server,
  total_calls,
  sessions_used,
  projects_used
FROM lake.tool_stats
ORDER BY total_calls DESC
LIMIT 25;

-- MCP server usage breakdown
SELECT
  mcp_server,
  count(DISTINCT tool_name) AS tools,
  sum(total_calls) AS total_calls,
  sum(sessions_used) AS total_sessions
FROM lake.tool_stats
WHERE mcp_server IS NOT NULL
GROUP BY mcp_server
ORDER BY total_calls DESC;

-- Tool adoption curve: when was each tool first/last used?
SELECT
  tool_name,
  first_used::DATE AS first_day,
  last_used::DATE AS last_day,
  last_used::DATE - first_used::DATE AS span_days,
  total_calls
FROM lake.tool_stats
WHERE total_calls > 5
ORDER BY first_used;

-------------------------------------------------------
-- BABASHKA DEEP-DIVE
-------------------------------------------------------

-- Usage pattern distribution
SELECT
  usage_category,
  count(*) AS calls,
  count(DISTINCT session_id) AS sessions,
  count(DISTINCT project_key) AS projects
FROM lake.babashka_usage
GROUP BY usage_category
ORDER BY calls DESC;

-- Babashka per-project breakdown
SELECT
  project_key,
  count(*) AS bb_calls,
  count(DISTINCT session_id) AS sessions,
  array_agg(DISTINCT usage_category ORDER BY usage_category) AS categories
FROM lake.babashka_usage
GROUP BY project_key
ORDER BY bb_calls DESC;

-- Babashka activity timeline (daily)
SELECT
  ts::DATE AS day,
  count(*) AS bb_calls,
  count(DISTINCT session_id) AS sessions,
  count(*) FILTER (WHERE usage_category = 'shell-exec') AS shell_calls,
  count(*) FILTER (WHERE usage_category = 'apple-containers') AS container_calls,
  count(*) FILTER (WHERE usage_category = 'filesystem') AS fs_calls,
  count(*) FILTER (WHERE usage_category = 'file-read') AS read_calls,
  count(*) FILTER (WHERE usage_category = 'file-write') AS write_calls
FROM lake.babashka_usage
WHERE ts IS NOT NULL
GROUP BY day
ORDER BY day;

-- Sample babashka snippets per category
SELECT DISTINCT ON (usage_category)
  usage_category,
  substr(code_snippet, 1, 200) AS example
FROM lake.babashka_usage
WHERE code_snippet IS NOT NULL AND code_snippet != ''
ORDER BY usage_category;

-------------------------------------------------------
-- TOKEN ECONOMICS
-------------------------------------------------------

-- Token usage by model
SELECT
  model,
  count(*) AS api_calls,
  sum(input_tokens) AS total_input,
  sum(output_tokens) AS total_output,
  sum(cache_read_tokens) AS total_cache_reads,
  sum(cache_creation_tokens) AS total_cache_creates,
  (sum(cache_read_tokens) * 100.0 /
    NULLIF(sum(cache_read_tokens) + sum(input_tokens), 0))::DECIMAL(5,1) AS cache_hit_pct
FROM lake.token_usage
WHERE model IS NOT NULL
GROUP BY model
ORDER BY total_input DESC;

-- Daily token consumption
SELECT
  ts::DATE AS day,
  sum(input_tokens) AS input_tok,
  sum(output_tokens) AS output_tok,
  sum(cache_read_tokens) AS cache_tok,
  count(*) AS api_calls
FROM lake.token_usage
WHERE ts IS NOT NULL
GROUP BY day
ORDER BY day;

-- Token usage per project
SELECT
  project_key,
  sum(input_tokens + output_tokens) AS total_tokens,
  sum(cache_read_tokens) AS cache_saved,
  count(DISTINCT session_id) AS sessions
FROM lake.token_usage
GROUP BY project_key
ORDER BY total_tokens DESC;

-------------------------------------------------------
-- COGNITIVE SCIENCE TABLES (from history.duckdb)
-------------------------------------------------------

-- Phase transition regimes
SELECT * FROM lake.phase_transitions ORDER BY id;

-- Reafference loop states
SELECT * FROM lake.reafference_loop ORDER BY id;

-- Strange loop cycle
SELECT * FROM lake.strange_loop_cycle ORDER BY step;

-- Rec2020 gamut with GF(3) trits
SELECT hex, trit, trit_name, gamut, coverage
FROM lake.rec2020_gamut ORDER BY trit, hue;

-------------------------------------------------------
-- CROSS-TABLE ANALYTICS
-------------------------------------------------------

-- Compare stats-cache totals with DuckLake computed totals
SELECT
  'stats-cache' AS source,
  sum(message_count) AS messages,
  sum(session_count) AS sessions,
  sum(tool_call_count) AS tool_calls,
  min(day) AS first_day,
  max(day) AS last_day
FROM lake.stats_cache
UNION ALL
SELECT
  'ducklake' AS source,
  (SELECT count(*) FROM lake.conversations) AS messages,
  (SELECT count(DISTINCT session_id) FROM lake.conversations) AS sessions,
  (SELECT count(*) FROM lake.tool_usage) AS tool_calls,
  (SELECT min(ts)::DATE FROM lake.conversations WHERE ts IS NOT NULL) AS first_day,
  (SELECT max(ts)::DATE FROM lake.conversations WHERE ts IS NOT NULL) AS last_day;

-- Telemetry event types
SELECT
  event_name,
  count(*) AS cnt,
  min(ts) AS first_seen,
  max(ts) AS last_seen
FROM lake.telemetry
GROUP BY event_name
ORDER BY cnt DESC;

-------------------------------------------------------
-- LIVE QUERIES against raw JSONL (no DuckLake needed)
-------------------------------------------------------

-- Quick: what did I ask Claude in the last hour?
-- SELECT display, epoch_ms(timestamp) as ts
-- FROM read_json_auto('~/.claude/history.jsonl')
-- WHERE epoch_ms(timestamp) > now() - INTERVAL '1 hour'
-- ORDER BY timestamp DESC;

-- Quick: tool usage in current session
-- SELECT type, count(*) as cnt
-- FROM read_json(
--   '~/.claude/projects/-Users-bob-i-comfy-pilot/*.jsonl',
--   columns={type: 'VARCHAR', sessionId: 'VARCHAR'},
--   maximum_object_size=20000000, ignore_errors=true
-- )
-- WHERE sessionId = 'YOUR_SESSION_ID'
-- GROUP BY type ORDER BY cnt DESC;
