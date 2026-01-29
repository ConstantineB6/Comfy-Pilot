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
