-- promote.lua
--
-- Moves jobs whose process-at instant has arrived from a waiting set
-- (scheduled or retrying) into the pending set.
--
-- The pending score is recomputed here from the job's own priority so that a
-- delayed job re-enters the queue at its correct rank rather than at the tail:
-- a critical job that waited out a backoff should still beat a low-priority job
-- that has been sitting in pending.
--
-- This is a bulk transition deliberately kept in Lua. The equivalent
-- single-job rule lives on the Job aggregate as Promote() and is unit-tested
-- there; running it per job from Go would cost two round trips per job and turn
-- a routine sweep into the system's bottleneck. The script is the batch form of
-- a documented domain rule, not a second source of truth.
--
-- KEYS[1] source sorted set (scheduled or retrying)
-- KEYS[2] pending sorted set
--
-- ARGV[1] now, unix milliseconds
-- ARGV[2] maximum number of jobs to move in this sweep
-- ARGV[3] job key prefix
-- ARGV[4] priority band multiplier
-- ARGV[5] maximum priority value
--
-- Returns the number of jobs moved.

local sourceKey  = KEYS[1]
local pendingKey = KEYS[2]

local now         = tonumber(ARGV[1])
local limit       = tonumber(ARGV[2])
local jobPrefix   = ARGV[3]
local band        = tonumber(ARGV[4])
local maxPriority = tonumber(ARGV[5])

local due = redis.call('ZRANGEBYSCORE', sourceKey, '-inf', now, 'LIMIT', 0, limit)
local moved = 0

for _, jobID in ipairs(due) do
  local jobKey = jobPrefix .. jobID
  redis.call('ZREM', sourceKey, jobID)

  if redis.call('EXISTS', jobKey) == 1 then
    local priority = tonumber(redis.call('HGET', jobKey, 'priority') or '5')
    local score = (maxPriority - priority) * band + now
    redis.call('ZADD', pendingKey, score, jobID)
    redis.call('HSET', jobKey,
      'state', 'pending',
      'process_at_ms', now,
      'lease_expiry_ms', '0',
      'worker_id', '')
    moved = moved + 1
  end
end

return moved
