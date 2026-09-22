-- reclaim.lua
--
-- Returns jobs whose worker lease has lapsed from the active set to the pending
-- set. This is the recovery path that makes delivery at-least-once: a worker
-- that is SIGKILLed, loses its network, or has its container evicted mid-job
-- leaves its jobs leased, and this sweep puts them back in circulation.
--
-- The lapsed attempt is already counted — the worker did start it — so the job
-- resumes with its remaining retry budget intact and an explicit last_error
-- that distinguishes an orphaned job from one that failed on its own merits.
--
-- Like promote.lua this is the batch form of a rule the domain also expresses
-- per job, as Job.ReclaimExpiredLease().
--
-- KEYS[1] active sorted set, scored by lease expiry
-- KEYS[2] pending sorted set
--
-- ARGV[1] now, unix milliseconds
-- ARGV[2] maximum number of jobs to reclaim in this sweep
-- ARGV[3] job key prefix
-- ARGV[4] priority band multiplier
-- ARGV[5] maximum priority value
--
-- Returns the number of jobs reclaimed.

local activeKey  = KEYS[1]
local pendingKey = KEYS[2]

local now         = tonumber(ARGV[1])
local limit       = tonumber(ARGV[2])
local jobPrefix   = ARGV[3]
local band        = tonumber(ARGV[4])
local maxPriority = tonumber(ARGV[5])

local expired = redis.call('ZRANGEBYSCORE', activeKey, '-inf', now, 'LIMIT', 0, limit)
local reclaimed = 0

for _, jobID in ipairs(expired) do
  local jobKey = jobPrefix .. jobID
  redis.call('ZREM', activeKey, jobID)

  if redis.call('EXISTS', jobKey) == 1 then
    local priority = tonumber(redis.call('HGET', jobKey, 'priority') or '5')
    local score = (maxPriority - priority) * band + now
    redis.call('ZADD', pendingKey, score, jobID)
    redis.call('HSET', jobKey,
      'state', 'pending',
      'process_at_ms', now,
      'lease_expiry_ms', '0',
      'worker_id', '',
      'last_error', 'lease expired: worker did not report completion')
    reclaimed = reclaimed + 1
  end
end

return reclaimed
