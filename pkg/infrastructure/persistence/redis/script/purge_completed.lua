-- purge_completed.lua
--
-- Drops completed jobs whose retention window has elapsed.
--
-- Completed job documents carry a Redis TTL so they expire on their own; this
-- sweep exists to clear the matching sorted-set entries, which Redis will not
-- expire for us. Left alone they would grow without bound and make the
-- dashboard's completed count drift away from reality.
--
-- KEYS[1] completed sorted set, scored by retention deadline
--
-- ARGV[1] now, unix milliseconds
-- ARGV[2] maximum number of entries to remove in this sweep
-- ARGV[3] job key prefix
--
-- Returns the number of entries removed.

local completedKey = KEYS[1]

local now       = tonumber(ARGV[1])
local limit     = tonumber(ARGV[2])
local jobPrefix = ARGV[3]

local stale = redis.call('ZRANGEBYSCORE', completedKey, '-inf', now, 'LIMIT', 0, limit)
local removed = 0

for _, jobID in ipairs(stale) do
  redis.call('ZREM', completedKey, jobID)
  redis.call('DEL', jobPrefix .. jobID)
  removed = removed + 1
end

return removed
