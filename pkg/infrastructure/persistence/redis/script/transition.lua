-- transition.lua
--
-- Moves a job between two lifecycle sets and rewrites its document atomically.
--
-- Complete, retry, kill and requeue are the same Redis shape — remove from one
-- sorted set, add to another, overwrite the document, optionally set a TTL — so
-- they share one script. The behavioural differences between them live in the
-- Job aggregate, which has already run by the time this executes; what reaches
-- Redis is only the resulting document and the two sets involved.
--
-- KEYS[1] job hash key
-- KEYS[2] source sorted set
-- KEYS[3] destination sorted set
--
-- ARGV[1]  job id
-- ARGV[2]  destination score
-- ARGV[3]  document TTL in seconds, 0 to keep the document forever
-- ARGV[4+] job hash field/value pairs
--
-- Returns 1 when the document existed and was moved, 0 when it had already been
-- deleted, in which case the caller treats the transition as a no-op rather
-- than resurrecting a job an operator removed on purpose.

local jobKey = KEYS[1]
local fromKey = KEYS[2]
local toKey   = KEYS[3]

local jobID = ARGV[1]
local score = tonumber(ARGV[2])
local ttl   = tonumber(ARGV[3])

if redis.call('EXISTS', jobKey) == 0 then
  redis.call('ZREM', fromKey, jobID)
  return 0
end

redis.call('ZREM', fromKey, jobID)
redis.call('ZADD', toKey, score, jobID)
redis.call('HSET', jobKey, unpack(ARGV, 4))

if ttl > 0 then
  redis.call('EXPIRE', jobKey, ttl)
else
  redis.call('PERSIST', jobKey)
end

return 1
