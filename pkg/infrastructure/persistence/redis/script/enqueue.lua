-- enqueue.lua
--
-- Writes a job document and adds it to a target lifecycle set in one atomic
-- step, and registers the queue so it appears in listings before it has ever
-- been drained.
--
-- Doing the write and the set insert together matters: a document written
-- without a set entry is an invisible job that never runs, and a set entry
-- without a document is a phantom a worker would pop and discard.
--
-- KEYS[1] job hash key
-- KEYS[2] target sorted set (pending or scheduled)
-- KEYS[3] queue registry set
-- KEYS[4] queue meta hash
--
-- ARGV[1]  job id
-- ARGV[2]  target score
-- ARGV[3]  queue name
-- ARGV[4]  queue weight, applied only when the queue is new
-- ARGV[5+] job hash field/value pairs
--
-- Returns 1 on insert, 0 when a document with this id already exists.

local jobKey      = KEYS[1]
local targetKey   = KEYS[2]
local registryKey = KEYS[3]
local metaKey     = KEYS[4]

local jobID  = ARGV[1]
local score  = tonumber(ARGV[2])
local queue  = ARGV[3]
local weight = ARGV[4]

if redis.call('EXISTS', jobKey) == 1 then
  return 0
end

redis.call('SADD', registryKey, queue)
if redis.call('EXISTS', metaKey) == 0 then
  redis.call('HSET', metaKey, 'weight', weight, 'paused', '0')
end

redis.call('HSET', jobKey, unpack(ARGV, 5))
redis.call('ZADD', targetKey, score, jobID)

return 1
