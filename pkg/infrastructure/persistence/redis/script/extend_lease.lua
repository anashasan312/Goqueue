-- extend_lease.lua
--
-- Pushes an active job's lease expiry further out, on behalf of a worker
-- heartbeat.
--
-- The guard matters: the script refuses to extend a lease for a job that is no
-- longer in the active set. Without it, a worker whose job was already
-- reclaimed and handed to someone else would keep renewing a lease on work it
-- no longer owns, and two workers would believe they held the same job.
--
-- KEYS[1] active sorted set
-- KEYS[2] job hash key
--
-- ARGV[1] job id
-- ARGV[2] new lease expiry, unix milliseconds
--
-- Returns 1 when the lease was extended, 0 when the job was no longer active.

local activeKey = KEYS[1]
local jobKey    = KEYS[2]

local jobID  = ARGV[1]
local expiry = tonumber(ARGV[2])

if redis.call('ZSCORE', activeKey, jobID) == false then
  return 0
end

redis.call('ZADD', activeKey, expiry, jobID)
redis.call('HSET', jobKey, 'lease_expiry_ms', expiry)

return 1
