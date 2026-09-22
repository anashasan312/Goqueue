-- dequeue.lua
--
-- Claims at most one job for a worker, scanning the supplied queues in order and
-- taking the first queue that has an eligible job.
--
-- This script is the reason two workers can never run the same job: ZPOPMIN and
-- the move into the active set happen inside a single Redis execution, so the
-- claim is indivisible. Everything else about job processing can be retried
-- safely; this step cannot, so it is the one that gets the script.
--
-- The lease is written here rather than by the caller so that a worker which
-- dies between the claim and its own bookkeeping still leaves a job in the
-- active set with an expiry, where the reclaim sweep will find it.
--
-- KEYS come in groups of three per queue, in polling order:
--   [i*3+1] pending sorted set
--   [i*3+2] active sorted set
--   [i*3+3] queue meta hash
--
-- ARGV[1] number of queues
-- ARGV[2] worker id
-- ARGV[3] lease expiry, unix milliseconds
-- ARGV[4] now, unix milliseconds
-- ARGV[5] job key prefix
--
-- Returns the job hash as a flat array, exactly as it was before this script
-- leased it, or false when every queue is empty. The pre-lease snapshot is
-- returned on purpose: the caller reconstitutes the aggregate and applies its
-- own Lease transition, so the attempt trail and the retry accounting stay
-- owned by the domain rather than duplicated in Lua.

local queueCount  = tonumber(ARGV[1])
local workerID    = ARGV[2]
local leaseExpiry = tonumber(ARGV[3])
local now         = tonumber(ARGV[4])
local jobPrefix   = ARGV[5]

for i = 0, queueCount - 1 do
  local pendingKey = KEYS[i * 3 + 1]
  local activeKey  = KEYS[i * 3 + 2]
  local metaKey    = KEYS[i * 3 + 3]

  if redis.call('HGET', metaKey, 'paused') ~= '1' then
    -- Loop so that a batch of orphaned set entries (documents already expired
    -- or deleted) is skipped rather than returning empty and making the worker
    -- sleep through a queue that still has real work in it.
    local drained = false
    while not drained do
      local popped = redis.call('ZPOPMIN', pendingKey, 1)
      if popped == nil or #popped == 0 then
        drained = true
      else
        local jobID  = popped[1]
        local jobKey = jobPrefix .. jobID
        if redis.call('EXISTS', jobKey) == 1 then
          local snapshot = redis.call('HGETALL', jobKey)
          redis.call('ZADD', activeKey, leaseExpiry, jobID)
          redis.call('HSET', jobKey,
            'state', 'active',
            'worker_id', workerID,
            'lease_expiry_ms', leaseExpiry,
            'leased_at_ms', now)
          return snapshot
        end
      end
    end
  end
end

return false
