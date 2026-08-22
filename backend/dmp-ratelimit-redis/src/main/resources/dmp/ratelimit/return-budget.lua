-- Puts back budget that was reserved and never spent.
--
-- The mirror of take-budget.lua and deliberately not its inverse: this adds tokens without any
-- notion of whether the caller was entitled to them, because the caller has already proved that by
-- counting the requests it made itself. What this script does guarantee is that no bucket ends up
-- holding more than its capacity, so a return can never create budget that the limit did not allow.
--
-- Refilling from elapsed time first, then adding, matters. Adding to a stale token count and then
-- capping would silently discard whatever had accrued since the bucket was last touched.
--
-- KEYS[1] records bucket   KEYS[2] calls bucket
-- ARGV[1..4] records: capacity, refill per window, window (ms), amount to return
-- ARGV[5..8] calls:   capacity, refill per window, window (ms), amount to return
-- ARGV[9]    key ttl (ms)

local time = redis.call('TIME')
local now = (tonumber(time[1]) * 1000) + math.floor(tonumber(time[2]) / 1000)
local ttl = tonumber(ARGV[9])

local function give(key, capacity, refill, window, amount)
  if capacity <= 0 or amount <= 0 then
    return
  end

  local state = redis.call('HMGET', key, 'tokens', 'at')
  local tokens = tonumber(state[1])
  local at = tonumber(state[2])

  if tokens == nil or at == nil then
    -- Nobody has taken from this bucket since it expired, so it is already full and there is
    -- nothing a return could add. Writing here would only resurrect a key Redis had reclaimed.
    return
  end

  local elapsed = now - at
  if elapsed > 0 then
    tokens = math.min(capacity, tokens + (elapsed * refill / window))
  end

  redis.call('HSET', key, 'tokens', math.min(capacity, tokens + amount), 'at', now)
  redis.call('PEXPIRE', key, ttl)
end

give(KEYS[1], tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4]))
give(KEYS[2], tonumber(ARGV[5]), tonumber(ARGV[6]), tonumber(ARGV[7]), tonumber(ARGV[8]))

return 1
