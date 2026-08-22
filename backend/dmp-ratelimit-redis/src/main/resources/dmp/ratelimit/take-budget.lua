-- Takes a chunk's whole budget, or takes nothing and says how long until it could.
--
-- Two token buckets — records and calls — moved in one script so that "enough of both, or neither"
-- is true rather than nearly true. Two round trips could take the records and then find the calls
-- short, leaving budget spent on work that will not happen; across a fleet of workers that is not a
-- rare interleaving, it is every time two pods ask at once.
--
-- Each bucket is a hash of {tokens, at}. Tokens are refilled from elapsed time rather than by any
-- timer, so an idle bucket costs nothing to keep. The clock is Redis' own, read here rather than
-- passed in: every pod must agree on how much has refilled, and pods do not agree on the time.
--
-- Capacity and refill are given separately rather than being the same number, which is what lets
-- the caller choose between spending a whole window at once and never exceeding it in any window.
-- See RateLimitPolicy.Pacing; this script only does the arithmetic it is handed.
--
-- KEYS[1] records bucket   KEYS[2] calls bucket
-- ARGV[1..4] records: capacity, refill per window, window (ms), wanted
-- ARGV[5..8] calls:   capacity, refill per window, window (ms), wanted
-- ARGV[9]    key ttl (ms)
--
-- Returns { granted, waitMillis }
--   granted =  1  budget taken, go
--   granted =  0  nothing taken, ask again in waitMillis
--   granted = -1  this request can never be granted; waiting will not help

local time = redis.call('TIME')
local now = (tonumber(time[1]) * 1000) + math.floor(tonumber(time[2]) / 1000)
local ttl = tonumber(ARGV[9])

local function peek(key, capacity, refill, window, wanted)
  -- A capacity of zero means this unit is not limited at all: always available, never written.
  if capacity <= 0 or wanted <= 0 then
    return { tokens = 0, wait = 0, skip = true }
  end
  -- Either the call is bigger than the bucket can hold, or nothing ever accrues into it. Both are
  -- permanent, and both are the same answer: this will not happen however long anybody waits.
  if wanted > capacity or refill <= 0 then
    return { tokens = 0, wait = -1, skip = false }
  end

  local state = redis.call('HMGET', key, 'tokens', 'at')
  local tokens = tonumber(state[1])
  local at = tonumber(state[2])

  if tokens == nil or at == nil then
    -- First sight of this connector: full. Starting empty would make every run after an idle
    -- period wait for its first record with a budget nobody had spent.
    tokens = capacity
    at = now
  else
    local elapsed = now - at
    if elapsed > 0 then
      tokens = math.min(capacity, tokens + (elapsed * refill / window))
    end
  end

  if tokens >= wanted then
    return { tokens = tokens, wait = 0, skip = false }
  end

  -- Rounded up, so a caller that waits exactly this long finds the budget waiting for it rather
  -- than missing it by a fraction of a millisecond and coming straight back.
  local short = wanted - tokens
  local wait = math.ceil(short * window / refill)
  return { tokens = tokens, wait = wait, skip = false }
end

local records = peek(KEYS[1], tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4]))
local calls   = peek(KEYS[2], tonumber(ARGV[5]), tonumber(ARGV[6]), tonumber(ARGV[7]), tonumber(ARGV[8]))

if records.wait == -1 or calls.wait == -1 then
  return { -1, 0 }
end

local wait = math.max(records.wait, calls.wait)
if wait > 0 then
  -- Nothing is written on refusal. A partial take is the bug this script exists to prevent, and
  -- leaving the buckets untouched also means a refused caller has not aged them on behalf of
  -- whichever pod eventually succeeds.
  return { 0, wait }
end

local function take(key, state, wanted)
  if state.skip then
    return
  end
  redis.call('HSET', key, 'tokens', state.tokens - wanted, 'at', now)
  redis.call('PEXPIRE', key, ttl)
end

take(KEYS[1], records, tonumber(ARGV[4]))
take(KEYS[2], calls, tonumber(ARGV[8]))

return { 1, 0 }
