local queuedKey = KEYS[1]
local activeKey = KEYS[2]
local pausedKey = KEYS[3]
local tempKey = KEYS[4]
local queueBase = ARGV[1]
local maxActivePerOwner = ARGV[2]

-- create a new set which is union of queued and active owners, with scores from active
redis.call("ZUNIONSTORE", tempKey, 2, queuedKey, activeKey, "WEIGHTS", 0, 1)

-- substract paused owners from this set
redis.call("ZDIFFSTORE", tempKey, 2, tempKey, pausedKey)
redis.call("EXPIRE", tempKey, 60)

-- get the owner with the least active tasks
local result = redis.call("ZRANGEBYSCORE", tempKey, "-inf", "(" .. maxActivePerOwner, "LIMIT", 0, 1)

-- nothing? return nothing
local owner = result[1]
if not owner then
    return {"empty", "", ""}
end

-- decrement queued count for this owner
local queuedCount = tonumber(redis.call("ZINCRBY", queuedKey, -1, owner))
if queuedCount <= 0 then
    redis.call("ZREM", queuedKey, owner)
end

-- TODO should not be using dynamic keys, see https://valkey.io/commands/eval/
local queue0Key = queueBase .. ":q:" .. owner .. "/0"
local queue1Key = queueBase .. ":q:" .. owner .. "/1"

-- pop off our queues
local result = redis.call("LPOP", queue1Key)
if not result then
    result = redis.call("LPOP", queue0Key)
end

-- found a result?
if result then
    -- and add a worker to this owner
    redis.call("ZINCRBY", activeKey, 1, owner)

    return {"ok", owner, result}
else
    -- no result found, remove this owner
    redis.call("ZREM", activeKey, owner)

    return {"retry", "", ""}
end
