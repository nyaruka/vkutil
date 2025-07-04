local activeKey = KEYS[1]
local pausedKey = KEYS[2]
local tempKey = KEYS[3]
local queueBase = ARGV[1]
local maxActivePerOwner = ARGV[2]

-- subtract paused owners and store as new set
redis.call("ZDIFFSTORE", tempKey, 2, activeKey, pausedKey)
redis.call("EXPIRE", tempKey, 60)

-- get the active owner with the least active tasks
local result = redis.call("ZRANGEBYSCORE", tempKey, "-inf", "(" .. maxActivePerOwner, "LIMIT", 0, 1)

-- nothing? return nothing
local owner = result[1]
if not owner then
    return {"empty", "", ""}
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
