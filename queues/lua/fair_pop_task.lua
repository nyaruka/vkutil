local activeKey = KEYS[1]
local queue0Key = KEYS[2]
local queue1Key = KEYS[3]
local owner = ARGV[1]

-- pop off our queues (priority first)
local result = redis.call("LPOP", queue1Key)
if not result then
    result = redis.call("LPOP", queue0Key)
end

-- found a result?
if result then
    -- we found a task, active count was already incremented in SelectOwner
    return {"ok", result}
else
    -- no result found, completely remove this owner from active set
    redis.call("ZREM", activeKey, owner)
    
    return {"empty", ""}
end