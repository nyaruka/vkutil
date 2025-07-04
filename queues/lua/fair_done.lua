local activeKey = KEYS[1]
local owner = ARGV[1]

-- decrement our active task count for this owner
local active = tonumber(redis.call("ZINCRBY", activeKey, -1, owner))

-- reset to zero if we somehow go below
if active < 0 then
    redis.call("ZADD", activeKey, 0, owner)
end
