local activeKey = KEYS[1]
local queue0Key = KEYS[2]
local queue1Key = KEYS[3]
local owner = ARGV[1]
local priority = ARGV[2]
local task = ARGV[3]

if priority == "0" then
    redis.call("RPUSH", queue0Key, task)
else
    redis.call("RPUSH", queue1Key, task)
end

redis.call("ZINCRBY", activeKey, 0, owner) -- ensure exists in the active set