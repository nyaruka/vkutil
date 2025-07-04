local activeKey = KEYS[1]
local queueBase = ARGV[1]

local results = redis.call("ZRANGE", activeKey, 0, -1)
local count = 0

for i = 1, #results do
    -- TODO should not be using dynamic keys, see https://valkey.io/commands/eval/
    count = count + redis.call("LLEN", queueBase .. ":q:" .. results[i] .. "/0")
    count = count + redis.call("LLEN", queueBase .. ":q:" .. results[i] .. "/1")
end

return count