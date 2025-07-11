local activeKey = KEYS[1]
local owner = ARGV[1]

-- decrement our active task count for this owner
local activeCount = tonumber(server.call("ZINCRBY", activeKey, -1, owner))

-- remove if zero (or somehow negative)
if activeCount <= 0 then
    server.call("ZREM", activeKey, owner)
end
