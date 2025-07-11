local field = ARGV[1]
local values = {}

for _, key in ipairs(KEYS) do
	table.insert(values, server.call("HGET", key, field))
end

return values