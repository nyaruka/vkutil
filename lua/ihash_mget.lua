local fields = ARGV
local values = {}
local found = 0

-- initialize our list of values to return to false/nils
for i, key in ipairs(fields) do
	values[i] = false
end

for _, key in ipairs(KEYS) do
	local vs = server.call("HMGET", key, unpack(fields))

	for i, v in ipairs(vs) do
		if (v ~= false and values[i] == false) then
			values[i] = v
			found = found + 1
		end
	end

	-- if we've found values for all fields we don't need to look in older keys
	if (found == #KEYS) then
		break
	end
end

return values