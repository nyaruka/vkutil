local field = ARGV[1]

for _, key in ipairs(KEYS) do
	local value = server.call("HGET", key, field)
	if (value ~= false) then
		return value
	end
end

return false