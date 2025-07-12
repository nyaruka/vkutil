local member = ARGV[1]

for _, key in ipairs(KEYS) do
	local found = server.call("SISMEMBER", key, member)
	if found == 1 then
		return 1
	end
end

return 0