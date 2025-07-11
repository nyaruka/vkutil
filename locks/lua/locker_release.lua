local lockKey, lockValue = KEYS[1], ARGV[1]

if server.call("GET", lockKey) == lockValue then
	return server.call("DEL", lockKey)
else
	return 0
end