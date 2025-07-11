local lockKey, lockValue, lockExpire = KEYS[1], ARGV[1], ARGV[2]

if server.call("GET", lockKey) == lockValue then
	return server.call("EXPIRE", lockKey, lockExpire)
else
	return 0
end