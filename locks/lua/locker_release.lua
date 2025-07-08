local lockKey, lockValue = KEYS[1], ARGV[1]

if redis.call("GET", lockKey) == lockValue then
	return redis.call("DEL", lockKey)
else
	return 0
end