local lockKey, lockValue, lockExpire = KEYS[1], ARGV[1], ARGV[2]

if redis.call("GET", lockKey) == lockValue then
	return redis.call("EXPIRE", lockKey, lockExpire)
else
	return 0
end