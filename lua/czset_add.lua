local key, score, member, cap, expire = KEYS[1], ARGV[1], ARGV[2], tonumber(ARGV[3]), ARGV[4]

redis.call("ZADD", key, score, member)
redis.call("PEXPIRE", key, expire)
local newSize = redis.call("ZCARD", key)

if newSize > cap then
	redis.call("ZREMRANGEBYRANK", key, 0, (newSize - cap) - 1)
end