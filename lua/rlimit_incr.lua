local key, window = KEYS[1], ARGV[1]

local count = redis.call("INCR", key)
if count == 1 then
	redis.call("PEXPIRE", key, window)
end
return count
