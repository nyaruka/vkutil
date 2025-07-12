local key, score, member, cap, expire = KEYS[1], ARGV[1], ARGV[2], tonumber(ARGV[3]), ARGV[4]

server.call("ZADD", key, score, member)
server.call("EXPIRE", key, expire)
local newSize = server.call("ZCARD", key)

if newSize > cap then
	server.call("ZREMRANGEBYRANK", key, 0, (newSize - cap) - 1)
end