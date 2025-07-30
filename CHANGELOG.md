v0.18.0 (2025-07-30)
-------------------------
 * Remove ping test from NewPool

v0.17.0 (2025-07-09)
-------------------------
 * Change queues.Fair so owner queue keys have same hashtag as other keys

v0.16.0 (2025-07-08)
-------------------------
 * Move Locker into new locks package

v0.15.0 (2025-07-08)
-------------------------
 * Remove :q from owner specific queue keys as it's not needed

v0.14.0 (2025-07-08)
-------------------------
 * Remove support for not using hashtags
 * Add notice to README about cluster mode

v0.13.0 (2025-07-07)
-------------------------
 * More redis > valkey renaming
 * For all multi-key structs, add option to put hashtags in keys
 * Add implementation of fair queue in queues.Fair

v0.12.0 (2025-06-12)
-------------------------
 * Switch to valkey

v0.11.0 (2025-06-09)
-------------------------
 * Update deps
 * Add context.Context support to all main library functions

v0.10.0 (2025-05-20)
-------------------------
 * Add Locker.IsLocked
 * Update go version and deps

v0.9.0 (2024-12-17)
-------------------------
 * Test against redis 7 and valkey 8 too
 * Update deps

v0.8.1 (2024-05-21)
-------------------------
 * Use std library errors

v0.8.0 (2024-03-13)
-------------------------
 * Update assertredis asserts to take a connection instead of a pool

v0.7.0 (2024-03-12)
-------------------------
 * Improve asserts

v0.6.4 (2024-01-30)
-------------------------
 * Fix again

v0.6.3 (2024-01-30)
-------------------------
 * Fix assertrange.LRange expected type

v0.6.2 (2024-01-30)
-------------------------
 * Add assertredis.LRange

v0.6.1 (2024-01-30)
-------------------------
 * Add assertredis.ZScore

v0.6.0 (2024-01-12)
-------------------------
 * Add NewPool helper
 * Update deps

v0.5.0 (2023-09-01)
-------------------------
 * Test on go 1.21
 * Tweak IntervalHash.Del and IntervalSet.Rem to support removing multiple keys like HDEL and SREM
 * Tweak method naming to be closer to the underlying redis commands
 * Implement MGET for interval hashes

v0.4.0 (2023-08-31)
-------------------------
 * Improve redis asserts so they return the equality result
 * Add pattern arg to assertredis.Keys
 * Add assertredis.HGet 
 * Properly support sub minute interval times

v0.3.1 (2023-05-24)
-------------------------
 * Fix assertredis.SIsMember

v0.3.0 (2023-05-24)
-------------------------
 * Update dependencies
 * Add assertredis.SIsMember

v0.2.2
----------
 * Add assertredis.HLen and LLen

v0.2.1
----------
 * Switch from retracted redigo release to latest

v0.2.0
----------
 * Add assertredis.ZCard and assertredis.SCard

v0.1.0
----------
* Initial revision

