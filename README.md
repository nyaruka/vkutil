# 🧰 vkutil [![Build Status](https://github.com/nyaruka/vkutil/workflows/CI/badge.svg)](https://github.com/nyaruka/vkutil/actions?query=workflow%3ACI) [![codecov](https://codecov.io/gh/nyaruka/vkutil/branch/main/graph/badge.svg)](https://codecov.io/gh/nyaruka/vkutil) [![Go Report Card](https://goreportcard.com/badge/github.com/nyaruka/vkutil)](https://goreportcard.com/report/github.com/nyaruka/vkutil)

A go library of [Valkey](https://valkey.io) utilities built on the [redigo](github.com/gomodule/redigo) client library.

> [!IMPORTANT]
> Because this library is built on [redigo](github.com/gomodule/redigo) it doesn't support cluster mode. However care 
> has been taken to ensure this is possible in future by 1) not dynamically constructing keys in LUA scripts and 
> 2) using [hashtags](https://valkey.io/topics/cluster-spec/) to ensure that keys that are accessed together would hash 
> to the same hash slot.

## Interval Based Structs

### IntervalSet

Creating very large numbers of keys can hurt performance, but putting them all in a single set requires that they all have the same expiration. `IntervalSet` is a way to have multiple sets based on time intervals, accessible like a single set. You trade accuracy of expiry times for a significantly reduced key space. For example using 2 intervals of 24 hours:

```go
set := vkutil.NewIntervalSet("foos", time.Hour*24, 2)
set.Add(ctx, vc, "A")  // time is 2021-12-02T09:00
...
set.Add(ctx, vc, "B")  // time is 2021-12-03T10:00
set.Add(ctx, vc, "C")  // time is 2021-12-03T11:00
```

Creates 2 sets as follows:

```
{foos}:2021-12-02 => {"A"}       // expires at 2021-12-04T09:00
{foos}:2021-12-03 => {"B", "C"}  // expires at 2021-12-05T11:00
```

But can be accessed like a single set:

```go
set.IsMember(ctx, vc, "A")   // true
set.IsMember(ctx, vc, "B")   // true
set.IsMember(ctx, vc, "D")   // false
```

### IntervalHash

Same idea as `IntervalSet` but for hashes, and works well for caching values. For example using 2 intervals of 1 hour:

```go
hash := vkutil.NewIntervalHash("foos", time.Hour, 2)
hash.Set(ctx, vc, "A", "1")  // time is 2021-12-02T09:10
...
hash.Set(ctx, vc, "B", "2")  // time is 2021-12-02T10:15
hash.Set(ctx, vc, "C", "3")  // time is 2021-12-02T10:20
```

Creates 2 hashes like:

```
{foos}:2021-12-02T09:00 => {"A": "1"}            // expires at 2021-12-02T11:10
{foos}:2021-12-02T10:00 => {"B": "2", "C": "3"}  // expires at 2021-12-02T12:20
```

But can be accessed like a single hash:

```go
hash.Get(ctx, vc, "A")   // "1"
hash.Get(ctx, vc, "B")   // "2"
hash.Get(ctx, vc, "D")   // ""
```

### IntervalSeries

When getting a value from an `IntervalHash` you're getting the newest value by looking back through the intervals. `IntervalSeries` however lets you get an accumulated value from each interval.

For example using 3 intervals of 1 hour:

```go
series := vkutil.NewIntervalSeries("foos", time.Hour, 3)
series.Record(ctx, vc, "A", 1)  // time is 2021-12-02T09:10
series.Record(ctx, vc, "A", 2)  // time is 2021-12-02T09:15
...
series.Record(ctx, vc, "A", 3)  // time is 2021-12-02T10:15
series.Record(ctx, vc, "A", 4)  // time is 2021-12-02T10:20
...
series.Record(ctx, vc, "A", 5)  // time is 2021-12-02T11:25
series.Record(ctx, vc, "B", 1)  // time is 2021-12-02T11:30
```

Creates 3 hashes like:

```
{foos}:2021-12-02T09:00 => {"A": "3"}            // expires at 2021-12-02T12:15
{foos}:2021-12-02T10:00 => {"A": "7"}            // expires at 2021-12-02T13:20
{foos}:2021-12-02T11:00 => {"A": "5", "B": "1"}  // expires at 2021-12-02T14:30
```

But lets us retrieve values across intervals:

```go
series.Get(ctx, vc, "A")   // [5, 7, 3]
series.Get(ctx, vc, "B")   // [1, 0, 0]
series.Get(ctx, vc, "C")   // [0, 0, 0]
```

## Queues

### Fair

```go
queue := queues.NewFair("jobs", 10)
queue.Push(ctx, vc, "owner1", true, []byte(`{...}`))
queue.Push(ctx, vc, "owner2", false, []byte(`{...}`))
owner, task, err := queue.Pop(ctx, vc)
...
queue.Done(ctx, vc, owner)
```

## Locks

### Locker

```go
locker := vkutil.NewLocker("mylock", time.Minute)
lock, err := locker.Grab(ctx, vp, 10 * time.Second)
...
locker.Release(ctx, vc, lock)
```

## Other

### NewPool

Simplifies creating a new connection pool, with optional auth, and tests that the connection works:

```go
vp, err := vkutil.NewPool(
    "valkey://username:password@localhost:6379/15", 
    vkutil.WithMaxActive(10), 
    vkutil.WithMaxIdle(3), 
    vkutil.WithIdleTimeout(time.Minute)
)
```

### CappedZSet

The `CappedZSet` type is based on a sorted set but enforces a cap on size, by only retaining the highest ranked members.

```go
cset := vkutil.NewCappedZSet("foos", 3, time.Hour*24)
cset.Add(ctx, vc, "A", 1) 
cset.Add(ctx, vc, "C", 3) 
cset.Add(ctx, vc, "D", 4)
cset.Add(ctx, vc, "B", 2) 
cset.Add(ctx, vc, "E", 5) 
cset.Members(ctx, vc)      // ["C", "D", "E"] / [3, 4, 5]
```

## Testing 

### Asserts

The `assertvk` package contains several asserts useful for testing the state of a database.

```go
vp := assertvk.TestDB()
vc := vp.Get()
defer vc.Close()

assertvk.Keys(t, vc, "*", []string{"foo", "bar"})
assertvk.Exists(t, vc, "foo")
assertvk.NotExists(t, vc, "bar")
assertvk.Get(t, vc, "foo", "123")
assertvk.SCard(t, vc, "foo_set", 2)
assertvk.SMembers(t, vc, "foo_set", []string{"123", "234"})
```
