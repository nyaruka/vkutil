# vkutil [![Build Status](https://github.com/nyaruka/vkutil/workflows/CI/badge.svg)](https://github.com/nyaruka/vkutil/actions?query=workflow%3ACI) [![codecov](https://codecov.io/gh/nyaruka/vkutil/branch/main/graph/badge.svg)](https://codecov.io/gh/nyaruka/vkutil) [![Go Report Card](https://goreportcard.com/badge/github.com/nyaruka/vkutil)](https://goreportcard.com/report/github.com/nyaruka/vkutil)

A go library of [Valkey](https://valkey.io) utilities built on the [redigo](github.com/gomodule/redigo) client library.

## NewPool

Simplifies creating a new connection pool, with optional auth, and tests that the connection works:

```go
rp, err := vkutil.NewPool(
    "redis://username:password@localhost:6379/15", 
    vkutil.WithMaxActive(10), 
    vkutil.WithMaxIdle(3), 
    vkutil.WithIdleTimeout(time.Minute)
)
```

## IntervalSet

Creating very large numbers of keys can hurt performance, but putting them all in a single set requires that they all have the same expiration. `IntervalSet` is a way to have multiple sets based on time intervals, accessible like a single set. You trade accuracy of expiry times for a significantly reduced key space. For example using 2 intervals of 24 hours:

```go
set := vkutil.NewIntervalSet("foos", time.Hour*24, 2, false)
set.Add(rc, "A")  // time is 2021-12-02T09:00
...
set.Add(rc, "B")  // time is 2021-12-03T10:00
set.Add(rc, "C")  // time is 2021-12-03T11:00
```

Creates 2 sets like:

```
foos:2021-12-02 => {"A"}       // expires at 2021-12-04T09:00
foos:2021-12-03 => {"B", "C"}  // expires at 2021-12-05T11:00
```

But can be accessed like a single set:

```go
set.IsMember(rc, "A")   // true
set.IsMember(rc, "B")   // true
set.IsMember(rc, "D")   // false
```

## IntervalHash

Same idea as `IntervalSet` but for hashes, and works well for caching values. For example using 2 intervals of 1 hour:

```go
hash := vkutil.NewIntervalHash("foos", time.Hour, 2, false)
hash.Set(rc, "A", "1")  // time is 2021-12-02T09:10
...
hash.Set(rc, "B", "2")  // time is 2021-12-02T10:15
hash.Set(rc, "C", "3")  // time is 2021-12-02T10:20
```

Creates 2 hashes like:

```
foos:2021-12-02T09:00 => {"A": "1"}            // expires at 2021-12-02T11:10
foos:2021-12-02T10:00 => {"B": "2", "C": "3"}  // expires at 2021-12-02T12:20
```

But can be accessed like a single hash:

```go
hash.Get(rc, "A")   // "1"
hash.Get(rc, "B")   // "2"
hash.Get(rc, "D")   // ""
```

## IntervalSeries

When getting a value from an `IntervalHash` you're getting the newest value by looking back through the intervals. `IntervalSeries` however lets you get an accumulated value from each interval.

For example using 3 intervals of 1 hour:

```go
series := vkutil.NewIntervalSeries("foos", time.Hour, 3, false)
series.Record(rc, "A", 1)  // time is 2021-12-02T09:10
series.Record(rc, "A", 2)  // time is 2021-12-02T09:15
...
series.Record(rc, "A", 3)  // time is 2021-12-02T10:15
series.Record(rc, "A", 4)  // time is 2021-12-02T10:20
...
series.Record(rc, "A", 5)  // time is 2021-12-02T11:25
series.Record(rc, "B", 1)  // time is 2021-12-02T11:30
```

Creates 3 hashes like:

```
foos:2021-12-02T09:00 => {"A": "3"}            // expires at 2021-12-02T12:15
foos:2021-12-02T10:00 => {"A": "7"}            // expires at 2021-12-02T13:20
foos:2021-12-02T11:00 => {"A": "5", "B": "1"}  // expires at 2021-12-02T14:30
```

But lets us retrieve values across intervals:

```go
series.Get(rc, "A")   // [5, 7, 3]
series.Get(rc, "B")   // [1, 0, 0]
series.Get(rc, "C")   // [0, 0, 0]
```

## CappedZSet

The `CappedZSet` type is based on a sorted set but enforces a cap on size, by only retaining the highest ranked members.

```go
cset := vkutil.NewCappedZSet("foos", 3, time.Hour*24)
cset.Add(rc, "A", 1) 
cset.Add(rc, "C", 3) 
cset.Add(rc, "D", 4)
cset.Add(rc, "B", 2) 
cset.Add(rc, "E", 5) 
cset.Members(rc)      // ["C", "D", "E"] / [3, 4, 5]
```

## Testing Asserts

The `assertvk` package contains several asserts useful for testing the state of a database.

```go
rp := assertvk.TestDB()
rc := rp.Get()
defer rc.Close()

assertvk.Keys(t, rc, "*", []string{"foo", "bar"})
assertvk.Exists(t, rc, "foo")
assertvk.NotExists(t, rc, "bar")
assertvk.Get(t, rc, "foo", "123")
assertvk.SCard(t, rc, "foo_set", 2)
assertvk.SMembers(t, rc, "foo_set", []string{"123", "234"})
```
