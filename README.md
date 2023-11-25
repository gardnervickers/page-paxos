# Page CASPaxos

PageCasPaxos is intended to be an extremely simple CASPaxos implementation,
useful for storing and modifying small, fixed size values.

## Why Page CASPaxos?

I wanted to see if I could build a dead simple system for consensus in
a day. At a minimum it needs to be able to support CAS operations on
values.