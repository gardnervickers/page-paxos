# Page CASPaxos

PageCasPaxos is an extremely simple CASPaxos implementation, useful for
storing and modifying small, fixed size values.

## Idea

What is the simplest possible CASPaxos acceptor that we can build, while still
being useful for real application?

CASPaxos does not require log replication, so we don't need to worry about trimming
or compaction, which seems simpler.

What if instead of offering general purpose storage for CASPaxos registers, we limited
writes to a multiple of the page size? Then we could do our writes with a single O_DIRECT
write, and reads with a single O_DIRECT read. For most disks this would be sufficient for
durability.


## API

Page CASPaxos exposes a dead simple HTTP API.