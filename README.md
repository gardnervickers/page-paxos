# Page CASPaxos

PageCasPaxos is intended to be an extremely simple CASPaxos implementation, useful for storing and modifying small, fixed size values (a page).

## Why

I wanted to see if I could build a really simple consensus implementation in a few hundred lines of code and a day or two.

## Were you successful?

Sorta. The main missing piece is reconfiguration. I spent a lot of time working on a simulator.

## TODO:
- [ ] Reconfiguration
- [ ] Support for FPaxos variably sized propose/accept quorums.
- [ ] 1RTT leader optimization prevote
