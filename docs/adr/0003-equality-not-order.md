# 3. Activity means "something changed", not "a number went up"

**Context.** The controller periodically asks a source whether anything has arrived since
the last check. Kafka answers with end offsets, plain integers, so comparing them
numerically ("the offset grew") is the obvious implementation. Other sources answer with
values that are not numbers (Kinesis sequence numbers, Pulsar message ids, object-store
keys), so a numeric comparison would not generalise.

**Decision.** A probe returns one opaque text position per source (for Kafka, per topic: the end
offsets of its partitions joined in order). The decider compares the whole map with the previous one: different means
something arrived. It never parses the values.

"I could not ask" (broker down, topic gone) is a third answer, distinct from "unchanged".
The decider does nothing in that case. Not knowing never counts as idle.

**Consequences.** One decider for every source. The controller learns whether something
arrived, never how much, which is all suspend and resume need. Backlog-aware behaviour
would be a new signal and its own ADR.
