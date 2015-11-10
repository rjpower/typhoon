## Basic operation

Typhoon is a master/worker system.  Workers execute various types of
tasks, which are scheduled and monitored by the master.  A communication
graph specifies what operations need to be scheduled and performed by
the system.  The masters role is to schedule tasks as optimally as 
possible and monitor for failures.

### Sources, sinks, shuffles

Sources provide a stream of key-value messages into the system.
Sinks accept key-value messages and hold them for later usage.
Tasks read from sources and write into sinks.
Shuffles group data from one set of sources to a set of sinks


## Graph Description

Consider a word-count mapreduce:

input = textreader(gfs://books@1000)
words = map(input, lambda k, v: v => input.split(" "))
grouped = shuffle(words, 50 /* shards */)
reduce(grouped, lambda k, vs: => sum(vs))

Inputs are a "virtual" source; as it is external to the system,
the master does not have to schedule it explicitly.

Our map is scheduled by asking our input to provide a set of 
split points and scheduling a mapper task for each one.  The
output of the mapper is placed in a store on the same node.

Shuffles are special: they transfer inputs from one store to
a number of output stores.  The shuffle is scheduled by the
master immediately, but blocks until the mapper has finished
writing to it's local store.

The final reduction is another local task emitting to either
an in-memory/disk store for further processing, or to an
external sink which can persist after the system shuts down.