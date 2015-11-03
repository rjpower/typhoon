@0xc2e093e0a95e821a;

struct Map(Key, Value) {
  entries @0 :List(Entry);
  struct Entry {
    key @0 :Key;
    value @1 :Value;
  }
}

struct FileRange {
  start @0 :UInt64;
  stop @1 :UInt64;
}

struct FileSplit {
  filename @0 :Text;
  range @1 :FileRange;
  options @2 :Map(Text, Text);
}

struct FileSplits {
  files @0 :List(FileSplit);
}

struct MapShardRequest {
  input @0 :FileSplit;
}

struct MapShardResponse {}

struct WorkerInfo {
  host @0 :Text;
  port @1 :UInt32;
  reducers @2 :List(UInt32);
}

struct PingRequest {
  workers @0 :List(WorkerInfo);
}

struct PingResponse {
}

struct ShuffleRequest {
  workerId @0 :UInt32;
  mapperId @1 :UInt32;
  done @2 :Bool;
  data @3 :Data;
}

struct ShuffleResponse {
}

interface TyphoonWorker {
  mapShard @0 (request :MapShardRequest) -> MapShardResponse;
  ping @1 (request :PingRequest) -> PingResponse;
  shuffle @2 (request: ShuffleRequest) -> ShuffleResponse;
}
