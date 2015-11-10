import logging

from grpc.beta import implementations
import typhoon_pb2

def main():
    mapper = 'CountMapper'
    source_type = 'WordSource'
    mem_store = 'MemStore<std::string, int64_t>'
    sink_type = 'TextSink<std::string, int64_t>'
    
    inputs = './testdata/shakespeare.txt'
    outputs = './testdata/counts.txt'
    
    source_desc = typhoon_pb2.StoreDescription(
        type=source_type,
        name='mapinput.0',
    )
    
    map_output = typhoon_pb2.StoreDescription()
    map_output.type = mem_store
    map_output.name = 'mapoutput.0'
    
    text_output = typhoon_pb2.StoreDescription(
        type=sink_type,
        name='reduceoutput.0',
    )
    
    map_task = typhoon_pb2.TaskDescription(
        id='mapper.0',
        type=mapper,
        source = [ source_desc ],
        sink = [ map_output ],                                       
    )
    
    reduce_task = typhoon_pb2.TaskDescription(
      id = 'reducer.0',
      type = 'CountReducer',
      source = [ map_output ],
      sink = [ text_output ],
    )
    
    graph = typhoon_pb2.RunGraphRequest(
       task=[ map_task, reduce_task ]
    )
    
    channel = implementations.insecure_channel('localhost', 29999)
    stub = typhoon_pb2.beta_create_TyphoonMaster_stub(channel)
    for response in stub.run(graph, 3600):
        logging.info('Response: %s', response)
    
if __name__ == '__main__':
    main()