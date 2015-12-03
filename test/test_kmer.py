import logging
import os
import sys
import subprocess
import time

from typhoon import typhoon_pb2
from grpc.beta import implementations

def test_kmer():
    source_desc = typhoon_pb2.StoreDescription(
        type='BlockSource',
        name='mapinput.0',
        source=typhoon_pb2.FileSplit(
          filename='./testdata/hs_alt_CHM1_1.1_chr1.fa',
          start=0,
          end=int(10e6),
        )
    )

    map_output = typhoon_pb2.StoreDescription(
        type='MemStore',
        name='mapoutput.0',
        combiner='CountReducer',
    )

    text_output = typhoon_pb2.StoreDescription(
        type='TextSink',
        name='reduceoutput.0',
        source=typhoon_pb2.FileSplit(
          filename='./testdata/kmer.txt',
        )
    )

    map_task = typhoon_pb2.TaskDescription(
        id='mapper.0',
        type='KMERMapper',
        source = [ source_desc ],
        sink = [ map_output ],
    )

    reduce_task = typhoon_pb2.TaskDescription(
      id = 'reducer.0',
      type = 'SumReducer',
      source = [ map_output ],
      sink = [ text_output ],
    )

    graph = typhoon_pb2.RunGraphRequest(
       task=[ map_task, reduce_task ]
    )
    return graph

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    try:
        master = subprocess.Popen(['python', 'typhoon/master.py'])
        time.sleep(1)
        worker = subprocess.Popen(['./build/typhoon_worker'], env={ 'MASTER_ADDRESS': '127.0.0.1:29999' })
        graph = test_kmer()

        channel = implementations.insecure_channel('localhost', 29999)
        time.sleep(1)
        stub = typhoon_pb2.beta_create_TyphoonMaster_stub(channel)
        response = stub.execute(graph, 3600)
        logging.info('Response: %s', response)

        time.sleep(60)
        logging.info('Success!')
    except:
        logging.info('Error.', exc_info=1)
    finally:
        master.kill()
        worker.kill()
        os._exit(1)

if __name__ == '__main__':
    main()
