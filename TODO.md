# TODO items

## Critical

* Block reads from a source until the input task is finished
* Alternatively allow read/writes to be split into epochs.
  ( source->iterator(minEpoch=1200): return only results from
    after minEpoch
  )
* Epochs would be allow for streaming, but then there would
  need to be some sort of garbage collection of old epochs 
  in general
  
* Handle blocking for local fetches!
* Starting tasks should be non-blocking!