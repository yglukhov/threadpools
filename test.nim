import threadpool_simple as tps
import threadpool_complex as tpc
import threadpool as tp
import times

template bench(name: string, body: untyped) =
    proc runBench() {.gensym.} =
        let s = epochTime()
        body
        let e = epochTime()
        echo name, ": ", e - s
    runBench()


var callsMade = 0

proc simpleCall() =
    atomicInc(callsMade)

const iterations = 100000

bench "complex pool - simpleCall":
    let p = tpc.newThreadPool()
    for i in 0 ..< iterations:
        p.spawn simpleCall()
    p.sync()

doAssert(callsMade == iterations)
callsMade = 0

bench "simple pool - simpleCall":
    let p = tps.newThreadPool()
    for i in 0 ..< iterations:
        p.spawn simpleCall()
    p.sync()

doAssert(callsMade == iterations)
