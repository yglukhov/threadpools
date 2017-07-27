import threadpool_simple as tps
import threadpool_complex as tpc
import threadpool as tp
import times, os, mersenne

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

const simpleCallIterations = 100000

bench "complex pool - simpleCall":
    let p = tpc.newThreadPool()
    for i in 0 ..< simpleCallIterations:
        p.spawn simpleCall()
    p.sync()

doAssert(callsMade == simpleCallIterations)
callsMade = 0

bench "simple pool - simpleCall":
    let p = tps.newThreadPool()
    for i in 0 ..< simpleCallIterations:
        p.spawn simpleCall()
    p.sync()

doAssert(callsMade == simpleCallIterations)

proc sleepForTime(a: int) =
    sleep(a)

const randomSeed = 12345
var randomGen = newMersenneTwister(randomSeed)

const randomSleepIterations = 100

var totalSleepTime1 = 0
var totalSleepTime2 = 0

bench "complex pool - sleepForTime":
    let p = tpc.newThreadPool()
    for i in 0 ..< randomSleepIterations:
        let s = randomGen.getNum() mod 300
        totalSleepTime1 += s
        p.spawn sleepForTime(s)
    p.sync()

randomGen = newMersenneTwister(randomSeed)

bench "simple pool - sleepForTime":
    let p = tps.newThreadPool()
    for i in 0 ..< randomSleepIterations:
        let s = randomGen.getNum() mod 300
        totalSleepTime2 += s
        p.spawn sleepForTime(s)
    p.sync()

echo "totalSleepTime1: ", totalSleepTime1
echo "totalSleepTime2: ", totalSleepTime2

assert(totalSleepTime1 == totalSleepTime2)
