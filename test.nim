import threadpool_simple as tps
import threadpool_complex as tpc
import threadpool as tp
import times, os, random

template bench(name: string, body: untyped) =
    proc runBench() {.gensym.} =
        let s = epochTime()
        body
        let e = epochTime()
        echo name, ": ", e - s
    runBench()


block:
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

block:
    proc sleepForTime(a: int) =
        sleep(a)

    const randomSeed = 12345
    var randomGen = initRand(randomSeed)

    const randomSleepIterations = 100

    var totalSleepTime1 = 0
    var totalSleepTime2 = 0

    bench "complex pool - sleepForTime":
        let p = tpc.newThreadPool()
        for i in 0 ..< randomSleepIterations:
            let s = randomGen.rand(300)
            totalSleepTime1 += s
            p.spawn sleepForTime s
        p.sync()

    randomGen = initRand(randomSeed)

    bench "simple pool - sleepForTime":
        let p = tps.newThreadPool()
        for i in 0 ..< randomSleepIterations:
            let s = randomGen.rand(300)
            totalSleepTime2 += s
            p.spawn sleepForTime s
        p.sync()

    assert(totalSleepTime1 == totalSleepTime2)


block:
    proc sleepAndReturnSomeResult(a: int): int =
        sleep(a)
        return a + 1

    const randomSeed = 54312
    var randomGen = initRand(randomSeed)

    const randomSleepIterations = 100

    var totalSleepTime1 = 0
    var totalSleepTime2 = 0

    bench "complex pool - sleepAndReturnSomeResult":
        let p = tpc.newThreadPool()
        var results = newSeq[tpc.FlowVar[int]](randomSleepIterations)
        for i in 0 ..< randomSleepIterations:
            let s = randomGen.rand(300)
            totalSleepTime1 += s
            results[i] = p.spawn sleepAndReturnSomeResult(s)

        randomGen = initRand(randomSeed)
        for i in 0 ..< randomSleepIterations:
            let s = randomGen.rand(300)
            doAssert(results[i].read() == s + 1)

        p.sync()

    randomGen = initRand(randomSeed)

    bench "simple pool - sleepAndReturnSomeResult":
        let p = tps.newThreadPool()
        var results = newSeq[tps.FlowVar[int]](randomSleepIterations)
        for i in 0 ..< randomSleepIterations:
            let s = randomGen.rand(300)
            totalSleepTime2 += s
            results[i] = p.spawn sleepAndReturnSomeResult(s)

        randomGen = initRand(randomSeed)
        for i in 0 ..< randomSleepIterations:
            let s = randomGen.rand(300)
            doAssert(results[i].read() == s + 1)

        p.sync()

    assert(totalSleepTime1 == totalSleepTime2)


block:
    let p = tps.newThreadPool()

    proc sleepAndReturnSomeResult(a: int): int =
        sleep(a)
        return a + 1

    const randomSeed = 83729
    var randomGen = initRand(randomSeed)

    const randomSleepIterations = 100

    bench "simple pool - awaitAny":
        let p = tps.newThreadPool()
        var results = newSeq[tps.FlowVar[int]](randomSleepIterations)
        for i in 0 ..< randomSleepIterations:
            let s = randomGen.rand(300)
            results[i] = p.spawn sleepAndReturnSomeResult(s)

        var iResults = newSeq[int](randomSleepIterations)

        iResults[5] = results[5].read()
        iResults[15] = results[15].read()

        while true:
            let i = awaitAny(results)
            if i == -1:
                break
            iResults[i] = results[i].read()

        randomGen = initRand(randomSeed)
        for i in 0 ..< randomSleepIterations:
            let s = randomGen.rand(300)
            doAssert(iResults[i] == s + 1)

        p.sync()

    randomGen = initRand(randomSeed)

    bench "complex pool - awaitAny":
        let p = tpc.newThreadPool()
        var results = newSeq[tpc.FlowVar[int]](randomSleepIterations)
        for i in 0 ..< randomSleepIterations:
            let s = randomGen.rand(300)
            results[i] = p.spawn sleepAndReturnSomeResult(s)

        var iResults = newSeq[int](randomSleepIterations)

        iResults[5] = results[5].read()
        iResults[15] = results[15].read()

        while true:
            let i = awaitAny(results)
            if i == -1:
                break
            iResults[i] = results[i].read()

        randomGen = initRand(randomSeed)
        for i in 0 ..< randomSleepIterations:
            let s = randomGen.rand(300)
            doAssert(iResults[i] == s + 1)

        p.sync()


block:
    let p = tps.newThreadPool()
    proc sleepAndReturnSomeResult(a: int): int =
        sleep(a)
        return a + 1
    let s = p.spawn sleepAndReturnSomeResult(100)
    while not s.isReady:
        sleep(10)
        echo "wating..."
    doAssert(^s == 101)

block: # openarrays
    let p = tps.newThreadPool()
    proc sleepAndReturnSomeResult(numbers: openarray[int]): int =
        for i in numbers:
            result += i
    let se = @[1, 2, 3]
    let s = p.spawn sleepAndReturnSomeResult(se)
    doAssert(^s == 6)
