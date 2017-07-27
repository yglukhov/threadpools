import macros, cpuinfo, locks, random

when not compileOption("threads"):
    {.error: "ThreadPool requires --threads:on compiler option".}

type
    ThreadPool* = ref object
        chansTo: seq[ChannelTo] # Tasks are added to this channel
        chanFrom: ChannelFrom # Results are read from this channel
        threads: seq[ThreadType]
        maxThreads: int
        taskCond: Cond
        taskLock: Lock
        complete: bool
        numWaitingThreads: int

    FlowVar*[T] = ref object
        tp: ThreadPool
        when T isnot void:
            v: T
        isComplete: bool

    MsgTo = ref object {.inheritable, pure.}
        action: proc(m: MsgTo, chanFrom: ChannelFromPtr) {.nimcall.}
        flowVar: pointer
        complete: bool

    MsgFrom = ref object {.inheritable, pure.}
        writeResult: proc(m: MsgFrom) {.nimcall.}
        flowVar: pointer

    ConcreteMsgFrom[T] = ref object of MsgFrom
        when T isnot void:
            v: T

    ChannelTo = Channel[MsgTo]
    ChannelFrom = Channel[MsgFrom]

    ChannelToPtr = ptr ChannelTo
    ChannelFromPtr = ptr ChannelFrom

    ChannelsArray {.unchecked.} = array[0..0, ChannelTo]

    ThreadProcArgs = object
        chansTo: seq[ChannelToPtr]
        chanFrom: ChannelFromPtr
        thisThread: int
        totalThreads: int
        taskCond: ptr Cond
        taskLock: ptr Lock
        isComplete: ptr bool
        numWaitingThreads: ptr int

    ThreadType = Thread[ThreadProcArgs]


proc cleanupAux(tp: ThreadPool) =
    tp.taskLock.acquire()
    tp.complete = true
    if tp.threads.len == 1:
        var msg: MsgTo
        msg.new()
        msg.complete = true
        tp.chansTo[0].send(msg)
    for i in 0 ..< tp.numWaitingThreads:
        tp.taskCond.signal()
    #echo "signled complete"
    tp.taskLock.release()
    joinThreads(tp.threads)
    for i in 0 ..< tp.threads.len:
        tp.chansTo[i].close()

proc sync*(tp: ThreadPool) =
    tp.cleanupAux()
    tp.threads.setLen(0)
    tp.chansTo.setLen(0)

proc finalize(tp: ThreadPool) =
    tp.cleanupAux()
    tp.chanFrom.close()

proc step(args: ThreadProcArgs, idx: int): bool =
    let m = args.chansTo[idx][].tryRecv()
    if m.dataAvailable:
        m.msg.action(m.msg, args.chanFrom)
        result = true

proc threadProc(args: ThreadProcArgs) {.thread.} =
    if args.totalThreads == 1:
        while true:
            let m = args.chansTo[args.thisThread][].recv()
            if m.complete:
                break
            m.action(m, args.chanFrom)
    else:
        var threadToStealFrom = args.thisThread
        template nextThreadToStealFrom(): int =
            threadToStealFrom = random(args.totalThreads)
            # threadToStealFrom = (threadToStealFrom * threadToStealFrom) mod args.totalThreads
            # if threadToStealFrom == args.thisThread:
            #     if threadToStealFrom == args.totalThreads - 1:
            #         dec threadToStealFrom
            #     else:
            #         inc threadToStealFrom
            threadToStealFrom

        block outer:
            while true:
                var t = args.thisThread
                var success = false
                for i in 0 ..< 4:
                    if not step(args, t):
                        t = nextThreadToStealFrom()
                    else:
                        success = true
                        break
                if not success:
                    args.taskLock[].acquire()
                    let m = args.chansTo[args.thisThread][].tryRecv()
                    if m.dataAvailable:
                        args.taskLock[].release()
                        m.msg.action(m.msg, args.chanFrom)
                    elif args.isComplete[]:
                        args.taskLock[].release()
                        break
                    else:
                        #echo args.thisThread, ": wait"
                        inc args.numWaitingThreads[]
                        args.taskCond[].wait(args.taskLock[])
                        dec args.numWaitingThreads[]
                        let comp = args.isComplete[]
                        #echo args.thisThread, ": complete: ", comp
                        args.taskLock[].release()
                        if comp:
                            break

    deallocHeap(true, false)

proc startThreads(tp: ThreadPool) =
    assert(tp.threads.len == 0)
    if tp.threads.isNil:
        tp.threads = newSeq[ThreadType](tp.maxThreads)
        tp.chansTo = newSeq[ChannelTo](tp.maxThreads)
    else:
        tp.threads.setLen(tp.maxThreads)
        tp.chansTo.setLen(tp.maxThreads)

    var args = ThreadProcArgs(chanFrom: addr tp.chanFrom, totalThreads: tp.maxThreads,
        taskCond: addr tp.taskCond, taskLock: addr tp.taskLock, isComplete: addr tp.complete,
        numWaitingThreads: addr tp.numWaitingThreads)
    args.chansTo = newSeq[ChannelToPtr](tp.maxThreads)

    for i in 0 ..< tp.maxThreads:
        args.chansTo[i] = addr tp.chansTo[i]

    for i in 0 ..< tp.maxThreads:
        tp.chansTo[i].open()
        args.thisThread = i
        createThread(tp.threads[i], threadProc, args)

proc newThreadPool*(maxThreads: int): ThreadPool =
    result.new(finalize)
    result.maxThreads = maxThreads
    result.chanFrom.open()
    result.taskCond.initCond()
    result.taskLock.initLock()

proc newThreadPool*(): ThreadPool {.inline.} =
    newThreadPool(countProcessors())

proc newSerialThreadPool*(): ThreadPool {.inline.} =
    newThreadPool(1)

proc dispatchMessage(tp: ThreadPool, m: MsgTo) =
    if tp.threads.len == 0:
        tp.startThreads()

    if tp.maxThreads == 1:
        tp.chansTo[0].send(m)
    else:
        var mostAvailableChanIdx = -1
        var mostAvailableChanTasks = int.high

        for i in 0 ..< tp.threads.len:
            let numTasks = tp.chansTo[i].peek()
            if numTasks == 0:
                mostAvailableChanIdx = i
                break
            elif numTasks < mostAvailableChanTasks:
                mostAvailableChanTasks = numTasks
                mostAvailableChanIdx = i

        tp.taskLock.acquire()
        tp.chansTo[mostAvailableChanIdx].send(m)
        tp.taskCond.signal()
        tp.taskLock.release()

proc dispatchMessageWithFlowVar[T](tp: ThreadPool, m: MsgTo): FlowVar[T] =
    result.new()
    result.tp = tp
    GC_ref(result)
    m.flowVar = cast[pointer](result)
    tp.dispatchMessage(m)

proc sendBack[T](v: T, c: ChannelFromPtr, flowVar: pointer) =
    if not flowVar.isNil:
        var msg: ConcreteMsgFrom[T]
        msg.new()
        when T isnot void:
            msg.v = v
        msg.writeResult = proc(m: MsgFrom) {.nimcall.} =
            let m = cast[ConcreteMsgFrom[T]](m)
            let fv = cast[FlowVar[T]](m.flowVar)
            fv.tp = nil
            when T isnot void:
                fv.v = m.v
            fv.isComplete = true
            GC_unref(fv)
        msg.flowVar = flowVar
        c[].send(msg)

proc spawnAux(tp: NimNode, e: NimNode, withFlowVar: bool): NimNode =
    let msgTypeName = genSym(nskType, "MsgSub")
    let dispatchProcName = genSym(nskProc, "dispatchProc")
    let msgParamIdent = newIdentNode("m")

    let origProcName = e[0]
    let procTypParams = origProcName.getTypeInst()[0]

    let msgFields = newNimNode(nnkRecList)

    let theCall = newCall(origProcName)

    let msgObjConstr = newNimNode(nnkObjConstr).add(
        msgTypeName,
        newNimNode(nnkExprColonExpr).add(
            newIdentNode("action"),
            dispatchProcName
        )
    )

    var iParam = 0
    for i in 1 ..< procTypParams.len:
        # msgFields.add(copyNimTree(procTypParams[i]))
        for j in 0 ..< procTypParams[i].len - 2:
            let fieldIdent = newIdentNode($procTypParams[i][j])
            msgFields.add(newNimNode(nnkIdentDefs).add(fieldIdent, procTypParams[i][^2], newEmptyNode()))
            theCall.add(newNimNode(nnkDotExpr).add(
                newNimNode(nnkCast).add(msgTypeName, msgParamIdent),
                fieldIdent))
            msgObjConstr.add(newNimNode(nnkExprColonExpr).add(fieldIdent, e[iParam + 1]))
            inc iParam

    let msgTypDef = newNimNode(nnkTypeSection).add(newNimNode(nnkTypeDef).add(
        msgTypeName,
        newEmptyNode(),
        newNimNode(nnkRefTy).add(
            newNimNode(nnkObjectTy).add(
                newEmptyNode(),
                newNimNode(nnkOfInherit).add(bindSym"MsgTo"),
                msgFields
            )
        )
    ))

    let chanFromIdent = newIdentNode("chanFrom")
    
    let dispatchProc = newProc(dispatchProcName, params = [
            newEmptyNode(),
            newNimNode(nnkIdentDefs).add(
                msgParamIdent,
                bindSym"MsgTo",
                newEmptyNode()
            ),
            newNimNode(nnkIdentDefs).add(
                chanFromIdent,
                bindSym"ChannelFromPtr",
                newEmptyNode()
            )
        ],
        body = newCall(bindSym"sendBack", theCall, chanFromIdent, newNimNode(nnkDotExpr).add(
                msgParamIdent, newIdentNode("flowVar")))
    )

    dispatchProc.addPragma(newIdentNode("gcsafe"))

    var dispatchCall: NimNode
    if withFlowVar:
        dispatchCall = newCall(newNimNode(nnkBracketExpr).add(bindSym"dispatchMessageWithFlowVar", procTypParams[0]), tp, msgObjConstr)
    else:
        dispatchCall = newCall(bindSym"dispatchMessage", tp, msgObjConstr)

    result = newNimNode(nnkStmtList).add(
        msgTypDef,
        dispatchProc,
        dispatchCall
    )

macro spawn*(tp: ThreadPool, e: typed{nkCall}): untyped =
    spawnAux(tp, e, false)

macro spawnFV*(tp: ThreadPool, e: typed{nkCall}): untyped =
    spawnAux(tp, e, true)

proc nextMessage(tp: ThreadPool) =
    let msg = tp.chanFrom.recv()
    msg.writeResult(msg)

proc read*[T](v: FlowVar[T]): T =
    while not v.isComplete:
        v.tp.nextMessage()
    result = v.v

when isMainModule:
    import os

    type Foo = ref object

    proc finalize(f: Foo) =
        echo "foo finalized"

    block:
        proc helloWorld(a: int): int =
            return 123 + a

        let tp = newThreadPool(4)
        const numCalcs = 100
        var results = newSeq[FlowVar[int]](numCalcs)
        for i in 0 ..< numCalcs:
            results[i] = tp.spawnFV helloWorld(i)

        for i in 0 ..< numCalcs:
            assert(results[i].read() == 123 + i)

    block:
        var ga = 0
        proc helloWorld(a: int) =
            atomicInc(ga)
            sleep(300)

        let tp = newThreadPool(1)
        const numCalcs = 10
        for i in 0 ..< numCalcs:
            tp.spawn helloWorld(i)
        tp.sync()
        assert ga == numCalcs

    echo "done"

    # tp.cleanup()

    # for j in 0 ..< 100:
    #     GC_fullCollect()
    #     let tp = newThreadPool(4)
    #     for i in 0 ..< 4:
    #         tp.spawn helloWorld(i)
    #         tp.spawn helloWorld1(i)
        # sleep(2000)


# proc foo() =
#     try:
#         foo_try()
#     except:
#         foo_except()
#     finally:
#         foo_finally()

# foo()

# const
#     SmallMinSize = 16
#     SmallMaxSize = 256
#     PageSize = 128 * 1024

# type
#     Page = ptr object
#         nextPage: Page
#         typ: int16
#         nextFreeObj: int16
#         totalObjects: int16

# proc mmap(p: pointer, len: csize, prot: cint, flags: cint, fd: cint, offset: csize): pointer {.importc, nodecl.}
# proc munmap(p: pointer, sz: cint): cint {.importc, nodecl.}
# proc getpagesize(): cint {.importc.}

# const
#     PROT_READ = cint(0x1)
#     PROT_WRITE = cint(0x2)
#     PROT_EXEC = cint(0x4)
#     PROT_NONE = cint(0x0)

# var MAP_ANONYMOUS {.importc: "MAP_ANONYMOUS", header: "<sys/mman.h>".}: cint
# var MAP_PRIVATE {.importc: "MAP_PRIVATE", header: "<sys/mman.h>".}: cint


# var myerrno {.importc: "errno", header: "<errno.h>".}: cint ## error variable

# proc strerror(errnum: cint): cstring {.importc.}


# proc allocPages(count: cint): Page =
#     # mmap returns pointer aligned to system page size which is normally less
#     # than PageSize. That is why we mmap 2 pages more than needed, to get
#     # PageSize-aligned pointer and munmap all the rest

#     result = cast[Page](mmap(nil, PageSize * (count + 2), PROT_READ or PROT_WRITE, MAP_ANONYMOUS or MAP_PRIVATE, -1, 0))

#     #echo "mmapped: ", cast[uint](result)

# #    echo "Errno: ", myerrno, ": ", strerror(myerrno)

#     assert(cast[uint](result) != cast[uint](-1))

#     #echo "offset into: ", (cast[uint](PageSize) - cast[uint](result) mod PageSize)

#     let alignedStart = cast[uint](result) + (cast[uint](PageSize) - cast[uint](result) mod PageSize)
#     if unlikely cast[uint](result) == alignedStart:
#         #echo "result aligned"
#         # result is properly aligned. munmap the tail
#         discard munmap(cast[pointer](cast[uint](result) + PageSize), PageSize * (count + 1))
#     else:
#         #echo "alignedStart: ", alignedStart
#         # aligned pointer is in the middle.
#         # munmap front
#         discard munmap(result, cast[cint](alignedStart - cast[uint](result)))
#         # munmap tail
#         let pend = cast[uint](result) + cast[uint](PageSize * (count + 1))
#         discard munmap(cast[pointer](alignedStart + PageSize), cast[cint](pend - alignedStart))
#         result = cast[Page](alignedStart)

#     assert(cast[uint](result) mod PageSize == 0)


# proc isSmallSize(sz: int): bool = sz <= SmallMaxSize

# template offset(objectSize: int16): int16 =
#     SmallMaxSize

# proc pageCapacity(objectSize: int16): int16 {.inline.} =
#     int16((PageSize - offset(objectSize)) div objectSize)

# proc capacity(p: Page): int16 = pageCapacity(p.typ)

# proc getPage(p: pointer): Page =
#     result = cast[Page](cast[uint](p) - (cast[uint](p) mod PageSize))

# var nextFree16: Page

# proc pointerAtIndex(p: Page, i: int): pointer =
#     cast[pointer](cast[uint](p) + cast[uint](offset(p.typ) + i * p.typ))

# proc allocSmallPage(): Page =
#     result = allocPages(1)
#     echo "allocSmallPage ", cast[int](result)
#     result.nextPage = nil
#     result.typ = 16
#     result.nextFreeObj = 0
#     result.totalObjects = 0
#     let first = cast[ptr int16](result.pointerAtIndex(0))
#     first[] = -1

# proc deallocSmallPage(p: Page) =
#     discard munmap(p, PageSize)

# proc allocSmall16(): pointer =
#     if nextFree16.isNil:
#         nextFree16 = allocSmallPage()

# #    echo "Allocating obj ", nextFree16.nextFreeObj
#     result = nextFree16.pointerAtIndex(nextFree16.nextFreeObj)
#     let follow = cast[ptr int16](result)[]
#     if follow == -1:
#         inc nextFree16.nextFreeObj
#         let p = nextFree16.pointerAtIndex(nextFree16.nextFreeObj)
#         cast[ptr int16](p)[] = -1
#     else:
#         nextFree16.nextFreeObj = follow
#     inc nextFree16.totalObjects
#     if nextFree16.totalObjects == pageCapacity(16) - 1:
#         nextFree16 = nil

# proc indexOfPointer(page: Page, p: pointer): int16 =
#     cast[int16](cast[uint](p) - cast[uint](page) - cast[uint](offset(page.typ))) div page.typ

# proc deallocSmall16(page: Page, p: pointer) =
#     let p = cast[ptr int16](p)
#     p[] = page.nextFreeObj
#     let i = page.indexOfPointer(p)
# #    echo "Freeing object ", i
#     page.nextFreeObj = i
#     dec page.totalObjects
#     if nextFree16.isNil:
#         nextFree16 = page
#     elif page.totalObjects == 0 and nextFree16 != page:
#         deallocSmallPage(page)

# proc roundSmallSize(sz: int): int =
#     if sz <= 16: return 16
#     if sz <= 32: return 32
#     if sz <= 64: return 64
#     if sz <= 128: return 128
#     return 256

# proc allocSmall(sz: int): pointer =
#     let sz = roundSmallSize(sz)
#     if sz == 16: result = allocSmall16()
#     else:
#         assert(false)

# proc qmalloc(sz: int): pointer =
#     if isSmallSize(sz):
#         result = allocSmall(sz)
#     else:
#         assert(false)
# #        allocBig(sz)

# proc qfree(p: pointer) =
#     let page = p.getPage()
#     if isSmallSize(page.typ):
#         if page.typ == 16: page.deallocSmall16(p)
#         else:
#             assert(false)
#     else:
#         assert(false)

# when isMainModule:
#     import random, times

#     proc c_malloc(sz: csize): pointer {.importc: "malloc".}
#     proc c_free(p: pointer) {.importc: "free".}

#     template testAllocator(alloc: untyped, dealloc: untyped) =
#         block:
#             let count = 300
#             var pointers = newSeq[ptr int16](count)

#             for i in 0 .. 10000000:
#                 let j = random(count)
#                 let val = int16(j.float * 2.2)

#                 if not pointers[j].isNil:
#                     assert(pointers[j][] == val)
#                     dealloc(pointers[j])
#                 pointers[j] = cast[ptr int16](alloc(16))
#                 pointers[j][] = val

#     template bench(name: string, b: untyped) =
#         block:
#             stdout.write("Running ")
#             stdout.write(name)
#             stdout.write("...")
#             let s = epochTime()
#             b
#             let e = epochTime()
#             echo " Done: ", e - s


#     bench "malloc":
#         testAllocator(c_malloc, c_free)

#     bench "qmalloc":
#         testAllocator(qmalloc, qfree)

#     bench "nimalloc":
#         testAllocator(alloc, dealloc)


# import os
# import imgtools.imgtools
# import nimPNG

# for f in walkfiles("/Users/yglukhov/Projects/falcon/res/tiledmap/assets/*.png"):
#     echo "F: ", f
#     var png = loadPNG32(f)

#     if png.data.len == png.width * png.height * 4:
#         zeroColorIfZeroAlpha(png.data)
#         colorBleed(png.data, png.width, png.height)

#     discard savePNG32(f, png.data, png.width, png.height)
