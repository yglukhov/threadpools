import macros, cpuinfo

when not compileOption("threads"):
    {.error: "ThreadPool requires --threads:on compiler option".}

type
    ThreadPool* = ref object
        chanTo: ChannelTo # Tasks are added to this channel
        chanFrom: ChannelFrom # Results are read from this channel
        threads: seq[ThreadType]
        maxThreads: int

    FlowVarBase {.inheritable, pure.} = ref object
        tp: ThreadPool
        idx: int # -1 if was never awaited

    FlowVar*[T] = ref object of FlowVarBase
        when T isnot void:
            v: T

    MsgTo = object
        action: proc(flowVar: pointer, chanFrom: ChannelFromPtr) {.gcsafe.}
        flowVar: pointer

    MsgFrom = object
        writeResult: proc(): int {.gcsafe.}

    ChannelTo = Channel[MsgTo]
    ChannelFrom = Channel[MsgFrom]

    ChannelToPtr = ptr ChannelTo
    ChannelFromPtr = ptr ChannelFrom

    ThreadProcArgs = object
        chanTo: ChannelToPtr
        chanFrom: ChannelFromPtr

    ThreadType = Thread[ThreadProcArgs]

template isReadyAux(v: FlowVarBase): bool = v.tp.isNil

proc cleanupAux(tp: ThreadPool) =
    var msg: MsgTo
    for i in 0 ..< tp.threads.len:
        tp.chanTo.send(msg)
    joinThreads(tp.threads)

# XXX: Do the GC_ref GC_unref correctly.
proc sync*(tp: ThreadPool) =
    if tp.threads.len != 0:
        tp.cleanupAux()
        tp.threads.setLen(0)

proc finalize(tp: ThreadPool) =
    if tp.threads.len != 0:
        tp.cleanupAux()
        GC_unref(tp.threads)
    tp.chanTo.close()
    tp.chanFrom.close()

proc threadProc(args: ThreadProcArgs) {.thread.} =
    while true:
        let m = args.chanTo[].recv()
        if m.action.isNil:
            break
        m.action(m.flowVar, args.chanFrom)
    deallocHeap(true, false)

proc startThreads(tp: ThreadPool) =
    assert(tp.threads.len == 0)
    if tp.threads.len == 0:
        tp.threads = newSeq[ThreadType](tp.maxThreads)
        GC_ref(tp.threads)
    else:
        tp.threads.setLen(tp.maxThreads)

    var args = ThreadProcArgs(chanTo: addr tp.chanTo, chanFrom: addr tp.chanFrom)
    for i in 0 ..< tp.maxThreads:
        createThread(tp.threads[i], threadProc, args)

proc newThreadPool*(maxThreads: int, maxMessages: int): ThreadPool =
    result.new(finalize)
    result.maxThreads = maxThreads
    result.chanTo.open()#maxMessages)
    result.chanFrom.open()

proc newThreadPool*(maxThreads: int): ThreadPool {.inline.} =
    newThreadPool(maxThreads, maxThreads * 4)

proc newThreadPool*(): ThreadPool {.inline.} =
    newThreadPool(countProcessors())

proc newSerialThreadPool*(): ThreadPool {.inline.} =
    newThreadPool(1)

proc dispatchMessage(tp: ThreadPool, m: MsgTo) =
    if tp.threads.len == 0:
        tp.startThreads()
    tp.chanTo.send(m)

proc tryDispatchMessage(tp: ThreadPool, m: MsgTo): bool =
    if tp.threads.len == 0:
        tp.startThreads()
    tp.chanTo.trySend(m)

proc newFlowVar[T](tp: ThreadPool): FlowVar[T] =
    result.new()
    result.tp = tp
    result.idx = -1
    GC_ref(result)

proc sendBack[T](v: T, c: ChannelFromPtr, flowVar: pointer) {.gcsafe.} =
    if not flowVar.isNil:
        var msg: MsgFrom
        msg.writeResult = proc(): int =
            let fv = cast[FlowVar[T]](flowVar)
            fv.tp = nil
            when T isnot void:
                fv.v = v
            GC_unref(fv)
            result = fv.idx
        c[].send(msg)

macro partial(e: typed): untyped =
    let par = newNimNode(nnkPar)
    proc skipHidden(n: NimNode): NimNode =
        result = n
        while result.kind in {nnkHiddenStdConv}:
            result = result[^1]

    for i in 1 ..< e.len: par.add(skipHidden(e[i]))
    par.add(newLit(0))

    let argsIdent = newIdentNode("args")

    let transformedCall = newCall(e[0])
    for i in 1 ..< e.len:
        transformedCall.add(newNimNode(nnkBracketExpr).add(argsIdent, newLit(i - 1)))

    let resultProc = newProc(params = [newIdentNode("auto")], body = transformedCall, procType = nnkLambda)
    resultProc.addPragma(newIdentNode("gcsafe"))

    let wrapperIdent = newIdentNode("tmpWrapper")

    let wrapperProc = newProc(wrapperIdent, params = [newIdentNode("auto"), newIdentDefs(argsIdent, newIdentNode("any"))], body = resultProc)

    result = newNimNode(nnkStmtList).add(
        wrapperProc,
        newCall(wrapperIdent, par))

    # echo repr result

proc setAction(m: var MsgTo, a: proc(flowVar: pointer, chanFrom: ChannelFromPtr) {.gcsafe.}) {.inline.} =
  m.action = a

template setupAction(msg: MsgTo, e: untyped, body: untyped) =
    block:
        proc setup(m: var MsgTo, pe: proc) {.inline, nimcall.} =
            setAction(m) do(flowVar: pointer, chanFrom: ChannelFromPtr):
                let pe {.inject.} = pe
                let chanFrom {.inject.} = chanFrom
                let flowVar {.inject.} = flowVar
                body
        {.push hints:off.} # [XDeclaredButNotUsed]
        setup(msg, partial(e))
        {.pop.}

template spawnFV*(tp: ThreadPool, e: typed{nkCall | nkCommand}): auto =
    when compiles(e isnot void):
        type RetType = type(e)
    else:
        type RetType = void

    var m: MsgTo
    setupAction(m, e):
        sendBack(pe(), chanFrom, flowVar)
    let fv = newFlowVar[RetType](tp)
    m.flowVar = cast[pointer](fv)
    mixin dispatchMessage
    tp.dispatchMessage(m)
    fv

template spawn*(tp: ThreadPool, e: typed{nkCall | nkCommand}): untyped =
    when compiles(e isnot void):
        spawnFV(tp, e)
    else:
        var m: MsgTo
        setupAction(m, e):
            pe()
        mixin dispatchMessage
        tp.dispatchMessage(m)

template trySpawn*(tp: ThreadPool, e: typed{nkCall | nkCommand}): bool =
    var m: MsgTo
    setupAction(m, e):
        pe()
    mixin tryDispatchMessage
    tp.tryDispatchMessage(m)

template spawnX*(tp: ThreadPool, call: typed) =
    if not tp.trySpawn(call):
        call

proc nextMessage(tp: ThreadPool): int =
    let msg = tp.chanFrom.recv()
    result = msg.writeResult()

proc tryNextMessage(tp: ThreadPool): bool {.inline.} =
    let m = tp.chanFrom.tryRecv()
    result = m.dataAvailable
    if result:
        discard m.msg.writeResult()

proc await*(v: FlowVarBase) =
    while not v.isReadyAux:
        discard v.tp.nextMessage()
    v.idx = 0

proc awaitAny*[T](vv: openarray[FlowVar[T]]): int =
    var foundIncomplete = false
    var tp: ThreadPool
    for i, v in vv:
        if v.isReadyAux:
            if v.idx == -1:
                v.idx = 0
                return i
        else:
            v.idx = i
            foundIncomplete = true
            tp = v.tp

    if foundIncomplete:
        tp.nextMessage()
    else:
        -1

proc isReady*(v: FlowVarBase): bool =
    while not v.isReadyAux:
        if not v.tp.tryNextMessage():
            return
    result = true

proc read*[T](v: FlowVar[T]): T =
    await(v)
    result = v.v

proc `^`*[T](fv: FlowVar[T]): T {.inline.} = fv.read()

################################################################################
# Deprecated spawn
const
  MaxThreadPoolSize* = 256 ## maximal size of the thread pool. 256 threads
                           ## should be good enough for anybody ;-)
  MaxDistinguishedThread* = 32 ## maximal number of "distinguished" threads.

type
  ThreadId* = range[0..MaxDistinguishedThread-1]

var gThreadPool {.threadvar.}: ThreadPool
var gPinnedPools {.threadvar.}: seq[ThreadPool]

proc sharedThreadPool(): ThreadPool =
    if gThreadPool.isNil:
        gThreadPool = newThreadPool()
    result = gThreadPool

proc pinnedPool(id: ThreadId): ThreadPool =
    if gPinnedPools.len <= id:
        gPinnedPools.setLen(id + 1)
    if gPinnedPools[id].isNil:
        gPinnedPools[id] = newSerialThreadPool()
    result = gPinnedPools[id]

proc preferSpawn*(): bool {.deprecated.} = true

template spawn*(call: typed): untyped {.deprecated.} =
    spawn(sharedThreadPool(), call)

template pinnedSpawn*(id: ThreadId; call: typed): untyped {.deprecated.} =
    pinnedPool(id).spawn(call)
  ## always spawns a new task on the worker thread with ``id``, so that
  ## the 'call' is **always** executed on
  ## the thread. 'call' has to be proc call 'p(...)' where 'p'
  ## is gcsafe and has a return type that is either 'void' or compatible
  ## with ``FlowVar[T]``.

template spawnX*(call: typed) {.deprecated.} =
    sharedThreadPool().spawnX(call)
  ## spawns a new task if a CPU core is ready, otherwise executes the
  ## call in the calling thread. Usually it is advised to
  ## use 'spawn' in order to not block the producer for an unknown
  ## amount of time. 'call' has to be proc call 'p(...)' where 'p'
  ## is gcsafe and has a return type that is either 'void' or compatible
  ## with ``FlowVar[T]``.

# proc parallel*(body: untyped) {.magic: "Parallel".}
#   ## a parallel section can be used to execute a block in parallel. ``body``
#   ## has to be in a DSL that is a particular subset of the language. Please
#   ## refer to the manual for further information.

proc sync*() {.deprecated, inline.} =
    sharedThreadPool().sync()
