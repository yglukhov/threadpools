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

    FlowVarBase = ref object {.inheritable, pure.}
        tp: ThreadPool
        idx: int # -1 if was never awaited

    FlowVar*[T] = ref object of FlowVarBase
        when T isnot void:
            v: T

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

    ChannelsArray = UncheckedArray[ChannelTo]

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

template isReady*(v: FlowVarBase): bool = v.tp.isNil

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

proc step(args: ThreadProcArgs, idx: int): bool {.inline.} =
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
        randomize(args.thisThread)

    var hits = 0
    var total = 0
    while true:
        var threadToStealFrom = args.thisThread
        var success = false
        for i in 0 ..< 2:
            if likely step(args, threadToStealFrom):
                inc total
                if i == 0:
                    inc hits
                success = true
                break
            else:
                threadToStealFrom = rand(args.totalThreads - 1)
        if not success:
            args.taskLock[].acquire()
            let m = args.chansTo[args.thisThread][].tryRecv()
            if m.dataAvailable:
                args.taskLock[].release()
                m.msg.action(m.msg, args.chanFrom)
                inc total
            elif args.isComplete[]:
                args.taskLock[].release()
                break
            else:
                inc args.numWaitingThreads[]
                args.taskCond[].wait(args.taskLock[])
                dec args.numWaitingThreads[]
                args.taskLock[].release()

    echo "thread: ", args.thisThread, " total: ", total, ", hits: ", hits, ", misses: ", total - hits

    deallocHeap(true, false)

proc startThreads(tp: ThreadPool) =
    assert(tp.threads.len == 0)
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
    result.idx = -1
    GC_ref(result)
    m.flowVar = cast[pointer](result)
    tp.dispatchMessage(m)

proc sendBack[T](v: T, c: ChannelFromPtr, flowVar: pointer) {.gcsafe.} =
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
            GC_unref(fv)
        msg.flowVar = flowVar
        c[].send(msg)

var i {.compileTime.} = 0

proc spawnAux(tp: NimNode, e: NimNode, withFlowVar: bool): NimNode =
    let msgTypeName = genSym(nskType, "MsgSub" & $i)
    inc i
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
        for j in 0 .. procTypParams[i].len - 3:
            let fieldIdent = newIdentNode($procTypParams[i][j])
            var fieldType = procTypParams[i][^2]
            if fieldType.typeKind == ntyOpenArray: # convert openarray to seq
                fieldType = copyNimTree(fieldType)
                fieldType[0] = bindSym"seq"

            msgFields.add(newNimNode(nnkIdentDefs).add(fieldIdent, fieldType, newEmptyNode()))
            theCall.add(newNimNode(nnkDotExpr).add(
                newNimNode(nnkCast).add(msgTypeName, msgParamIdent),
                fieldIdent))

            var par = e[iParam + 1]
            if par.typeKind == ntyOpenArray: # convert openarray to seq
                par = newCall(bindSym"@", par)

            msgObjConstr.add(newNimNode(nnkExprColonExpr).add(fieldIdent, par))
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

macro spawn*(tp: ThreadPool, e: typed{nkCall | nkCommand}): untyped =
    spawnAux(tp, e, getTypeInst(e).typeKind != ntyVoid)

macro spawnFV*(tp: ThreadPool, e: typed{nkCall | nkCommand}): untyped =
    spawnAux(tp, e, true)

proc nextMessage(tp: ThreadPool): int =
    let msg = tp.chanFrom.recv()
    msg.writeResult(msg)
    result = cast[FlowVarBase](msg.flowVar).idx

proc await*(v: FlowVarBase) =
    while not v.isReady:
        discard v.tp.nextMessage()
    v.idx = 0

proc awaitAny*[T](vv: openarray[FlowVar[T]]): int =
    var foundIncomplete = false
    var tp: ThreadPool
    for i, v in vv:
        if v.isReady:
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

proc awaitAndThen*[T](fv: FlowVar[T]; action: proc (x: T)) {.deprecated, inline.} =
    await(fv)
    action(fv.v)

proc read*[T](v: FlowVar[T]): T =
    await(v)
    result = v.v

proc `^`*[T](fv: FlowVar[T]): T {.inline.} = fv.read()
