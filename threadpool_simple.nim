import macros, cpuinfo

when not compileOption("threads"):
    {.error: "ThreadPool requires --threads:on compiler option".}

type
    ThreadPool* = ref object
        chanTo: ChannelTo # Tasks are added to this channel
        chanFrom: ChannelFrom # Results are read from this channel
        threads: seq[ThreadType]
        maxThreads: int

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

    ThreadProcArgs = object
        chanTo: ChannelToPtr
        chanFrom: ChannelFromPtr

    ThreadType = Thread[ThreadProcArgs]


proc cleanupAux(tp: ThreadPool) =
    var msg: MsgTo
    msg.new()
    msg.complete = true
    for i in 0 ..< tp.threads.len:
        tp.chanTo.send(msg)
    joinThreads(tp.threads)

proc sync*(tp: ThreadPool) =
    tp.cleanupAux()
    tp.threads.setLen(0)

proc finalize(tp: ThreadPool) =
    tp.cleanupAux()
    tp.chanTo.close()
    tp.chanFrom.close()

proc threadProc(args: ThreadProcArgs) {.thread.} =
    var burstId = 0
    while true:
        let m = args.chanTo[].recv()
        if m.complete:
            break
        m.action(m, args.chanFrom)
    deallocHeap(true, false)

proc startThreads(tp: ThreadPool) =
    assert(tp.threads.len == 0)
    if tp.threads.isNil:
        tp.threads = newSeq[ThreadType](tp.maxThreads)
    else:
        tp.threads.setLen(tp.maxThreads)

    var args = ThreadProcArgs(chanTo: addr tp.chanTo, chanFrom: addr tp.chanFrom)
    for i in 0 ..< tp.maxThreads:
        createThread(tp.threads[i], threadProc, args)

proc newThreadPool*(maxThreads: int): ThreadPool =
    result.new(finalize)
    result.maxThreads = maxThreads
    result.chanTo.open()
    result.chanFrom.open()

proc newThreadPool*(): ThreadPool {.inline.} =
    newThreadPool(countProcessors())

proc newSerialThreadPool*(): ThreadPool {.inline.} =
    newThreadPool(1)

proc dispatchMessage(tp: ThreadPool, m: MsgTo) =
    if tp.threads.len == 0:
        tp.startThreads()
    tp.chanTo.send(m)

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
