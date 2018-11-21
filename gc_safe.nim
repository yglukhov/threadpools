# this code should not compile
import threadpool_simple

var f = "hello"

proc gcUnsafeProc() =
  echo "hi"
  f = "bye"

proc main() =
  spawn gcUnsafeProc()
  sync()

main()

