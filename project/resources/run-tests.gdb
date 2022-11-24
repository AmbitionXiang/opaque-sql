set pagination off
handle SIGSEGV pass
catch signal SIGSEGV
commands
bt
c
end
r -Xmx32G org.scalatest.run edu.berkeley.cs.rise.opaque.SinglePartitionJoinSuite
bt
