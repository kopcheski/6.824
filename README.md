# Introdution

This code is downloaded from MIT 6.824 course.

You can find the course here.  [course](https://pdos.csail.mit.edu/6.824/schedule.html) 

Please use the command below if you want to clone the original code.

```shell
$ git clone git://g.csail.mit.edu/6.824-golabs-2022 6.824
```

# MapReduce(Done)

In src/main directory, run the command below.

```shell
bash test-mr.sh
```

It will show **PASSED ALL TESTS**

# Raft

## 2A (DONE)

```shell
go test -run 2A --race

#PASS
#ok      6.824/raft      16.257s
```

## 2B(DONE)

```shell
go test -run 2B --race > log.txt

#PASS
#ok      6.824/raft    71.911s
```

## 2C(Pass with issue)

```go
rf.heartbeatDurationMillSecond = 20
```

```shell
go test -run 2C --race > log.txt

#PASS
#ok  	6.824/raft	142.222s
```

In the requirement, it asks us to set the heartbeat inteval to 100 ms. 

However, current implementation can only pass when set it below 25 ms.

Please feel free to change the code and I would appreciate it if you can commit the better version : -) .