# 修复：两处「只在成功时才 return」的无界轮询测试（#1016）

## 需求描述

`collector/coordinator/extra_job.go` 的 `CheckUniqueIndexExistsJob` 是一个后台守护任务：
当用户配置了 `incr_sync.shard_by_object_id.white_list` 时，它周期性检查白名单里的集合是否存在
**非 `_id` 的唯一索引**。存在就必须中止——按 `_id` 哈希分发会打乱同一文档上多个操作的顺序，
而唯一索引依赖这个顺序，结果是目标端可能出现重复键错误或数据不一致（具体序列见 #1002）。

发布回归时发现：`go test ./...` 在 `collector/coordinator` 包上挂满 30 分钟超时，
堆栈停在 `TestExtraJob` → `innerRun`。单独跑同一个测试 1.17s 通过并正确返回唯一索引错误；
`go test -p 1 ./...` 整包 2.1s 通过。

## 根因分析

### 1. `innerRun` 把「一次检查」和「无限轮询」揉在一起

```go
// 修复前
for range time.NewTicker(time.Duration(cui.interval) * time.Second).C {
    ...
    // 只有「找到唯一索引」这一条 return 路径
}
return nil   // 永远不可达
```

循环唯一的出口就是找到它要找的东西。生产环境里这是刻意的（守护任务本就该轮询到进程结束），
但 `TestExtraJob` 是**同步调用** `innerRun` 的，于是「这一 tick 没看到索引」直接等价于「永远不返回」。

触发条件是包间串扰：`go test ./...` 并行跑所有包，而本仓库每个包都指向同一个 MongoDB。
原测试用的是共享的 `test` 库，别的包 drop 或改写 `test` 库时，`test.c2` 上的唯一索引瞬间消失，
循环就再也出不来了。这不是内存模型意义上的 race，`-race` 报不出来——它是一次无界等待。

连带后果更麻烦：挂住的测试会阻塞 `collector/coordinator` 包里所有后续测试，
新加的用例静默地根本没跑。

### 2. ticker 从未 Stop

`time.NewTicker(...)` 没有赋值给变量，`Stop()` 无从调用；`RunExtraJob` 又是
`go job.Run()` 发射后不管（`replication.go:49`），没有任何 shutdown 钩子。
与 #1008 在 `common/metric.go` 修掉的是同一类缺陷。

### 3. 连接失败会静默废掉这道保护

```go
conns[i], err = utils.NewMongoCommunityConn(...)
if err != nil {
    l.Logger.Errorf(...)
    return nil      // <-- 保护就此不存在了
}
```

`Run()` 对返回的 error 一律 `Panicf`，也就是说**快速失败本来就是设计意图**。
而连接失败返回 `nil`，意味着 URL 写错或源端不可达时，这道唯一索引保护悄无声息地什么都不做，
同步照常进行——对一个「职责就是中止不安全配置」的守护任务来说，这是最坏的结局。

顺带：循环遇错即 return，多源场景下只有第一个失败会被报告，其余源根本没尝试连接。

## 修复方案

### 1. 拆出 `checkOnce`

把「一轮检查」从轮询循环里抽出来：

```go
func (cui *CheckUniqueIndexExistsJob) checkOnce(conns []*utils.MongoCommunityConn, nsList []utils.NS) error
```

`innerRun` 只负责建连接、解析 namespace、驱动 ticker；每 tick 调一次 `checkOnce`，
拿到 error 就返回。测试直接调 `checkOnce`——**有界、确定性、跑完就回**，
从根上消除了挂死，而不是靠给测试加超时把症状盖住。

### 2. ticker 赋值 + `defer Stop()`

与 #1008 的处理保持一致。

### 3. 连接失败返回 error

```go
return fmt.Errorf("extra job[%s] connect source[%v] failed: %v",
    cui.Name(), utils.BlockMongoUrlPassword(source.URL, "***"), err)
```

**这是一处有意的行为变更**：源端不可达时从「静默继续」变成「中止本次运行」。
理由是启动顺序——`coordinator.Run()` 先调 `sanitizeMongoDB()`（逐个连接 `MongoD` 源，
失败即返回 error），再调 `RunExtraJob()`。走到这里时源端刚被验证过，
此刻连不上说明配置或网络确实有问题，而用户显式打开 `shard_by_object_id.white_list`
就是在要求这道检查。宁可响亮地失败，也不要静默地失去保护。

同时用 `defer` 关闭已建立的连接：`conns[i]` 对失败位置之后的源保持 nil，
defer 里做 nil 判断后逐个 `Close()`。

### 4. 测试各自使用私有库

`extra_job_ut0` ~ `extra_job_ut4`，不再用共享的 `test` 库，也不 drop 别人的数据。
这直接消除了触发挂死的串扰来源。

## 影响范围

- `collector/coordinator/extra_job.go`：`innerRun` 重构 + 新增 `checkOnce`。
- `collector/coordinator/extra_job_test.go`：改为调 `checkOnce`；新增 3 个用例。
- 生产行为：唯一索引检查逻辑本身**完全不变**（同样的 `List` + `name`/`unique` 判定 +
  `_id` 前缀排除 + 同样的错误文案）。变的只有连接失败的处理，以及 ticker/连接的回收。
- 无新增配置项。

### 已知边界

- 守护任务本身仍然轮询到进程结束——这是设计意图，没有引入 stop channel。
  仓库目前没有 collector 侧的 shutdown 钩子，加一个没人 close 的 channel 只是死代码。
- `interval <= 0` 会让 `time.NewTicker` panic，这是既有行为，未改动
  （`RunExtraJob` 固定传 10）。

## 连带发现：修掉挂死会暴露别的包的潜在串扰

用 `git worktree add /tmp/base-f2b9408 f2b9408` 在**基线**上跑同一条并行命令做对比，
才发现修复改变了并行门禁的失败构成：

| 包 | 基线并行 | 修好 extra_job 后并行 | `-p 1`（发布门禁） |
|---|---|---|---|
| `collector/coordinator` | FAIL 601.8s（挂死） | **ok 3.234s** | ok 2.835s |
| `collector/reader` | FAIL 9.6s（快速失败） | FAIL **600.5s**（挂死） | ok 28.049s |
| `common` | ok 7.3s | FAIL 4.6s | ok 4.332s |

机制：`TestExtraJob` 是 `collector/coordinator` 的**第一个**测试（`extra_job_test.go`
字母序在 `utils_test.go` 之前），它挂满超时 ⇒ 同包其余测试**从未执行**，该包对 27030
只产生开头几个事件后就静默 10 分钟。修好之后整包 3.2s 跑完，写入突发进入并行窗口，
把 `collector/reader` 与 `common` 的潜在缺陷掀了出来。

**定性纪律**：不能只看"红/绿"，要看**红的形态**（快速失败 vs 挂死）——形态变了才说明有因果关系；
而"是否引入回归"必须落到 `-p 1` 门禁上判定，两处并行失败在 `-p 1` 下都是 ok。

## 第二处同类缺陷：`TestEventReader`

`collector/reader/event_reader_test.go` 是与 #1016 完全同形的缺陷，一并修掉：

### 1. namespace 过滤写成了 `&&`

```go
// 修复前
if val.Ns["db"] != "db1" && val.Ns["coll"] != "c1" {   // 想跳过"不是 db1.c1"的事件
    time.Sleep(2 * time.Second)
    continue
}
```

reader 是 cluster-wide 的，服务器上的每个 namespace 都看得见。这个条件按德摩根律等价于
「只要 db 是 `db1` **或** coll 是 `c1` 就当成自己的事件处理」，于是**任何库里名为 `c1` 的集合**
都会被喂进后面的 `m["x"].(int32)`——而 `createIndexes` 这类事件根本没有 `FullDocument`，
不是断言失败而是直接 panic。

改为正向匹配并抽成谓词，用表驱动测试钉住（唯一命名之后 `&&`/`||` 的差异在端到端里已经触发不到，
不单独测就等于没有护栏）：

```go
func eventReaderOwnNamespace(val *oplog.Event) bool {
    return val.Ns["db"] == testDB && val.Ns["coll"] == testColl
}
```

### 2. 循环除了成功没有别的出口

唯一 `break` 在 `startIndex >= testInsertTimes`，而 `startIndex` 只在事件通过过滤时才 +1。
抓到的 goroutine 栈停在**第 86 行**（`unexpect ns` → `time.Sleep(2s)` → `continue`），
即在无尽外来事件里空转、永远凑不满 10 个，`wg.Wait()` 随之永久阻塞。

改为进入循环先查 deadline（`testReadDeadline`），超时用 `t.Errorf` + `return`
（不能用 `t.Fatal`——它必须在测试 goroutine 里调用）。同时**删掉外来事件分支的 2s sleep**：
那是纯 `continue` 路径，睡 2 秒没有任何意义，正是空转被放大的原因。

超时分支的 sleep 一开始保留了，理由写在注释里；但实测证明**它才是这个测试慢的主因**，
见下面第 6 点，最终也删掉了。

顺带删掉了超时分支里的 `fmt.Printf("timeout, resumeToken: %v", er.client.ResumeToken())`：
`EnsureNetwork` 失败时 `er.client` 为 nil，`ResumeToken()` 会 nil-deref panic
（env 配错时实测到过，栈停在 `common/change_stream.go:152`）。

### 3. 两个包互删对方的库

`event_reader_test.go:41-49` 与 `common/community_client_test.go:59-68` **都会把 27030 上
除 admin/local/config 之外的库全删**。并行时互删 fixture，`common` 因此报
`Cannot create collection test_db.test - database is in the process of being dropped`（code 215）。

reader 侧改为只 drop 自己的私有库 `ut_event_reader_db`。（`executor/db_writer_test.go:1791/1831`
的 `ListDatabaseNames` 只是读取校验，不是 mass-drop，全仓就这两处。）

**但 `common` 侧修不了，而且是结构性的**：`TestCommonFunctions` 断言的是
`conn.HasUniqueIndex(bson.M{"type": "collection"})` == false，这是**服务器级**查询。
并行跑时它扫到的是全服务器的集合，实测日志：

```
HasUniqueIndex checkNs:[{test_db3 test_ut} {ts_cs_warn_log_test system.views} ...
                        {ut_event_reader_db ut_event_reader_coll} {writer_test a}]
Found unique index x_1 on writer_test.a in auto shard mode
```

`writer_test.a` 是 `executor` 包的 fixture，带唯一索引。只要多个包共享一个 MongoDB 并行跑，
这个断言就**不可能**稳定通过——不是时序问题，是断言本身假设了独占服务器。
本轮不动它（另一个包、另一个测试的职责）；结论是**并行 `go test ./...` 无法做到全绿**，
发布门禁只能是 `-p 1`。

### 4. 原来的 break 条件少校验一条

`startIndex` 从首个 x（=1）起，`break` 条件是 `startIndex >= 10`，所以只处理了 x=1..9
就退出，**第 10 条从未被校验**。改为 `> testInsertTimes` 并在 `wg.Wait()` 后断言
`startIndex == testInsertTimes+1`，这样"流被截短"也会红，而不是静默通过。

### 5. 私有库 + 唯一集合名

`db1`/`c1` 换成 `ut_event_reader_db`/`ut_event_reader_coll`。`c1` 这种通用名正是
`&&` 那个 bug 的触发条件——别的包（包括修复前的 `TestExtraJob`）都在建 `c1`。

### 6. `buffer_time = 0` 让 `select` 变成抛硬币，是这个测试慢的真正主因

`EventReader.get()`：

```go
select {
case ret := <-er.eventChan:
	return ret.log, ret.err
case <-time.After(time.Second * time.Duration(conf.Options.IncrSyncReaderBufferTime)):
	return nil, TimeoutError
}
```

`incr_sync.reader.buffer_time` **既不在 `conf/collector.conf` 模板里，也没有在
`DefaultConfiguration()` 里播种**（全仓只有 `configure.go:113` 一处结构体定义），
所以测试和生产里它都是 **0** ⇒ `time.After(0)` 永远立刻就绪。

Go 的 `select` 在**多个 case 同时就绪时随机选一个**。于是即便 `eventChan` 里已经有事件，
每次 `Next()` 也有约一半概率返回 `TimeoutError`；原来的代码在这条分支上再睡 2 秒。
实测代价：

| 场景 | 保留 2s sleep（buffer_time=0） | 设 `buffer_time=1` 并删掉 sleep |
|---|---|---|
| 单独跑 | 33.12s | **13.73s** |
| 与 `common` 两包并行 | 113.46s | **13.37s** |

13.7s 基本等于测试自己那 13s 固定 sleep（3s 等 fetcher + 10s 再插入），
说明读取本身已经是瞬时的。两包并行从 113s 掉回 13.4s，也解释了完整并行下
为什么会撞到 180s deadline——**不是丢事件，是被这个抛硬币 + 2s sleep 拖死的**。

修法是在测试里显式设 `conf.Options.IncrSyncReaderBufferTime = 1`（该测试本来就已经在设
`IncrSyncReaderFetchBatchSize`），然后删掉超时分支的人为 sleep——`Next()` 自己已经阻塞过了。

> **顺带记录一个未处理的观察**：既然该键在生产配置里也是 0，`get()` 在生产中同样是
> 一空就立刻返回 `TimeoutError`，由调用方（`collector/syncer.go`）决定重试节奏。
> 本轮**没有动它**——那是生产行为，超出 #1016 的测试修复范围，需要单独评估。

## 验证

```bash
export MONGOSHAKE_TEST_URL="mongodb://127.0.0.1:27030" \
       MONGOSHAKE_TEST_URL_SHARDING="mongodb://127.0.0.1:27018" \
       MONGOSHAKE_TEST_URL_CONFIG_SERVER="mongodb://127.0.0.1:27020"

# 红：修复前 innerRun 对不可达源返回 nil
go test ./collector/coordinator -run TestCheckUniqueIndexExistsJobReportsConnectFailure -v
#   Error: Should not be: <nil>
#   Messages: connect failure must not silently disable the guard

# 绿：修复后
go test ./collector/coordinator -run 'TestExtraJob|TestCheckUniqueIndexExistsJob' -v -count=1
#   --- PASS: TestExtraJob (0.68s)                                       # 3 个子用例
#   --- PASS: TestCheckUniqueIndexExistsJobInnerRunReturnsOnUniqueIndex (1.16s)
#   --- PASS: TestCheckUniqueIndexExistsJobReportsConnectFailure (0.00s)
#   ok  github.com/alibaba/MongoShake/v2/collector/coordinator  2.227s
```

新增用例覆盖：

| 用例 | 覆盖点 |
|---|---|
| `TestExtraJob` case 0 | 非 `_id` 唯一索引被报出，且错误文案含集合名 |
| `TestExtraJob` case 1 | 普通（非唯一）索引不报 |
| `TestExtraJob` case 2 | `_id` 唯一索引不报 |
| `...InnerRunReturnsOnUniqueIndex` | 轮询循环确实有出口：找到索引后 `innerRun` 返回，30s 内不回则 `t.Fatal` |
| `...ReportsConnectFailure` | 源端不可达返回 error 而非 nil |

后两个用例都用 goroutine + `select`/`time.After` 包住，所以将来再出现无界等待，
代价是一个 30 秒的红测试，而不是 30 分钟的挂死加上整包其他用例被一起掩盖。

发布门禁（`go test -p 1 ./...` 全量单测 + `-tags integration ./integration` 集成测试）结果见提交说明。

### `collector/reader`

```bash
# 绿：修复后（整包，含新谓词测试）
go test ./collector/reader -v -count=1
#   --- PASS: TestEventReaderOwnNamespace (0.00s)
#   --- PASS: TestEventReader (33.12s)
#   --- PASS: TestCloneOplogRawDetachesCursorBuffer (0.00s)
#   ok  github.com/alibaba/MongoShake/v2/collector/reader  33.326s

# 红绿对照：把谓词改回等价的错误形式（|| 即原 && 跳过条件的德摩根反面）
#   return val.Ns["db"] == testDB || val.Ns["coll"] == testColl
#   --- FAIL: TestEventReaderOwnNamespace (0.00s)
#     Messages: same collection name in another database
#     Messages: another collection in this database
```

`testReadDeadline` 取 180s：实测空载 27–33s，180s 足够扛住并行负载下的抖动，
同时仍比包级超时短一个数量级——真挂死时 `collector/reader` 在 3 分钟内红，
而不是吃满 10 分钟并把同包后续用例一起掩盖。
