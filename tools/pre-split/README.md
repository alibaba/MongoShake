## 预分片工具

### 背景
没有预分片的情况下，DTS等数据迁移工具分片迁移可能会出现写入倾斜的原因（尤其是范围分片且shard key单调递增的情况），而导致迁移期间性能受限。

最好能在开启全量同步前，在目标端进行预分片的操作，使得后续的写入尽可能均匀地落在所有分片上。

> PS：目前仅为预览版本，包含的功能仍有完善空间。欢迎使用体验以及共建，未来不排除移植进MongoShake中并提供可选配置项的考虑。

### usage
```bash
./pre_split.linux -log_dir=./ -src_url="xx" -dst_url="xx" -db_name=ycsb -coll_name=test1 -sample_rate=0.1 -dry_run=true
./pre_split.linux -log_dir=./ -db_name="*" -coll_name="*"
```

### 入参说明

- `src_url`: 源端的mongodb url。
- `dst_url`: 目标端的mongodb url。
- `db_name`: 需要进行预分片的数据库名。
- `coll_name`: 需要进行预分片的集合名。
- `sample_rate`: 采样率，默认为0.1，即10%的数据进行采样。
- `dry_run`: 预分片的dry run，仅输出需要执行的命令，并不真正执行命令；默认false。

### 补充说明
要兼容多个大版本/小版本差异，比预期中复杂：
1. 4.4及以下的config.chunks元数据可以直接用namespace关联；5.0+的config.chunks元数据则只能用uuid关联；
2. 需要实现采样的逻辑，比如源端有1w个chunk，目标端预分片时并不需要那么多chunk，可能200个均匀地分布在目标端上的chunk就能有效避免“写入倾斜”的问题;
3. 6.0开始默认的chunk size已经从64MB调整到128MB。如果是从<6.0的内核版本迁移到>=6.0的版本，chunks数量本来就不需要跟源端保持一致；
4. 6.0开始无法对空range/chunk进行pre-split了，必须要手动执行moveChunk/moveRange; （由于6.0.3 balancer策略的变更）
5. 哈希分片无法使用sh.moveChunk方法，必须要使用带bounds选项的moveChunk命令；当然哈希分片一般都直接在shardCollection的时候进行pre-split了，compound hashed index则需要处理；
6. 还有mongodb-go-driver处理uuid类型的问题。 关联 https://jira.mongodb.org/browse/GODRIVER-2484
7. 以及针对minKey和maxKey的处理
8. compound hashed index的处理（下面的后2种）
```javascript
// 1)
{ "fieldA" : "hashed"}
// 2) 
{ "fieldA" : "hashed", "fieldB": 1}
// 3)
{ "fieldA" : 1, "fieldB" : "hashed", "fieldC": 1}
// 4) 
{ "fieldA" : 1, "fieldB" : 1, "fieldC" : "hashed" }
```
9. 如果是6.0.3以上的版本作为源端，还会有“原本chunk/range数就比较少”的问题
```javascript
{ "_id" : "ycsb.test11", "lastmodEpoch" : ObjectId("666bffc7c44b2ae3ec6a87d0"), "lastmod" : ISODate("2024-06-14T08:31:03.519Z"), "timestamp" : Timestamp(1718353863, 15), "uuid" : UUID("9df9cf80-f766-47b0-87be-f92ddcdfa43e"), "key" : { "_id" : 1 }, "unique" : false, "noBalance" : false }
```
这会导致如果源端的chunk数量偏少，split+moveChunk带来的收益并不大。（比如这个case里总共就4个chunks，就算预分片将这4个chunk均匀地分布到2个shard上，但实际上覆盖的范围也是不均的，还是依赖同步数据后的balancer自均衡）
10. 当然，如果源端是>7.0的版本，由于autoMerger的存在，会自动将相邻的hcunk进行合并。因此我们看到的可能就只有跟分片数相等的chunk。（下面的示例里是2个）

https://www.mongodb.com/docs/manual/core/automerger-concept/

这种情况下， 预分片也只能分为2个chunk。如果正好是[minKey，xxx]的chunk在一个shardA，[xxx,maxKey]的chunk在一个shardB上，则按照DTS全量阶段的“按_id排序逻辑”进行插入，依然会有数据倾斜的问题，只不过一开始流量倾斜到shardA，后面倾斜到shardB而已。
要处理这种场景，则需要侵入业务shard key逻辑，在shard key的取值范围内选择合适的split point，需要考虑的因素更多，ROI并不高。

### 【参考文档】
- https://www.mongodb.com/docs/manual/reference/glossary/#std-term-pre-splitting
- https://www.mongodb.com/docs/manual/tutorial/create-chunks-in-sharded-cluster/

cmd:
- https://www.mongodb.com/docs/manual/reference/method/sh.splitAt/
- https://www.mongodb.com/docs/manual/reference/method/sh.moveChunk/#mongodb-method-sh.moveChunk
- https://www.mongodb.com/docs/manual/reference/command/split/#mongodb-dbcommand-dbcmd.split
- https://www.mongodb.com/docs/manual/reference/command/moveChunk/#mongodb-dbcommand-dbcmd.moveChunk