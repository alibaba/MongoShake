# standalone 时间戳节点回归

`TestGetAllTimestampStandaloneMatchesReaderLive` 使用独立的本机副本集：一个
primary 和一个 hidden secondary。先写入数据并等待 hidden 追平，再暂停 hidden
应用复制日志，最后推进 primary，使两端 oplog 窗口不同。hidden 节点必须配置为
`hidden: true, priority: 0, votes: 0`，两个节点均启用 `enableTestCommands=1`。

以下命令需要本机安装 `mongod`、`mongosh` 和项目要求的 Go 工具链。
只在专用测试环境使用；27050、27051 必须空闲，不要改成生产地址。
在同一个 Bash 会话中执行，数据和日志保留在新建的临时目录中。

```bash
set -e
fixture_dir=$(mktemp -d "${TMPDIR:-/tmp}/mongoshake-timestamp.XXXXXX")
mkdir "$fixture_dir/primary" "$fixture_dir/hidden"
cleanup_timestamp_fixture() {
  for node in primary hidden; do
    if [ -f "$fixture_dir/$node.pid" ]; then
      kill -TERM "$(cat "$fixture_dir/$node.pid")" 2>/dev/null || true
    fi
  done
}
trap cleanup_timestamp_fixture EXIT

mongod --bind_ip 127.0.0.1 --port 27050 --replSet timestamp_fixture \
  --dbpath "$fixture_dir/primary" --oplogSize 128 \
  --setParameter enableTestCommands=1 --fork \
  --logpath "$fixture_dir/primary.log" --pidfilepath "$fixture_dir/primary.pid"
mongod --bind_ip 127.0.0.1 --port 27051 --replSet timestamp_fixture \
  --dbpath "$fixture_dir/hidden" --oplogSize 128 \
  --setParameter enableTestCommands=1 --fork \
  --logpath "$fixture_dir/hidden.log" --pidfilepath "$fixture_dir/hidden.pid"

mongosh 'mongodb://127.0.0.1:27050/?directConnection=true' --quiet <<'JS'
assert.commandWorked(rs.initiate({_id: "timestamp_fixture", members: [
  {_id: 0, host: "127.0.0.1:27050"},
  {_id: 1, host: "127.0.0.1:27051", hidden: true, priority: 0, votes: 0}
]}));
for (let i = 0; i < 150 && !db.hello().isWritablePrimary; i++) sleep(200);
assert(db.hello().isWritablePrimary, "primary election timed out");
db.getSiblingDB("timestamp_fixture").seed.insertMany(
  Array.from({length: 100}, (_, i) => ({_id: i, payload: "seed"})));
JS

mongosh 'mongodb://127.0.0.1:27051/?directConnection=true&readPreference=secondary' --quiet <<'JS'
const seed = db.getSiblingDB("timestamp_fixture").seed;
for (let i = 0; i < 150 && seed.countDocuments({}) !== 100; i++) sleep(200);
assert.eq(100, seed.countDocuments({}), "hidden did not catch up");
assert.commandWorked(db.adminCommand({configureFailPoint: "rsSyncApplyStop", mode: "alwaysOn"}));
JS

mongosh 'mongodb://127.0.0.1:27050/?directConnection=true' --quiet --eval \
  'db.getSiblingDB("timestamp_fixture").seed.insertOne({_id: 100, payload: "primary-only"})'

# 在仓库根目录执行。两种 URI 各覆盖 directConnection 未设置 / false / true。
for uri in 'mongodb://127.0.0.1:27051' 'mongodb://127.0.0.1:27051/'; do
  MONGOSHAKE_TIMESTAMP_HIDDEN_SOURCE="$uri" \
    go test -race ./collector/reader \
    -run '^TestGetAllTimestampStandaloneMatchesReaderLive$' -count=3 -v -timeout=5m
done

cleanup_timestamp_fixture
trap - EXIT
echo "测试数据和日志保留于 $fixture_dir"
```

每轮应执行三个子测试，并记录六条时间戳查询日志。断言包括时间戳边界、
实际增量游标返回的完整 BSON、实际节点地址和连接模式。缺少环境变量会跳过，
不算通过。未带 `/` 的入口用于防止追加 query 时产生非法 MongoDB URI。

关闭测试进程会解除该专用 hidden 节点上的暂停状态；不要把暂停复制的实例
留给其他测试。测试本身不负责启动或关闭这些进程。

两个日志测试结束时会刷新临时日志并切回控制台输出，再删除临时目录。
这不会恢复此前的日志配置；需要特定日志配置的后续测试应自行初始化。
