# 将 Redis 数据转移到 MySQL 数据库的工具

备份 Redis 的 4 个过程 scan -> write -> compare & rename -> remove

1. 扫描匹配指定 pattern 的 Keys 保存的一个文本文件中，每行一个 Key
2. 读取这个文本文件的每一行，然后从 Redis 读取，写入数据库
3. 读取这个文本文件的每一行，读取数据库，再读 Redis，比较是否相等
   如果相等则计划执行 rename 操作，把 rename 后的 key 写另外一个文本文件
   如果不相等报错停止执行，提示 key 名称和不相等的值，二者的 json_encode，
4. 打开客户端测试此备份操作是否影响使用
5. 读取 rename 后的 key 文本文件，执行 del 操作

## 使用方法

### 创建 autoload

```bash
composer dump-autoload
```

### 编辑配置文件

```bash
vim config.php
```

### 创建 MySQL 表

```bash
php index.php create_table
```

### 选择 Redis 集群节点

先获取节点信息

```bash
php index.php cluster_info
```

然后在 Node ID 列表选择一个，填入 `config.php`

### 执行

```bash
php index.php scan
php filter.php
php index.php write
php index.php compare_rename
```

其中 `filter.php` 不是必须存在的，`filter.php` 用于对已扫描到的 keys 进行一次过滤，这个文件可以手动修改，修改其中的匹配模式，以达到更复杂的过滤功能。

测试是否对服务器造成影响，如果服务器受到影响，则需执行如下脚本，恢复重命名的 key

```bash
php index.php revert
```

如果运行一段时间之后没有产生影响，则可以执行如下脚本，删除重命名的 key

```bash
php index.php remove
```

## 配置文件说明

* `$config['backup']['scan_append_keys']` 如果设为 `true` 则不会备份 scanned_keys_file，而是向 scanned_keys_file 中继续追加新扫描到的 keys，您可以通过多次执行 `php index.php scan` 来继续上次扫描

* `$config['backup']['rename_debug']` 如果设为 `true` 则在执行 `php index.php compare_rename` 时不会真的执行 Redis 的 `RENAME` 指令，而是把执行的指令打印出来，相当于仅执行 compare，测试一下数据是否完整。

## 关于批处理

每个操作均可使用批处理加速，使用批处理的可能比单条处理快 10 倍（时间主要浪费在等待网络回应和单行写入日志），通常在 300 ~ 500 比较合适，再大效果不明显。程序会在出错时将整个一批的 key 写入 `error_keys.txt`，但是这一批 keys 并不一定是全失败了，由于是批处理所以不容易得知哪些成功了。因此，如果你的数据中出错的概率较大，则应该将批处理的行数减小。

## Benchmark

腾讯云内网，从 Redis 4.0 社区版集群的其中一个节点，向高可用版 2 核 4GB 内存 MySQL 转移。

单节点 20GB 内存，已使用约 18GB，40M 个 Key。批量扫描 5M 个 key，得到 265k 个 key，过滤得到 245k 个 key，总共需要 73.97 秒。

其中过滤是自己写的一个操作，使用 Redis Pipeline 批量执行，每个 Key 执行 1 次 HGET 操作。

写入 MySQL 中 Key 的平均长度为 22 字节，Value 的平均长度为 116 字节。

花费时间如下

| 过程| 总处理 Key 数 | 一批 Key 数 | 总执行时间 |
| :--- | ---: | ---: | ---: |
| 扫描 | 5M | 10k | 8.06 秒 |
| 过滤 | 265k | 1k | 5.83 秒 |
| 写入 | 245k | 500 | 21.58 秒 |
| 比较 | 245k | 500 | 33.34 秒 |
| 删除 | 245k | 500 | 5.16 秒 |

其中大部分时间应该是花费在网络传输和日志写入上，Redis 单节点 CPU 在执行操作时轻松达到 100%，MySQL 约 40%（可能我的操作只跑在 CPU 其中一个核的上）。

总结一下：

* 扫描：约 620k/s
* 写入、比较、删除加在一起：4k/s

## 已知问题

* 仅支持 `string` 和 `hash` 类型。
* 业务层应该在读取 Redis 不存在时再去读取数据库，这部分代码需要手工修改，并且可能造成原来的代码比较难看。
* 提示：读取数据库不存在时也要在 Redis 中设置一个空字符串的值，防止频繁读取数据库。
