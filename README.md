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

### 创建 MySQL 表

```bash
mysql -h 127.0.0.1 -P 3306 -uroot -p -D database
Enter your password:
mysql> source /path/to/redis-backup/migrations/20191010_162235_create_redis_backup_table.sql
```

### 执行

```bash
php index.php scan
php index.php write
php index.php compare_rename
```

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

## 已知问题

* 仅支持 `string` 和 `hash` 类型。
* 业务层应该在读取 Redis 不存在时再去读取数据库，这部分代码需要手工修改，并且可能造成原来的代码比较难看。
