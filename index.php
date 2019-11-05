<?php

use RedisBackup\Exception\RedisBackupClusterNotSelectNodeException;
use RedisBackup\Exception\RedisBackupException;
use RedisBackup\KeyFileReader;
use RedisBackup\KeyFileWriter;
use RedisBackup\RedisBackup;
use RedisBackup\Util\FileStorage;
use RedisBackup\Util\Logger;

// 只允许 cli 运行
if (php_sapi_name() !== 'cli') {
    return;
}

// 加载 Autoloader
require __DIR__ . '/vendor/autoload.php';

// 设置时间地区
date_default_timezone_set('Asia/Shanghai');

// 读取配置
$config = include __DIR__ . '/config.php';

// 获取执行命令
if (isset($argv[1]) && in_array($argv[1], array('create_table', 'cluster_info', 'scan', 'write', 'compare_rename', 'revert', 'remove'))) {
    $action = $argv[1];
} else {
    echo '配置文件：', PHP_EOL;
    echo '  Redis:', PHP_EOL;
    echo '    Host: ', $config['redis']['host'], PHP_EOL;
    echo '    Port: ', $config['redis']['port'], PHP_EOL;
    echo '    Node ID: ', $config['redis']['node_id'], PHP_EOL;
    echo '  MySQL:', PHP_EOL;
    echo '    Host: ', $config['mysql']['host'], PHP_EOL;
    echo '    Port: ', $config['mysql']['port'], PHP_EOL;
    echo '    Username: ', $config['mysql']['username'], PHP_EOL;
    echo '    Database: ', $config['mysql']['database'], PHP_EOL;
    echo '  备份:', PHP_EOL;
    echo '    扫描匹配模式: ', $config['backup']['pattern'], PHP_EOL;
    echo '    SCAN 初始指针: ', $config['backup']['start_pointer'], PHP_EOL;
    echo '    连续扫描: ', $config['backup']['scan_append_keys'] ? '继续向上一次扫描的 scan_pointer_file 中写入扫描到的 keys' : '每次扫描备份 scan_pointer_file 然后向一个新的 scan_pointer_file 中写入扫描到的 keys', PHP_EOL;
    echo '    扫描 keys 数量: 一共扫描 ', $config['backup']['scan_count'], ' 个，每次获取 ', $config['backup']['scan_batch'], ' 个', PHP_EOL;
    echo '    扫描 key 类型: ', $config['backup']['type'], PHP_EOL;
    echo '    表名: ', $config['backup']['table_name'], PHP_EOL;
    echo '    重命名后缀: ', $config['backup']['rename_suffix'], PHP_EOL;
    echo '    重命名 Debug: ', $config['backup']['rename_debug'] ? '仅打印 RENAME 指令及参数，不执行 RENAME' : '否', PHP_EOL;
    echo '  记录文件:', PHP_EOL;
    echo '    扫描指针: ', $config['backup']['scan_pointer_file'], PHP_EOL;
    echo '    扫描到的 keys: ', $config['backup']['scanned_keys_file'], PHP_EOL;
    echo '    已写入的 keys: ', $config['backup']['written_keys_file'], PHP_EOL;
    echo '    写入错误的 keys: ', $config['backup']['write_error_keys_file'], PHP_EOL;
    echo '    已重命名的 keys: ', $config['backup']['renamed_keys_file'], PHP_EOL;
    echo '    重命名出错的 keys: ', $config['backup']['rename_error_keys_file'], PHP_EOL;
    echo '    已回滚的 keys: ', $config['backup']['reverted_keys_file'], PHP_EOL;
    echo '    回滚错误的 keys: ', $config['backup']['revert_error_keys_file'], PHP_EOL;
    echo '    已删除的 keys: ', $config['backup']['removed_keys_file'], PHP_EOL;
    echo '    删除失败的 keys: ', $config['backup']['remove_error_keys_file'], PHP_EOL;
    echo PHP_EOL;
    echo '命令：', PHP_EOL;
    echo '  编辑配置文件      vim config.php', PHP_EOL;
    echo '  创建表            php index.php create_table', PHP_EOL;
    echo '  Redis 集群信息    php index.php cluster_info', PHP_EOL;
    echo '  扫描 keys         php index.php scan', PHP_EOL;
    echo '  写入数据库        php index.php write', PHP_EOL;
    echo '  比较并重命名 key  php index.php compare_rename', PHP_EOL;
    echo '  回滚重命名        php index.php revert', PHP_EOL;
    echo '  删除 keys         php index.php remove', PHP_EOL;
    echo '  使用帮助          php index.php help', PHP_EOL;
    exit;
}

// 初始化 Logger
Logger::init(__DIR__ . '/log');

// 连接 Redis
$redis = new Redis();
$redis->connect($config['redis']['host'], $config['redis']['port']);
if ($config['redis']['password']) {
    $redis->auth($config['redis']['password']);
}

// 连接 MySQL
$mysqli = new mysqli($config['mysql']['host'], $config['mysql']['username'], $config['mysql']['password'], $config['mysql']['database'], $config['mysql']['port']);
$mysqli->set_charset('utf8');

// 创建表指令
if ($action === 'create_table') {
    Logger::info("创建 MySQL 表. 类型: {$config['backup']['type']}. 表名: {$config['backup']['table_name']}");
    switch ($config['backup']['type']) {
        case 'string':
            $sql = "CREATE TABLE `{$config['backup']['table_name']}` (
	`k` CHAR(191) NOT NULL,
	`v` VARCHAR(4096) NOT NULL,
	`created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	`updated_at` TIMESTAMP NULL DEFAULT NULL,
	PRIMARY KEY (`k`)
)
COLLATE='utf8mb4_unicode_ci'
ENGINE=InnoDB
;";
            $mysql_result = $mysqli->query($sql);
            if (!$mysql_result) {
                Logger::error("创建 MySQL 表失败: {$mysqli->error}\n{$sql}");
            } else {
                Logger::info("创建 MySQL 表成功: {$config['backup']['table_name']}");
            }
            break;
        case 'hash':
            $sql = "CREATE TABLE `{$config['backup']['table_name']}` (
	`k` CHAR(191) NOT NULL,
	`v` TEXT NOT NULL,
	`created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	`updated_at` TIMESTAMP NULL DEFAULT NULL,
	PRIMARY KEY (`k`)
)
COLLATE='utf8mb4_unicode_ci'
ENGINE=InnoDB
;
";
            $mysql_result = $mysqli->query($sql);
            if (!$mysql_result) {
                Logger::error("创建 MySQL 表失败: {$mysqli->error}\n{$sql}");
            } else {
                Logger::info("创建 MySQL 表成功: {$config['backup']['table_name']}");
            }
            break;
        default:
            Logger::error("不支持类型: {$config['backup']['type']}");
    }
    return;
}

// 创建实例
$redisBackup = new RedisBackup($redis, $mysqli, $config['redis']['node_id']);

// Redis 集群信息
if ($action === 'cluster_info') {
    echo $redisBackup->getClusterNodesInfo(), PHP_EOL;
    return;
}

// 不同操作
switch ($action) {
    case 'scan':
        // 检查是否是集群
        try {
            $redisBackup->checkRedisCluster();
        } catch (RedisBackupClusterNotSelectNodeException $e) {
            Logger::error($e->getMessage());
            Logger::error($e->getTraceAsString());
            return;
        }

        $scanner = $redisBackup->scanner(
            $config['backup']['pattern'],
            $config['backup']['type'],
            $config['backup']['start_pointer'],
            new KeyFileWriter($config['backup']['scanned_keys_file'], $config['backup']['scan_append_keys']),
            new FileStorage($config['backup']['scan_pointer_file'])
        );
        $scanner->setTargetScanKeyCount($config['backup']['scan_count']);
        if ($scanner->isFinished()) {
            Logger::info("已经扫描完成一趟，如果真的需要重新扫描一次，您可以手动删除 {$config['backup']['scan_pointer_file']}，也可以输入 y");
            if (confirm('是否重新扫描？[y/N] ')) {
                $scanner->scannedRound = 0;
            }
        }

        $ignoreAll = false;
        while (!$scanner->isFinished()) {
            try {
                $scanner->run($config['backup']['scan_batch']);
            } catch (RedisBackupException $e) {
                Logger::error($e->getMessage());
                Logger::error($e->getTraceAsString());
                if (!$ignoreAll) {
                    $confirm_result = confirm('是否继续？[y/N/a] ');
                    if ($confirm_result === 'a') {
                        $ignoreAll = true;
                    } elseif (!$confirm_result) {
                        break;
                    }
                }
            }
        }
        break;
    case 'write':
        $writer = $redisBackup->writer(
            $config['backup']['type'],
            new KeyFileReader($config['backup']['write_read_keys_file']),
            new KeyFileWriter($config['backup']['written_keys_file']),
            $config['backup']['table_name']
        );
        $writeErrorKeysWriter = new KeyFileWriter($config['backup']['write_error_keys_file']);

        $ignoreAll = false;
        while (!$writer->isFinished()) {
            try {
                $writer->run($config['backup']['write_batch']);
            } catch (RedisBackupException $e) {
                Logger::error($e->getMessage() . " 当前行: {$writer->keyFileReader->getLineNumber()} 当前 Key: {$writer->currentKey}");
                Logger::error($e->getTraceAsString());
                $writeErrorKeysWriter->writeMultiple($writer->queuedKeys);
                $writer->clearQueuedValues();
                $writer->clearQueuedKeys();
                if (!$ignoreAll) {
                    $confirm_result = confirm('是否继续？[y/N/a] ');
                    if ($confirm_result === 'a') {
                        $ignoreAll = true;
                    } elseif (!$confirm_result) {
                        break;
                    }
                }
            }
        }
        break;
    case 'compare_rename':
        $renamer = $redisBackup->renamer(
            $config['backup']['type'],
            new KeyFileReader($config['backup']['rename_read_keys_file']),
            new KeyFileWriter($config['backup']['renamed_keys_file']),
            $config['backup']['rename_suffix'],
            $config['backup']['table_name']
        );
        $renamer->isDebug = $config['backup']['rename_debug'];
        $renameErrorKeysWriter = new KeyFileWriter($config['backup']['rename_error_keys_file']);

        $ignoreAll = false;
        while (!$renamer->isFinished()) {
            try {
                $renamer->run($config['backup']['rename_batch']);
            } catch (RedisBackupException $e) {
                Logger::error($e->getMessage() . " 当前行: {$renamer->keyFileReader->getLineNumber()} 当前 Key: {$renamer->currentKey}");
                Logger::error($e->getTraceAsString());
                $renameErrorKeysWriter->writeMultiple($renamer->queuedKeys);
                $renamer->clearQueuedValues();
                $renamer->clearQueuedKeys();
                if (!$ignoreAll) {
                    $confirm_result = confirm('是否继续？[y/N/a] ');
                    if ($confirm_result === 'a') {
                        $ignoreAll = true;
                    } elseif (!$confirm_result) {
                        break;
                    }
                }
            }
        }
        break;
    case 'revert':
        $reverter = $redisBackup->reverter(
            new KeyFileReader($config['backup']['revert_read_keys_file']),
            new KeyFileWriter($config['backup']['reverted_keys_file']),
            $config['backup']['rename_suffix']
        );
        $revertErrorKeysWriter = new KeyFileWriter($config['backup']['revert_error_keys_file']);

        $ignoreAll = false;
        while (!$reverter->isFinished()) {
            try {
                $reverter->run($config['backup']['revert_batch']);
            } catch (RedisBackupException $e) {
                Logger::error($e->getMessage() . " 当前行: {$reverter->keyFileReader->getLineNumber()} 当前 Key: {$reverter->currentKey}");
                Logger::error($e->getTraceAsString());
                $revertErrorKeysWriter->writeMultiple($reverter->queuedKeys);
                $reverter->clearQueuedKeys();
                if (!$ignoreAll) {
                    $confirm_result = confirm('是否继续？[y/N/a] ');
                    if ($confirm_result === 'a') {
                        $ignoreAll = true;
                    } elseif (!$confirm_result) {
                        break;
                    }
                }
            }
        }
        break;
    case 'remove':
        $remover = $redisBackup->remover(
            new KeyFileReader($config['backup']['remove_read_keys_file']),
            new KeyFileWriter($config['backup']['removed_keys_file'])
        );
        $removeErrorKeysWriter = new KeyFileWriter($config['backup']['remove_error_keys_file']);

        $ignoreAll = false;
        while (!$remover->isFinished()) {
            try {
                $remover->run($config['backup']['remove_batch']);
            } catch (RedisBackupException $e) {
                Logger::error($e->getMessage() . " 当前行: {$remover->keyFileReader->getLineNumber()} 当前 Key: {$remover->currentKey}");
                Logger::error($e->getTraceAsString());
                $removeErrorKeysWriter->writeMultiple($remover->queuedKeys);
                $remover->clearQueuedKeys();
                if (!$ignoreAll) {
                    $confirm_result = confirm('是否继续？[y/N/a] ');
                    if ($confirm_result === 'a') {
                        $ignoreAll = true;
                    } elseif (!$confirm_result) {
                        break;
                    }
                }
            }
        }
        break;
}

function confirm($prompt)
{
    echo $prompt;
    $handle = fopen('php://stdin', 'r');
    $line = fgets($handle);
    $line = trim($line);
    $line = strtolower($line);
    if ($line === 'yes' || $line === 'y') {
        return true;
    }
    if ($line === 'all' || $line === 'a') {
        return 'a';
    }
    return false;
}
