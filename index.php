<?php

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

// 获取执行命令
if (isset($argv[1]) && in_array($argv[1], array('scan', 'write', 'compare_rename', 'revert', 'remove'))) {
    $action = $argv[1];
} else {
    echo '扫描 keys         php index.php scan', PHP_EOL;
    echo '写入数据库        php index.php write', PHP_EOL;
    echo '比较并重命名 key  php index.php compare_rename', PHP_EOL;
    echo '回滚重命名        php index.php revert', PHP_EOL;
    echo '删除 keys         php index.php remove', PHP_EOL;
    echo '使用帮助          php index.php help', PHP_EOL;
    exit;
}

// 初始化 Logger
Logger::init(__DIR__ . '/log');

// 读取配置
$config = include __DIR__ . '/config.php';

// 连接 Redis
$redis = new Redis();
$redis->connect($config['redis']['host'], $config['redis']['port']);
if ($config['redis']['password']) {
    $redis->auth($config['redis']['password']);
}

// 连接 MySQL
$mysqli = new mysqli($config['mysql']['host'], $config['mysql']['username'], $config['mysql']['password'], $config['mysql']['database'], $config['mysql']['port']);
$mysqli->set_charset('utf8');

// 创建实例
$redisBackup = new RedisBackup($redis, $mysqli, $config['redis']['node_id']);
try {
    $redisBackup->init();
} catch (RedisBackupException $e) {
    Logger::error($e->getMessage());
    echo $e->getMessage(), PHP_EOL;
    echo $e->getTraceAsString(), PHP_EOL;
    return;
}

// 不同操作
switch ($action) {
    case 'scan':
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
                echo $e->getMessage(), PHP_EOL;
                echo $e->getTraceAsString(), PHP_EOL;
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
            new KeyFileReader($config['backup']['scanned_keys_file']),
            new KeyFileWriter($config['backup']['written_keys_file'])
        );
        $writeErrorKeysWriter = new KeyFileWriter($config['backup']['write_error_keys_file']);

        $ignoreAll = false;
        while (!$writer->isFinished()) {
            try {
                $writer->run();
            } catch (RedisBackupException $e) {
                Logger::error($e->getMessage() . " Key: {$writer->currentKey}");
                $writeErrorKeysWriter->write($writer->currentKey);
                echo $e->getMessage(), PHP_EOL;
                echo $e->getTraceAsString(), PHP_EOL;
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
            new KeyFileReader($config['backup']['written_keys_file']),
            new KeyFileWriter($config['backup']['renamed_keys_file']),
            $config['backup']['rename_suffix']
        );
        $renamer->isDebug = $config['backup']['rename_debug'];
        $renameErrorKeysWriter = new KeyFileWriter($config['backup']['rename_error_keys_file']);

        $ignoreAll = false;
        while (!$renamer->isFinished()) {
            try {
                $renamer->run();
            } catch (RedisBackupException $e) {
                Logger::error($e->getMessage() . " Key: {$renamer->currentKey}");
                $renameErrorKeysWriter->write($renamer->currentKey);
                echo $e->getMessage(), PHP_EOL;
                echo $e->getTraceAsString(), PHP_EOL;
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
            new KeyFileReader($config['backup']['renamed_keys_file']),
            new KeyFileWriter($config['backup']['reverted_keys_file']),
            $config['backup']['rename_suffix']
        );
        $revertErrorKeysWriter = new KeyFileWriter($config['backup']['revert_error_keys_file']);

        $ignoreAll = false;
        while (!$reverter->isFinished()) {
            try {
                $reverter->run();
            } catch (RedisBackupException $e) {
                Logger::error($e->getMessage() . " Key: {$reverter->currentKey}");
                $revertErrorKeysWriter->write($reverter->currentKey);
                echo $e->getMessage(), PHP_EOL;
                echo $e->getTraceAsString(), PHP_EOL;
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
            new KeyFileReader($config['backup']['renamed_keys_file']),
            new KeyFileWriter($config['backup']['removed_keys_file'])
        );
        $removeErrorKeysWriter = new KeyFileWriter($config['backup']['remove_error_keys_file']);

        $ignoreAll = false;
        while (!$remover->isFinished()) {
            try {
                $remover->run();
            } catch (RedisBackupException $e) {
                Logger::error($e->getMessage() . " Key: {$remover->currentKey}");
                $writeErrorKeysWriter->write($remover->currentKey);
                echo $e->getMessage(), PHP_EOL;
                echo $e->getTraceAsString(), PHP_EOL;
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
