<?php

use RedisBackup\Exception\RedisBackupException;
use RedisBackup\KeyFileReader;
use RedisBackup\KeyFileWriter;
use RedisBackup\KeyFilter;
use RedisBackup\Util\Logger;

// 只允许 cli 运行
if (php_sapi_name() !== 'cli') {
    return;
}

// 加载 Autoloader
require __DIR__ . '/vendor/autoload.php';

// 设置时间地区
date_default_timezone_set('Asia/Shanghai');

// 配置
$filtered_read_keys_file = __DIR__ . '/data/scanned_keys.txt';
$filtered_keys_file = __DIR__ . '/data/filtered_keys.txt';
$filter_error_keys_file = __DIR__ . '/data/filter_error_keys.txt';
$filter_batch = 1000;

// 初始化 Logger
Logger::init(__DIR__ . '/log');

class FilterExample extends KeyFilter
{
    public $pattern;

    /**
     * @param \RedisBackup\KeyFileReader $keyFileReader
     * @param \RedisBackup\KeyFileWriter $keyFileWriter
     * @param string $pattern
     */
    public function __construct($keyFileReader, $keyFileWriter, $pattern)
    {
        parent::__construct($keyFileReader, $keyFileWriter);
        $this->pattern = $pattern;
    }

    public function filterQueuedKeys()
    {
        foreach ($this->queuedKeys as $key) {
            if (1 === preg_match($this->pattern, $key)) {
                $this->queuedFilteredKeys($key);
            }
        }
    }
}

// 创建过滤器
$filter = new FilterExample(
    new KeyFileReader($config['backup']['scanned_keys_file']),
    new KeyFileWriter($filtered_keys_file),
    '/redis-backup-hash-test:.{20}/'
);
$filterErrorKeyWriter = new KeyFileWriter($filter_error_keys_file);

// 主循环
$ignoreAll = false;
while (!$filter->isFinished()) {
    try {
        $filter->run($filter_batch);
    } catch (RedisBackupException $e) {
        Logger::error($e->getMessage() . " 当前行: {$filter->keyFileReader->getLineNumber()} 当前 Key: {$filter->currentKey}");
        Logger::error($e->getTraceAsString());
        $filterErrorKeyWriter->writeMultiple($filter->queuedKeys);
        $filter->clearQueuedFilteredKeys();
        $filter->clearQueuedKeys();
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
