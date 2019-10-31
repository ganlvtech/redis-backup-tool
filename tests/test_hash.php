<?php

date_default_timezone_set('Asia/Shanghai');

// 读取配置
$config = include __DIR__ . '/../config.php';

// 连接 Redis
$redis = new Redis();
$redis->connect($config['redis']['host'], $config['redis']['port']);
if ($config['redis']['password']) {
    $redis->auth($config['redis']['password']);
}

$key_count = mt_rand(100, 200);
for ($i = 0; $i < $key_count; $i++) {
    $key = 'redis-backup-hash-test:' . generateRandomString(mt_rand(5, 30));
    $field_count = mt_rand(1, 10);
    for ($j = 0; $j < $field_count; $j++) {
        $field = generateRandomString(mt_rand(1, 10));
        $value = generateRandomString(mt_rand(1, 512));
        $redis->hSet($key, $field, $value);
    }
}

/**
 * @param int $length
 *
 * @return string
 *
 * @link https://stackoverflow.com/questions/4356289/php-random-string-generator/4356295#4356295
 */
function generateRandomString($length = 10)
{
    $characters = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_-:|.';
    $charactersLength = strlen($characters);
    $randomString = '';
    for ($i = 0; $i < $length; $i++) {
        $randomString .= $characters[rand(0, $charactersLength - 1)];
    }
    return $randomString;
}
