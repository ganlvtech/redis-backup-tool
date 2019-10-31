<?php

date_default_timezone_set('Asia/Shanghai');

$config = include __DIR__ . '/../config.php';
$redis = new Redis();
$redis->connect($config['redis']['host'], $config['redis']['port']);
$redis->auth($config['redis']['password']);

// 内容过长
$redis->del('a');
$redis->hset('a', 'a', str_repeat('a', 65537));

// 不支持的 Unicode 字符
$redis->del('b');
$redis->hSet('b', 'a', "\xf6");

// 不正确的类型
$redis->del('c');
$redis->set('c', '123456');
