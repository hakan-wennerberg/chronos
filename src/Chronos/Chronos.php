<?php

namespace Chronos;

use Chronos\KeyBucket;
use Chronos\TimelineBucket;
use Chronos\TimelineIndex;
use Chronos\UserBucket;

class Chronos
{
	use Error;
	
	/* Bucket type constants */
	const BUCKET_TYPE_KEY = 1;
	const BUCKET_TYPE_TIMELINE = 2;
	const BUCKET_TYPE_USER = 3;
	
	/* Index type constants */
	const INDEX_TYPE_TIMELINE = 1000;
	
	/** @var array Array of PDO connections indexed by server id. */
	private $connection = [];
	
	/** @var array Array of library options. */
	private $options = [];
	
	/** @var array Array of server ids indexed by shard id */
	private $serverHasShard = [];
	
	/** @var array Array of server info. */
	private $server = [];
	
	/**
	 * @param array $param Associative array with configuration options.
	 */
	public function __construct($param = [])
	{
		$this->options['cache'] = (isset($param['cache'])) ? $param['cache'] : false;
		
		// Validate that cache object implements ICache, else set it to false.
		if ($this->options['cache'] !== false 
				&& in_array('Chronos\ICache', class_implements($this->options['cache'])) === false) {
			$this->options['cache'] = false;
		}
		
		$this->options['keyBucketKeySize'] = (isset($param['keyBucketKeySize'])) ? $param['keyBucketKeySize'] : 30;
		$this->options['keyBucketPrefix'] = (isset($param['keyBucketPrefix'])) ? $param['keyBucketPrefix'] : 'kb_';
		$this->options['keyBucketSuffix'] = (isset($param['keyBucketSuffix'])) ? $param['keyBucketSuffix'] : '';
		$this->options['shardPrefix'] = (isset($param['shardPrefix'])) ? $param['shardPrefix'] : 'sha_';
		$this->options['shardSuffix'] = (isset($param['shardSuffix'])) ? $param['shardSuffix'] : '';
		$this->options['timelineBucketPrefix'] = (isset($param['timelineBucketPrefix'])) ? $param['timelineBucketPrefix'] : 'tb_';
		$this->options['timelineBucketSuffix'] = (isset($param['timelineBucketSuffix'])) ? $param['timelineBucketSuffix'] : '';
		$this->options['timelineIndexPrefix'] = (isset($param['timelineIndexPrefix'])) ? $param['timelineIndexPrefix'] : 'tbx_';
		$this->options['timelineIndexSuffix'] = (isset($param['timelineIndexSuffix'])) ? $param['timelineIndexSuffix'] : '';
		$this->options['userBucketPrefix'] = (isset($param['userBucketPrefix'])) ? $param['userBucketPrefix'] : 'ub_';
		$this->options['userBucketSuffix'] = (isset($param['userBucketSuffix'])) ? $param['userBucketSuffix'] : '';
	}

	/**
	 * Creates a new bucket of the specified type in the specified shard.
	 * 
	 * @param int $bucketType Any of the Chronos::BUCKET_TYPE_* constants.
	 * @param int $shardId Id of shard to create the bucket in.
	 * @param int $bucketId Id of the bucket to create.
	 * @return boolean|TimelineBucket|UserBucket|KeyBucket Object if success, false on failure.
	 */
	public function createBucket($bucketType, $shardId, $bucketId = 0)
	{
		switch ($bucketType) {
			case Chronos::BUCKET_TYPE_KEY:
				return $this->createKeyBucket($shardId, $bucketId);
			case Chronos::BUCKET_TYPE_TIMELINE:
				return $this->createTimelineBucket($shardId, $bucketId);
			case Chronos::BUCKET_TYPE_USER;
				return $this->createUserBucket($shardId);
			default:
				return false;
		}
	}
	
	/**
	 * Creates a new index of the specified type in the specified location.
	 * 
	 * @param int $indexType Any of the Chronos::INDEX_TYPE_* constants.
	 * @param int $shardId
	 * @param string $indexId
	 * @return boolean|TimelineIndex TimelineIndex on success, false on failure.
	 */ 
	public function createIndex($indexType, $shardId, $indexId)
	{
		// We currently only have one index type, ignore index param.
		return $this->createTimelineIndex($shardId, $indexId);
	}
	
	/**
	 * Creates a KeyBucket.
	 * 
	 * @param int $shardId
	 * @param int $bucketId
	 * @return boolean|KeyBucket KeyBucket on success, false on failure.
	 */
	private function createKeyBucket($shardId, $bucketId)
	{
		$pdo = $this->getConnection($this->serverHasShard[$shardId]);
		$shardName = $this->getShardName($shardId);
		$keyBucketName = KeyBucket::getBucketName($this->options, $bucketId);
		
		$cmd = $pdo->prepare("
			CREATE TABLE {$shardName}.{$keyBucketName} (
				created_at TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',
				`key` VARCHAR(40) NOT NULL,
				payload LONGTEXT NULL DEFAULT NULL,
				updated_at TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',
				version INT(10) unsigned NOT NULL DEFAULT '1',
				PRIMARY KEY (`key`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8");
		$cmd->execute();
		
		if ($cmd->errorCode() === '00000') {
			return new KeyBucket(
					$pdo, 
					$this->options, 
					$this->serverHasShard[$shardId],
					$shardId,
					$bucketId
				);
		} else {
			$this->pushError($cmd->errorCode(), $cmd->errorInfo());
			return false;
		}
	}
	
	/**
	 * Creates a TimelineBucket.
	 * 
	 * @param int $shardId
	 * @param int $bucketId
	 * @return boolean|TimelineBucket TimelineBucket on success, false on failure.
	 */
	private function createTimelineBucket($shardId, $bucketId)
	{
		$pdo = $this->getConnection($this->serverHasShard[$shardId]);
		$shardName = $this->getShardName($shardId);
		$timelineBucketName = TimelineBucket::getBucketName($this->options, $bucketId);
		
		$cmd = $pdo->prepare("
			CREATE TABLE {$shardName}.{$timelineBucketName} (
				created_at TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',
				id BIGINT(20) unsigned NOT NULL AUTO_INCREMENT,
				payload LONGTEXT NULL DEFAULT NULL,
				updated_at TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',
				version INT(10) unsigned NOT NULL DEFAULT '1',
				PRIMARY KEY (id)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8");
		$cmd->execute();

		if ($cmd->errorCode() === '00000') {
			return new TimelineBucket(
					$pdo, 
					$this->options, 
					$this->serverHasShard[$shardId],
					$shardId,
					$bucketId
				);
		} else {
			$this->pushError($cmd->errorCode(), $cmd->errorInfo());
			return false;
		}
	}
	
	/**
	 * Creates a TimelineIndex.
	 * 
	 * @param int $shardId
	 * @param int $indexId
	 * @return boolean|TimelineIndex TimelineIndex on success, false on failure.
	 */
	private function createTimelineIndex($shardId, $indexId)
	{
		$pdo = $this->getConnection($this->serverHasShard[$shardId]);
		$shardName = $this->getShardName($shardId);
		$timelineBucketIndexName = TimelineIndex::getIndexName($this->options, $indexId);
		
		$cmd = $pdo->prepare("
			CREATE TABLE {$shardName}.{$timelineBucketIndexName} (
				a BIGINT(20) UNSIGNED NOT NULL,
				b BIGINT(20) UNSIGNED NOT NULL,
				created_at TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',
				id INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
				updated_at TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',
				PRIMARY KEY (id, a)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8");
		$cmd->execute();
		
		if ($cmd->errorCode() === '00000') {
			return new TimelineIndex(
					$pdo, 
					$this->options, 
					$this->serverHasShard[$shardId],
					$shardId,
					$indexId
				);
		} else {
			$this->pushError($cmd->errorCode(), $cmd->errorInfo());
			return false;
		}
	}
	
	/**
	 * Creates a UserBucket.
	 * 
	 * @param int $shardId
	 * @return boolean|UserBucket UserBucket on success, false on failure.
	 */
	private function createUserBucket($shardId)
	{
		$pdo = $this->getConnection($this->serverHasShard[$shardId]);
		$shardName = $this->getShardName($shardId);
		$userBucketName = UserBucket::getBucketName($this->options);
		
		$cmd = $pdo->prepare("
			CREATE TABLE {$shardName}.{$userBucketName} (
				created_at TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',
				deleted_at TIMESTAMP NULL DEFAULT NULL,
				email VARCHAR(255) NOT NULL,
				id BIGINT(20) unsigned NOT NULL AUTO_INCREMENT,
				payload LONGTEXT NULL DEFAULT NULL,
				shard INT(10) unsigned NOT NULL,
				updated_at TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',
				version SMALLINT(5) unsigned NOT NULL DEFAULT '1',
				PRIMARY KEY (id),
				UNIQUE KEY idx_email (email)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8");
		$cmd->execute();
		
		if ($cmd->errorCode() === '00000') {
			return new UserBucket(
					$pdo, 
					$this->options, 
					$this->serverHasShard[$shardId],
					$shardId
				);
		} else {
			$this->pushError($cmd->errorCode(), $cmd->errorInfo());
			return false;
		}
	}
	
	/**
	 * Creates a new shard on the specified server.
	 * 
	 * @param int $serverId
	 * @param int $shardId
	 * @return boolean True on success, false on failure.
	 */
	public function createShard($serverId, $shardId)
	{
		// Create Shard.
		$pdo = $this->getConnection($serverId);
		$shardName = $this->getShardName($shardId);
		$cmd = $pdo->prepare("CREATE DATABASE {$shardName} DEFAULT CHARACTER SET utf8");
		$cmd->execute();
		
		if ($cmd->errorCode() === '00000') {
			// Register Shard.
			$this->server[$serverId]['shards'][] = $shardId;
			$this->serverHasShard[$shardId] = $serverId;
			return true;
		} else {
			$this->pushError($cmd->errorCode(), $cmd->errorInfo());
			return false;
		}
	}
	
	/**
	 * Drops a bucket of the specified type and location.
	 * 
	 * @param int $bucketType Any of the Chronos::BUCKET_TYPE_* constants.
	 * @param int $shardId
	 * @param int $bucketId
	 * @return boolean True on success, false on failure.
	 */
	public function dropBucket($bucketType, $shardId, $bucketId = 0)
	{
		switch ($bucketType) {
			case Chronos::BUCKET_TYPE_KEY:
				return $this->dropKeyBucket($shardId, $bucketId);
			case Chronos::BUCKET_TYPE_TIMELINE:
				return $this->dropTimelineBucket($shardId, $bucketId);
			case Chronos::BUCKET_TYPE_USER;
				return $this->dropUserBucket($shardId);
			default:
				return false;
		}
	}
	
	/**
	 * Drops an index of the specified type and location.
	 * 
	 * @param int $indexType Any of the Chronos::INDEX_TYPE_* constants.
	 * @param int $shardId
	 * @param string $indexId
	 */
	public function dropIndex($indexType, $shardId, $indexId)
	{
		// We only have one index type right now, ignore param.
		return $this->dropTimelineIndex($shardId, $indexId);
	}
	
	/**
	 * Drops a KeyBucket.
	 * 
	 * @param int $shardId
	 * @param int $bucketId
	 * @return boolean True on success, false on failure.
	 */
	private function dropKeyBucket($shardId, $bucketId)
	{
		$pdo = $this->getConnection($this->serverHasShard[$shardId]);
		$shardName = $this->getShardName($shardId);
		$keyBucketName = KeyBucket::getBucketName($this->options, $bucketId);
		
		$cmd = $pdo->prepare("DROP TABLE {$shardName}.{$keyBucketName}");
		$cmd->execute();
		
		if ($cmd->errorCode() === '00000') {
			return true;
		} else {
			$this->pushError($cmd->errorCode(), $cmd->errorInfo());
			return false;
		}
	}
	
	/**
	 * Drops a shard.
	 * 
	 * @param int $shardId
	 * @return boolean True on success, false on failure.
	 */
	public function dropShard($shardId)
	{
		// Drop Shard.
		$serverId = $this->serverHasShard[$shardId];
		$pdo = $this->getConnection($serverId);
		$shardName = $this->getShardName($shardId);
		$cmd = $pdo->prepare("DROP DATABASE {$shardName}");
		$cmd->execute();
		
		if ($cmd->errorCode() === '00000') {
			// Unregister Shard.
			unset($this->connection[$serverId]);
			unset($this->serverHasShard[$shardId]);
			unset($this->server[$serverId]['shards'][array_search($shardId, $this->server[$serverId]['shards'])]);
			return true;
		} else {
			$this->pushError($cmd->errorCode(), $cmd->errorInfo());
			return false;
		}
	}
	
	/**
	 * Drops a TimelineBucket.
	 * 
	 * @param int $shardId
	 * @param int $bucketId
	 * @return boolean True on success, false on failure.
	 */
	private function dropTimelineBucket($shardId, $bucketId)
	{
		$pdo = $this->getConnection($this->serverHasShard[$shardId]);
		$shardName = $this->getShardName($shardId);
		$timelineBucketName = TimelineBucket::getBucketName($this->options, $bucketId);
		
		$cmd = $pdo->prepare("DROP TABLE {$shardName}.{$timelineBucketName}");
		$cmd->execute();
		
		if ($cmd->errorCode() === '00000') {
			return true;
		} else {
			$this->pushError($cmd->errorCode(), $cmd->errorInfo());
			return false;
		}
	}
	
	/**
	 * Drops a TimelineIndex.
	 * 
	 * @param int $shardId
	 * @param int $indexId
	 * @return boolean True on success, false on failure.
	 */
	private function dropTimelineIndex($shardId, $indexId)
	{
		$pdo = $this->getConnection($this->serverHasShard[$shardId]);
		$shardName = $this->getShardName($shardId);
		$timelineIndexName = TimelineIndex::getIndexName($this->options, $indexId);
		
		$cmd = $pdo->prepare("DROP TABLE {$shardName}.{$timelineIndexName}");
		$cmd->execute();
		
		if ($cmd->errorCode() === '00000') {
			return true;
		} else {
			$this->pushError($cmd->errorCode(), $cmd->errorInfo());
			return false;
		}
	}
	
	/**
	 * Drops the user bucket on the specified shard.
	 * 
	 * @param int $shardId
	 * @return boolean True on success, false on failure.
	 */
	private function dropUserBucket($shardId)
	{
		$pdo = $this->getConnection($this->serverHasShard[$shardId]);
		$shardName = $this->getShardName($shardId);
		$userBucketName = UserBucket::getBucketName($this->options);
		
		$cmd = $pdo->prepare("DROP TABLE {$shardName}.{$userBucketName}");
		$cmd->execute();
		
		if ($cmd->errorCode() === '00000') {
			return true;
		} else {
			$this->pushError($cmd->errorCode(), $cmd->errorInfo());
			return false;
		}
	}
	
	/**
	 * Retrieves the PDO object for the specified server.
	 * 
	 * @param int $serverId
	 * @return \PDO
	 */
	private function getConnection($serverId)
	{
		if (isset($this->connection[$serverId]) === false) {
			$server = $this->server[$serverId];
			$this->connection[$serverId] = new \PDO(
				"{$server['driver']}:host={$server['host']};port={$server['port']};" .
				"dbname=", $server['username'], $server['password'],
				array(\PDO::ATTR_PERSISTENT => true));
		}
		
		return $this->connection[$serverId];
	}
	
	/**
	 * Gets a bucket of the specified type and location. This will return a 
	 * bucket object even if the bucket do not exist.
	 * 
	 * @param int $bucketType Any of the Chronos::BUCKET_TYPE_* constants.
	 * @param int $shardId
	 * @param int $bucketId
	 * @return boolean|TimelineBucket|UserBucket|KeyBucket Object if success, false on failure.
	 */
	public function getBucket($bucketType, $shardId, $bucketId = 0)
	{
		switch ($bucketType) {
			case Chronos::BUCKET_TYPE_KEY:
				return new KeyBucket(
						$this->getConnection($this->serverHasShard[$shardId]),
						$this->options,
						$this->serverHasShard[$shardId],
						$shardId,
						$bucketId
					);
			case Chronos::BUCKET_TYPE_TIMELINE:
				return new TimelineBucket(
						$this->getConnection($this->serverHasShard[$shardId]),
						$this->options,
						$this->serverHasShard[$shardId],
						$shardId,
						$bucketId
					);
			case Chronos::BUCKET_TYPE_USER;
				return new UserBucket(
						$this->getConnection($this->serverHasShard[$shardId]),
						$this->options,
						$this->serverHasShard[$shardId],
						$shardId
					);
			default:
				return false;
		}
	}

	/**
	 * Gets a index of the specified type and location. This will return a 
	 * index object even if the index do not exist.
	 * 
	 * @param int $indexType Any of the Chronos::INDEX_TYPE_* constants.
	 * @param int $shardId
	 * @param int $indexId
	 * @return boolean|TimelineIndex TimelineIndex if success, false on failure.
	 */
	public function getIndex($indexType, $shardId, $indexId)
	{
		// We only have one index type right now, ignore param.
		return new TimelineIndex(
				$this->getConnection($this->serverHasShard[$shardId]),
				$this->options,
				$this->serverHasShard[$shardId],
				$shardId,
				$indexId
			);
	}
	
	/**
	 * Get a Chronos option currently in use.
	 * 
	 * @param string $key Option name.
	 * @return mixed Option value.
	 */
	public function getOption($key)
	{
		if (isset($this->options[$key])) {
			return $this->options[$key];
		} else {
			return null;
		}
	}
	
	/**
	 * Return the DB-name of the shard.
	 * 
	 * @param int $shardId
	 * @return string DB-name of shard.
	 */
	private function getShardName($shardId)
	{
		return $this->options['shardPrefix'] . $shardId . $this->options['shardSuffix'];
	}
	
	/**
	 * Adds a new server to the Chronos servers list.
	 * 
	 * @param array $params Array of server parameters. Defaults will be used for missing parameters.
	 * @return \Chronos\Chronos Returns $this for chaining.
	 */
	public function registerServer($params)
	{
		$server = [];
		$server['pdoAttrs'] = (isset($params['pdoAttrs'])) ? $params['pdoAttrs'] : [];
		$server['driver'] = (isset($params['driver'])) ? $params['driver'] : 'mysql';
		$server['host'] = (isset($params['host'])) ? $params['host'] : '127.0.0.1';
		$server['id'] = (isset($params['id'])) ? $params['id'] : 0;
		$server['password'] = (isset($params['password'])) ? $params['password'] : '';
		$server['port'] = (isset($params['port'])) ? $params['port'] : 3306;
		$server['shards'] = (isset($params['shards'])) ? $params['shards'] : [];
		$server['username'] = (isset($params['username'])) ? $params['username'] : '';
		
		// Keep server info.
		$this->server[$server['id']] = $server;
		
		// Keep info on shard locations.
		foreach ($server['shards'] as $shardId) {
			$this->serverHasShard[$shardId] = $server['id'];
		}
		
		return $this;
	}
	
	/**
	 * Set a Chronos option.
	 * 
	 * @param string $key Option name.
	 * @param mixed $value Option value.
	 * @return \Chronos\Chronos Returns $this for chaining.
	 */
	public function setOption($key, $value)
	{
		$this->options[$key] = $value;
		return $this;
	}
}