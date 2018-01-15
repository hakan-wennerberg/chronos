<?php

namespace Chronos;

use Carbon\Carbon;
use Chronos\KeyBucketEntry;
use PDO;

class KeyBucket
{
	use Error, Transaction;
	
	private $bucketName = '';
	private $bucketId = 0;
	private $connection = null;
	private $options = null;
	private $serverId = 0;
	private $shardName = '';
	private $shardId = 0;

	/**
	 * 
	 * @param PDO $connection
	 * @param array $options A reference to the Chronos options.
	 * @param int $serverId
	 * @param int $shardId
	 * @param int $bucketId
	 */
	public function __construct(\PDO $connection, &$options, $serverId, $shardId, $bucketId)
	{
		$this->bucketName = self::getBucketName($options, $bucketId);
		$this->bucketId = $bucketId;
		$this->connection = $connection;
		$this->options = $options;
		$this->serverId = $serverId;
		$this->shardName = $options['shardPrefix'] . $shardId . $options['shardSuffix'];
		$this->shardId = $shardId;
	}
	
	/**
	 * Delete an entry by key.
	 * 
	 * @param string $key
	 * @param boolean $useCache False to bypass cache (if available).
	 * @return boolean True if success and one entry was affected. False if failure or no entry was affected.
	 */
	public function delete($key, $useCache = true)
	{
		$cmd = $this->connection->prepare("
			DELETE FROM {$this->shardName}.{$this->bucketName}
			WHERE `key` = :key");
		$cmd->bindParam(':key', $key, PDO::PARAM_STR);
		$cmd->execute();
		
		if ($cmd->errorCode() === '00000' && $cmd->rowCount() === 1) {
			// Delete from cache.
			if ($useCache === true && $this->options['cache'] !== false) {
				$this->options['cache']->delete($this->getCacheMeta($key));
			}
			return true;
		}  elseif ($cmd->rowCount() === 0) {
			$this->pushError('C00001', 'No row affected.');
		} else {
			$this->pushError($cmd->errorCode(), $cmd->errorInfo());
		}
		return false;
	}
	
	/**
	 * Get an entry by key.
	 * 
	 * @param string $key
	 * @param boolean $useCache False to bypass cache (if available).
	 * @return boolean|KeyBucketEntry KeyBucketEntry on success, false on failure.
	 */
	public function get($key, $useCache = true)
	{
		// Get from cache.
		if ($useCache === true && $this->options['cache'] !== false) {
			if (($entry = $this->options['cache']->get($this->getCacheMeta($key))) !== false) {
				return $entry;
			}
		}
		
		$entry = new KeyBucketEntry();
		$entry->loading();
		
		$cmd = $this->connection->prepare(
				"SELECT created_at, `key`, payload, updated_at, version
				FROM {$this->shardName}.{$this->bucketName}
				WHERE `key` = :key");
		$cmd->bindParam(':key', $key, PDO::PARAM_STR);
		$cmd->execute();
		
		if ($cmd->errorCode() !== '00000') {
			$this->pushError($cmd->errorCode(), $cmd->errorInfo());
			return false;
		}
		
		$cmd->bindColumn(1, $entry->created_at, PDO::PARAM_STR);
		$cmd->bindColumn(2, $entry->key, PDO::PARAM_STR);
		$cmd->bindColumn(3, $entry->payload, PDO::PARAM_LOB);
		$cmd->bindColumn(4, $entry->updated_at, PDO::PARAM_STR);
		$cmd->bindColumn(5, $entry->version, PDO::PARAM_INT);
		$cmd->fetch(PDO::FETCH_BOUND);
		
		if ($cmd->rowCount() > 0) {
			$entry->created_at = new Carbon($entry->created_at);
			$entry->updated_at = new Carbon($entry->updated_at);
			$entry->loaded();
			return $entry;
		} else {
			return false;
		}
	}
	
	/**
	 * Meta data to send to the cache layer driver.
	 * 
	 * @param string $key
	 * @return array
	 */
	private function getCacheMeta($key)
	{
		return [
			'bucketId' => $this->bucketId,
			'bucketName' => $this->bucketName,
			'bucketType' => Chronos::BUCKET_TYPE_KEY,
			'key' => $key,
			'cacheKey' => "KB-{$this->shardId}-{$this->bucketId}-{$key}",
			'serverId' => $this->serverId,
			'shardId' => $this->shardId,
			'shardName' => $this->shardName
		];
	}
	
	/**
	 * 
	 * @param array $options Reference to the Chronos options.
	 * @param int $bucketId
	 * @return string DB-bucket name.
	 */
	public static function getBucketName(&$options, $bucketId)
	{
		return $options['keyBucketPrefix'] . $bucketId . $options['keyBucketSuffix'];
	}
	
	/**
	 * Insert or update an entry in the bucket.
	 * 
	 * @param KeyBucketEntry $entry Reference to the entry to insert or update.
	 * @param boolean $useCache False to bypass cache (if available).
	 * @return boolean|KeyBucketEntry KeyBucketEntry on success, false on failure.
	 */
	public function put(KeyBucketEntry &$entry, $useCache = true)
	{
		$entry->saving();
		
		$cmd = $this->connection->prepare("
			INSERT INTO {$this->shardName}.{$this->bucketName}
				(created_at, `key`, payload, updated_at, version) VALUES
				(:created_at, :key, :payload, :updated_at, :version)
			ON DUPLICATE KEY UPDATE 
				payload = :payload, updated_at = :updated_at, version = :version");
		
		$now = Carbon::now();
		if (is_null($entry->created_at)) {
			$entry->created_at = $now;
		}
		$entry->updated_at = $now;
		
		$cmd->bindParam(':created_at', $entry->created_at, PDO::PARAM_STR);
		$cmd->bindParam(':key', $entry->key, PDO::PARAM_STR);
		$cmd->bindParam(':payload', $entry->payload, PDO::PARAM_LOB);
		$cmd->bindParam(':updated_at', $entry->updated_at, PDO::PARAM_STR);
		$cmd->bindParam(':version', $entry->version, PDO::PARAM_INT);
		$cmd->execute();
		
		if ($cmd->errorCode() === '00000') {
			// Update cache.
			if ($useCache === true && $this->options['cache'] !== false) {
				$this->options['cache']->put($this->getCacheMeta($entry->key), $entry);
			}
			$entry->saved();
			return $entry;
		} else {
			$this->pushError($cmd->errorCode(), $cmd->errorInfo());
			return false;
		}
	}
	
	/**
	 * Scans the bucket for data. Slow and should be avoided.
	 * 
	 * @param int $offset Entry offset (starts at 0).
	 * @param int $count Number of entries to return.
	 * @param boolean $useCache False to bypass cache (if available).
	 * @return boolean|KeyBucketEntry[] Returns an array of KeyBucketEntry on success. False on failure.
	 */
	public function scan($offset, $count, $useCache = true)
	{
		$cmd = $this->connection->prepare(
			"SELECT created_at, `key`, payload, updated_at, version
			FROM {$this->shardName}.{$this->bucketName}
			LIMIT :offset, :count");
		
		$cmd->bindParam(':offset', $offset, PDO::PARAM_INT);
		$cmd->bindParam(':count', $count, PDO::PARAM_INT);
		$cmd->execute();
		
		if ($cmd->errorCode() !== '00000') {
			$this->pushError($cmd->errorCode(), $cmd->errorInfo());
			return false;
		}
		
		$cmd->bindColumn(1, $createdAt, PDO::PARAM_STR);
		$cmd->bindColumn(2, $key, PDO::PARAM_STR);
		$cmd->bindColumn(3, $payload, PDO::PARAM_LOB);
		$cmd->bindColumn(4, $updatedAt, PDO::PARAM_STR);
		$cmd->bindColumn(5, $version, PDO::PARAM_INT);
		
		$entries = [];
		
		while ($cmd->fetch()) {
			$entry = new KeyBucketEntry();
			$entry->created_at = new Carbon($createdAt);
			$entry->key = $key;
			$entry->payload = $payload;
			$entry->updated_at = new Carbon($updatedAt);
			$entry->version = $version;
			$entries[] = $entry;
			
			// Put into cache.
			if ($useCache === true && $this->options['cache'] !== false) {
				$this->options['cache']->put($this->getCacheMeta($entry->key), $entry);
			}
		}
		
		return $entries;
	}
}