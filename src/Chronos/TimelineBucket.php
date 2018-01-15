<?php

namespace Chronos;

use Carbon\Carbon;
use Chronos\TimelineBucketEntry;
use PDO;

class TimelineBucket
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
	 * Delete an entry by id.
	 * 
	 * @param int $id
	 * @param boolean $useCache False to bypass cache (if available).
	 * @return boolean True if success and one entry was affected. False if failure or no entry was affected.
	 */
	public function delete($id, $useCache = true)
	{
		$cmd = $this->connection->prepare("
			DELETE FROM {$this->shardName}.{$this->bucketName}
			WHERE id = :id");
		$cmd->bindParam(':id', $id, PDO::PARAM_INT);
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
	 * Get an entry by id.
	 * 
	 * @param int $id
	 * @param boolean $useCache False to bypass cache (if available).
	 * @return boolean|TimelineBucketEntry KeyBucketEntry on success, false on failure.
	 */
	public function get($id, $useCache = true)
	{
		// Get from cache.
		if ($useCache === true && $this->options['cache'] !== false) {
			if (($entry = $this->options['cache']->get($this->getCacheMeta($id))) !== false) {
				return $entry;
			}
		}
		
		$entry = new TimelineBucketEntry();
		$entry->loading();
		
		$cmd = $this->connection->prepare(
				"SELECT created_at, id, payload, updated_at, version
				FROM {$this->shardName}.{$this->bucketName}
				WHERE id = :id");
		$cmd->bindParam(':id', $id, PDO::PARAM_INT);
		$cmd->execute();
		
		if ($cmd->errorCode() !== '00000') {
			$this->pushError($cmd->errorCode(), $cmd->errorInfo());
			return false;
		}
		
		$cmd->bindColumn(1, $entry->created_at, PDO::PARAM_STR);
		$cmd->bindColumn(2, $entry->id, PDO::PARAM_INT);
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
	 * @param string $id
	 * @return array
	 */
	private function getCacheMeta($id)
	{
		return [
			'bucketId' => $this->bucketId,
			'bucketName' => $this->bucketName,
			'bucketType' => Chronos::BUCKET_TYPE_TIMELINE,
			'cacheKey' => "TB-{$this->shardId}-{$this->bucketId}-{$id}",
			'id' => $id,
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
		return $options['timelineBucketPrefix'] . $bucketId . $options['timelineBucketSuffix'];
	}
	
	/**
	 * Insert or update an entry in the bucket.
	 * 
	 * @param TimelineBucketEntry $entry Reference to the entry to insert or update.
	 * @param boolean $useCache False to bypass cache (if available).
	 * @return boolean|TimelineBucketEntry TimelineBucketEntry on success, false on failure.
	 */
	public function put(TimelineBucketEntry &$entry, $useCache = true)
	{
		$entry->saving();
		
		if ($entry->id === 0) {
			$cmd = $this->connection->prepare("
				INSERT INTO {$this->shardName}.{$this->bucketName}
					(created_at, payload, updated_at, version) VALUES
					(:created_at, :payload, :updated_at, :version)");
		} else {
			$cmd = $this->connection->prepare("
				INSERT INTO {$this->shardName}.{$this->bucketName}
					(created_at, id, payload, updated_at, version) VALUES
					(:created_at, :id, :payload, :updated_at, :version)
				ON DUPLICATE KEY UPDATE 
					payload = :payload, updated_at = :updated_at, version = :version");
		}
		
		$now = Carbon::now();
		if (is_null($entry->created_at)) {
			$entry->created_at = $now;
		}
		$entry->updated_at = $now;
		
		$cmd->bindParam(':created_at', $entry->created_at, PDO::PARAM_STR);
		if ($entry->id !== 0) {
			$cmd->bindParam(':id', $entry->id, PDO::PARAM_INT);
		}
		$cmd->bindParam(':payload', $entry->payload, PDO::PARAM_LOB);
		$cmd->bindParam(':updated_at', $entry->updated_at, PDO::PARAM_STR);
		$cmd->bindParam(':version', $entry->version, PDO::PARAM_INT);
		$cmd->execute();
		
		if ($cmd->errorCode() === '00000') {
			if ($entry->id === 0) {
				$entry->id = $this->connection->lastInsertId();
			}
			// Update cache.
			if ($useCache === true && $this->options['cache'] !== false) {
				$this->options['cache']->put($this->getCacheMeta($entry->id), $entry);
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
	 * @param boolean $reverse Get newest first.
	 * @param boolean $useCache False to bypass cache (if available).
	 * @return boolean|TimelineBucketEntry[] Returns an array of TimelineBucketEntry on success. False on failure.
	 */
	public function scan($offset, $count, $reverse = false, $useCache = true)
	{
		if ($reverse === true) {
			$cmd = $this->connection->prepare(
				"SELECT created_at, id, payload, updated_at, version
				FROM {$this->shardName}.{$this->bucketName}
				ORDER BY id DESC
				LIMIT :offset, :count");
		} else {
			$cmd = $this->connection->prepare(
				"SELECT created_at, id, payload, updated_at, version
				FROM {$this->shardName}.{$this->bucketName}
				LIMIT :offset, :count");
		}
		
		$cmd->bindParam(':offset', $offset, PDO::PARAM_INT);
		$cmd->bindParam(':count', $count, PDO::PARAM_INT);
		$cmd->execute();
		
		if ($cmd->errorCode() !== '00000') {
			$this->pushError($cmd->errorCode(), $cmd->errorInfo());
			return false;
		}
		
		$cmd->bindColumn(1, $createdAt, PDO::PARAM_STR);
		$cmd->bindColumn(2, $id, PDO::PARAM_INT);
		$cmd->bindColumn(3, $payload, PDO::PARAM_LOB);
		$cmd->bindColumn(4, $updatedAt, PDO::PARAM_STR);
		$cmd->bindColumn(5, $version, PDO::PARAM_INT);
		
		$entries = [];
		
		while ($cmd->fetch()) {
			$entry = new TimelineBucketEntry();
			$entry->created_at = new Carbon($createdAt);
			$entry->id = $id;
			$entry->payload = $payload;
			$entry->updated_at = new Carbon($updatedAt);
			$entry->version = $version;
			$entries[] = $entry;
			
			// Put into cache.
			if ($useCache === true && $this->options['cache'] !== false) {
				$this->options['cache']->put($this->getCacheMeta($entry->id), $entry);
			}
		}
		
		return $entries;
	}
}