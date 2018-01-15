<?php

namespace Chronos;

use Carbon\Carbon;
use Chronos\UserBucketEntry;
use PDO;

class UserBucket
{
	use Error, Transaction;
	
	private $bucketName = '';
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
	public function __construct(\PDO $connection, &$options, $serverId, $shardId)
	{
		$this->bucketName = self::getBucketName($options);
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
	 * @param boolean $hard True to actually delete the entry, false if to soft-delete instead.
	 * @param boolean $useCache False to bypass cache (if available).
	 * @return boolean True if success and one entry was affected. False if failure or no entry was affected.
	 */
	public function delete($id, $hard = false, $useCache = true)
	{
		if ($hard === true) {
			$cmd = $this->connection->prepare("
				DELETE FROM {$this->shardName}.{$this->bucketName}
				WHERE id = :id");
			$cmd->bindParam(':id', $id, PDO::PARAM_INT);
		} else {
			$cmd = $this->connection->prepare("
				UPDATE {$this->shardName}.{$this->bucketName}
				SET deleted_at = :deleted_at
				WHERE id = :id");
			$now = Carbon::now();
			$cmd->bindParam(':deleted_at', $now, PDO::PARAM_STR);
			$cmd->bindParam(':id', $id, PDO::PARAM_INT);
		}
		
		$cmd->execute();
		
		if ($cmd->errorCode() === '00000' && $cmd->rowCount() === 1) {
			// Delete from cache (or update on soft delete).
			if ($useCache === true && $this->options['cache'] !== false) {
				if ($hard) {
					$this->options['cache']->delete($this->getCacheMeta($id));
				} else {
					$this->options['cache']->put($this->getCacheMeta($id),
							$this->get($id));
				}
			}
			return true;
		} elseif ($cmd->rowCount() === 0) {
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
	 * @return boolean|UserBucketEntry UserBucketEntry on success, false on failure.
	 */
	public function get($id, $useCache = true)
	{
		// Get from cache.
		if ($useCache === true && $this->options['cache'] !== false) {
			if (($entry = $this->options['cache']->get($this->getCacheMeta($id))) !== false) {
				return $entry;
			}
		}
		
		$entry = new UserBucketEntry();
		$entry->loading();
		
		if (is_int($id)) {
			$cmd = $this->connection->prepare(
				"SELECT created_at, deleted_at, email, id, payload, shard, updated_at, version
				FROM {$this->shardName}.{$this->bucketName}
				WHERE id = :id");
			$cmd->bindParam(':id', $id, PDO::PARAM_INT);
		} else {
			$cmd = $this->connection->prepare(
				"SELECT created_at, deleted_at, email, id, payload, shard, updated_at, version
				FROM {$this->shardName}.{$this->bucketName}
				WHERE email = :email");
			$cmd->bindParam(':email', $id, PDO::PARAM_INT);
		}
		
		$cmd->execute();
		
		if ($cmd->errorCode() !== '00000') {
			$this->pushError($cmd->errorCode(), $cmd->errorInfo());
			return false;
		}
		
		$cmd->bindColumn(1, $entry->created_at, PDO::PARAM_STR);
		$cmd->bindColumn(2, $entry->deleted_at, PDO::PARAM_STR);
		$cmd->bindColumn(3, $entry->email, PDO::PARAM_STR);
		$cmd->bindColumn(4, $entry->id, PDO::PARAM_INT);
		$cmd->bindColumn(5, $entry->payload, PDO::PARAM_LOB);
		$cmd->bindColumn(6, $entry->shard, PDO::PARAM_INT);
		$cmd->bindColumn(7, $entry->updated_at, PDO::PARAM_STR);
		$cmd->bindColumn(8, $entry->version, PDO::PARAM_INT);
		$cmd->fetch(PDO::FETCH_BOUND);
		
		if ($cmd->rowCount() > 0) {
			$entry->created_at = new Carbon($entry->created_at);
			$entry->updated_at = new Carbon($entry->updated_at);
			
			if (is_null($entry->deleted_at) === false) {
				$entry->deleted_at = new Carbon($entry->deleted_at);
			}
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
			'bucketType' => Chronos::BUCKET_TYPE_USER,
			'key' => $key,
			'cacheKey' => "UB-{$this->shardId}-{$this->bucketId}-{$key}",
			'serverId' => $this->serverId,
			'shardId' => $this->shardId,
			'shardName' => $this->shardName
		];
	}
	
	/**
	 * 
	 * @param array $options Reference to the Chronos options.
	 * @return string DB-bucket name.
	 */
	public static function getBucketName(&$options)
	{
		return $options['userBucketPrefix'] . 'users' . $options['userBucketSuffix'];
	}
	
	/**
	 * Insert or update an entry in the bucket.
	 * 
	 * @param UserBucketEntry $entry Reference to the entry to insert or update.
	 * @param boolean $useCache False to bypass cache (if available).
	 * @return boolean|UserBucketEntry UserBucketEntry on success, false on failure.
	 */
	public function put(UserBucketEntry &$entry, $useCache = true)
	{
		$entry->saving();
		
		if ($entry->id === 0) {
			$cmd = $this->connection->prepare("
				INSERT INTO {$this->shardName}.{$this->bucketName}
					(created_at, deleted_at, email, payload, shard, updated_at, version) VALUES
					(:created_at, :deleted_at, :email, :payload, :shard, :updated_at, :version)
				ON DUPLICATE KEY UPDATE 
					deleted_at = :deleted_at, email = :email, payload = :payload, shard = :shard, updated_at = :updated_at, version = :version");
		} else {
			$cmd = $this->connection->prepare("
				INSERT INTO {$this->shardName}.{$this->bucketName}
					(created_at, deleted_at, email, id, payload, shard, updated_at, version) VALUES
					(:created_at, :deleted_at, :email, :id, :payload, :shard, :updated_at, :version)
				ON DUPLICATE KEY UPDATE 
					deleted_at = :deleted_at, email = :email, payload = :payload, shard = :shard, updated_at = :updated_at, version = :version");
		}
		
		$now = Carbon::now();
		if (is_null($entry->created_at)) {
			$entry->created_at = $now;
		}
		$entry->updated_at = $now;
				
		$cmd->bindParam(':created_at', $entry->created_at, PDO::PARAM_STR);
		$cmd->bindParam(':deleted_at', $entry->deleted_at, PDO::PARAM_STR);
		$cmd->bindParam(':email', $entry->email, PDO::PARAM_STR);
		if ($entry->id !== 0) {
			$cmd->bindParam(':id', $entry->id, PDO::PARAM_INT);
		}
		$cmd->bindParam(':payload', $entry->payload, PDO::PARAM_LOB);
		$cmd->bindParam(':shard', $entry->shard, PDO::PARAM_INT);
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
	 * @return boolean|UserBucketEntry[] Returns an array of UserBucketEntry on success. False on failure.
	 */
	public function scan($offset, $count, $reverse = false, $useCache = true)
	{
		if ($reverse === true) {
			$cmd = $this->connection->prepare(
				"SELECT created_at, deleted_at, email, id, payload, shard, updated_at, version
				FROM {$this->shardName}.{$this->bucketName}
				ORDER BY id DESC
				LIMIT :offset, :count");
		} else {
			$cmd = $this->connection->prepare(
				"SELECT created_at, deleted_at, email, id, payload, shard, updated_at, version
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
		$cmd->bindColumn(2, $deletedAt, PDO::PARAM_STR);
		$cmd->bindColumn(3, $email, PDO::PARAM_STR);
		$cmd->bindColumn(4, $id, PDO::PARAM_INT);
		$cmd->bindColumn(5, $payload, PDO::PARAM_LOB);
		$cmd->bindColumn(6, $shard, PDO::PARAM_INT);
		$cmd->bindColumn(7, $updatedAt, PDO::PARAM_STR);
		$cmd->bindColumn(8, $version, PDO::PARAM_INT);
		
		$entries = [];
		
		while ($cmd->fetch()) {
			$entry = new UserBucketEntry();
			$entry->created_at = new Carbon($createdAt);
			
			if (is_null($deletedAt) === false) {
				$entry->deleted_at = new Carbon($deletedAt);
			}
			
			$entry->email = $email;
			$entry->id = $id;
			$entry->payload = $payload;
			$entry->shard = $shard;
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