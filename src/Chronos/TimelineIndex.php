<?php

namespace Chronos;
	
use Carbon\Carbon;
use Chronos\TimelineIndexEntry;
use PDO;

class TimelineIndex
{
	use Error, Transaction;
	
	private $connection = null;
	private $indexName = '';
	private $indexId = '';
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
	 * @param int $indexId
	 */
	public function __construct(\PDO $connection, &$options, $serverId, $shardId, $indexId)
	{
		$this->connection = $connection;
		$this->indexName = self::getIndexName($options, $indexId);
		$this->indexId = $indexId;
		$this->options = $options;
		$this->serverId = $serverId;
		$this->shardName = $options['shardPrefix'] . $shardId . $options['shardSuffix'];
		$this->shardId = $shardId;
	}
	
	/**
	 * Delete an entry by id (partial clustered index key).
	 * 
	 * @param int $id
	 * @return boolean True if success and one or more entries were affected. False if failure or no entry was affected.
	 */
	public function delete($id)
	{
		$cmd = $this->connection->prepare("
			DELETE FROM {$this->shardName}.{$this->indexName}
			WHERE id = :id");
		$cmd->bindParam(':id', $id, PDO::PARAM_INT);
		$cmd->execute();
		
		if ($cmd->errorCode() === '00000' && $cmd->rowCount() > 0) {
			return true;
		} elseif ($cmd->rowCount() === 0) {
			$this->pushError('C00001', 'No row affected.');
		} else {
			$this->pushError($cmd->errorCode(), $cmd->errorInfo());
		}
		return false;
	}
	
	/**
	 * Delete an entry by A (partial clustered index key).
	 * 
	 * @param int $id
	 * @return boolean True if success and one or more entries were affected. False if failure or no entry was affected.
	 */
	public function deleteByA($a)
	{
		$cmd = $this->connection->prepare("
			DELETE FROM {$this->shardName}.{$this->indexName}
			WHERE a = :a");
		$cmd->bindParam(':a', $a, PDO::PARAM_INT);
		$cmd->execute();
		
		if ($cmd->errorCode() === '00000' && $cmd->rowCount() > 0) {
			return true;
		}  elseif ($cmd->rowCount() === 0) {
			$this->pushError('C00001', 'No row affected.');
		} else {
			$this->pushError($cmd->errorCode(), $cmd->errorInfo());
		}
		return false;
	}
	
	/**
	 * Delete an entry by Id + A (full clustered index key). This is the
	 * recommended to delete a single entry and the only way you can be certain 
	 * only one item is deleted.
	 * 
	 * @param int $id
	 * @param int $a
	 * @return boolean True if success and one entry was affected. False if failure or no entry was affected.
	 */
	public function deleteByIdA($id, $a)
	{
		$cmd = $this->connection->prepare("
			DELETE FROM {$this->shardName}.{$this->indexName}
			WHERE a = :a AND id = :id");
		$cmd->bindParam(':a', $a, PDO::PARAM_INT);
		$cmd->bindParam(':id', $id, PDO::PARAM_INT);
		$cmd->execute();
		
		if ($cmd->errorCode() === '00000' && $cmd->rowCount() === 1) {
			return true;
		}  elseif ($cmd->rowCount() === 0) {
			$this->pushError('C00001', 'No row affected.');
		} else {
			$this->pushError($cmd->errorCode(), $cmd->errorInfo());
		}
		return false;
	}
	
	/**
	 * Get an entry by id. If multiple entries with the same id exists, the
	 * oldest one will likley be returned. To be sure that you get a specific
	 * entry you need to use getByIdA().
	 * 
	 * @param int $id
	 * @return boolean|TimelineIndexEntry TimelineIndexEntry on success, false on failure.
	 */
	public function get($id)
	{
		$entry = new TimelineIndexEntry();
		$cmd = $this->connection->prepare(
				"SELECT a, b, created_at, id, updated_at
				FROM {$this->shardName}.{$this->indexName}
				WHERE id = :id");
		$cmd->bindParam(':id', $id, PDO::PARAM_INT);
		$cmd->execute();
		
		if ($cmd->errorCode() !== '00000') {
			$this->pushError($cmd->errorCode(), $cmd->errorInfo());
			return false;
		}
		
		$cmd->bindColumn(1, $entry->a, PDO::PARAM_INT);
		$cmd->bindColumn(2, $entry->b, PDO::PARAM_INT);
		$cmd->bindColumn(3, $entry->created_at, PDO::PARAM_STR);
		$cmd->bindColumn(4, $entry->id, PDO::PARAM_INT);
		$cmd->bindColumn(5, $entry->updated_at, PDO::PARAM_STR);
		$cmd->fetch(PDO::FETCH_BOUND);
		
		if ($cmd->rowCount() > 0) {
			$entry->created_at = new Carbon($entry->created_at);
			$entry->updated_at = new Carbon($entry->updated_at);
			return $entry;
		} else {
			return false;
		}
	}
	
	/**
	 * Get an entry by A + B. If possible, use getByIdA() instead to avoid
	 * loosing performance.
	 * 
	 * @param int $a
	 * @param int $b
	 * @return boolean|TimelineIndexEntry TimelineIndexEntry on success, false on failure.
	 */
	public function getByAB($a, $b)
	{
		$entry = new TimelineIndexEntry();
		$cmd = $this->connection->prepare(
				"SELECT a, b, created_at, id, updated_at
				FROM {$this->shardName}.{$this->indexName}
				WHERE a = :a AND b = :b");
		$cmd->bindParam(':a', $a, PDO::PARAM_INT);
		$cmd->bindParam(':b', $b, PDO::PARAM_INT);
		$cmd->execute();
		
		if ($cmd->errorCode() !== '00000') {
			$this->pushError($cmd->errorCode(), $cmd->errorInfo());
			return false;
		}
		
		$cmd->bindColumn(1, $entry->a, PDO::PARAM_INT);
		$cmd->bindColumn(2, $entry->b, PDO::PARAM_INT);
		$cmd->bindColumn(3, $entry->created_at, PDO::PARAM_STR);
		$cmd->bindColumn(4, $entry->id, PDO::PARAM_INT);
		$cmd->bindColumn(5, $entry->updated_at, PDO::PARAM_STR);
		$cmd->fetch(PDO::FETCH_BOUND);
		
		if ($cmd->rowCount() > 0) {
			$entry->created_at = new Carbon($entry->created_at);
			$entry->updated_at = new Carbon($entry->updated_at);
			return $entry;
		} else {
			return false;
		}
	}
	
	/**
	 * Get an entry by Id + A (full compound key). Fastest way to retrieve a
	 * specific entry.
	 * 
	 * @param int $id
	 * @param int $a
	 * @return boolean|TimelineIndexEntry TimelineIndexEntry on success, false on failure.
	 */
	public function getByIdA($id, $a)
	{
		$entry = new TimelineIndexEntry();
		$cmd = $this->connection->prepare(
				"SELECT a, id, created_at, id, updated_at
				FROM {$this->shardName}.{$this->indexName}
				WHERE a = :a AND id = :id");
		$cmd->bindParam(':a', $a, PDO::PARAM_INT);
		$cmd->bindParam(':id', $id, PDO::PARAM_INT);
		$cmd->execute();
		
		if ($cmd->errorCode() !== '00000') {
			$this->pushError($cmd->errorCode(), $cmd->errorInfo());
			return false;
		}
		
		$cmd->bindColumn(1, $entry->a, PDO::PARAM_INT);
		$cmd->bindColumn(2, $entry->b, PDO::PARAM_INT);
		$cmd->bindColumn(3, $entry->created_at, PDO::PARAM_STR);
		$cmd->bindColumn(4, $entry->id, PDO::PARAM_INT);
		$cmd->bindColumn(5, $entry->updated_at, PDO::PARAM_STR);
		$cmd->fetch(PDO::FETCH_BOUND);
		
		if ($cmd->rowCount() > 0) {
			$entry->created_at = new Carbon($entry->created_at);
			$entry->updated_at = new Carbon($entry->updated_at);
			return $entry;
		} else {
			return false;
		}
	}
	
	/**
	 * 
	 * @param array $options Reference to the Chronos options.
	 * @param int $indexId
	 * @return string DB-index name.
	 */
	public static function getIndexName(&$options, $indexId)
	{
		return $options['timelineIndexPrefix'] . $indexId . $options['timelineIndexSuffix'];
	}
	
	/**
	 * Insert or update an entry in the bucket. Be aware that we are using a
	 * clustered index (Id + A), its possible that you will end up
	 * with a "duplicate" entry when you intended to update an existing entry.
	 * 
	 * A "duplicate" entry will be the result of the following scenario:
	 * 
	 * Put (Id=1,A=20)
	 * $e = Get (Id=1,A=20)
	 * $e->A = 30
	 * Put $e
	 * 
	 * Now we have both have (Id=1,A=20) and (Id=1,A=30) in the database. This 
	 * is expexted. To avoid this, never change A of an existing entry.
	 * 
	 * @param TimelineIndexEntry $entry Reference to the entry to insert or update.
	 * @param boolean $useCache False to bypass cache (if available).
	 * @return boolean|TimelineIndexEntry TimelineIndexEntry on success, false on failure.
	 */
	public function put(TimelineIndexEntry &$entry)
	{
		if ($entry->id === 0) {
			$cmd = $this->connection->prepare("
				INSERT INTO {$this->shardName}.{$this->indexName}
					(a, b, created_at, updated_at) VALUES
					(:a, :b, :created_at, :updated_at)");
		} else {
			$cmd = $this->connection->prepare("
				INSERT INTO {$this->shardName}.{$this->indexName}
					(a, b, created_at, id, updated_at) VALUES
					(:a, :b, :created_at, :id, :updated_at)
				ON DUPLICATE KEY UPDATE 
					a = :a, b = :b, updated_at = :updated_at");
		}
		
		$now = Carbon::now();
		if (is_null($entry->created_at)) {
			$entry->created_at = $now;
		}
		$entry->updated_at = $now;
		
		$cmd->bindParam(':a', $entry->a, PDO::PARAM_INT);
		$cmd->bindParam(':b', $entry->b, PDO::PARAM_INT);
		$cmd->bindParam(':created_at', $entry->created_at, PDO::PARAM_STR);
		if ($entry->id !== 0) {
			$cmd->bindParam(':id', $entry->id, PDO::PARAM_INT);
		}
		$cmd->bindParam(':updated_at', $entry->updated_at, PDO::PARAM_STR);
		$cmd->execute();
		
		if ($cmd->errorCode() === '00000') {
			if ($entry->id === 0) {
				$entry->id = $this->connection->lastInsertId();
			}
			return $entry;
		} else {
			$this->pushError($cmd->errorCode(), $cmd->errorInfo());
			return false;
		}
	}
	
	/**
	 * Scans the bucket for data.
	 * 
	 * @param int $offset Entry offset (starts at 0).
	 * @param int $count Number of entries to return.
	 * @param boolean $reverse Get newest first, you probably want that.
	 * @return boolean|TimelineIndexEntry[] Returns an array of TimelineIndexEntry on success. False on failure.
	 */
	public function scan($offset, $count, $reverse = false)
	{
		if ($reverse === true) {
			$cmd = $this->connection->prepare(
				"SELECT a, b, created_at, id, updated_at
				FROM {$this->shardName}.{$this->indexName}
				ORDER BY id DESC
				LIMIT :offset, :count");
		} else {
			$cmd = $this->connection->prepare(
				"SELECT a, b, created_at, id, updated_at
				FROM {$this->shardName}.{$this->indexName}
				LIMIT :offset, :count");
		}
		
		$cmd->bindParam(':offset', $offset, PDO::PARAM_INT);
		$cmd->bindParam(':count', $count, PDO::PARAM_INT);
		$cmd->execute();
		
		if ($cmd->errorCode() !== '00000') {
			$this->pushError($cmd->errorCode(), $cmd->errorInfo());
			return false;
		}
		
		$cmd->bindColumn(1, $a, PDO::PARAM_INT);
		$cmd->bindColumn(2, $b, PDO::PARAM_INT);
		$cmd->bindColumn(3, $createdAt, PDO::PARAM_STR);
		$cmd->bindColumn(4, $id, PDO::PARAM_INT);
		$cmd->bindColumn(5, $updatedAt, PDO::PARAM_STR);
		
		$entries = [];
		
		while ($cmd->fetch()) {
			$entry = new TimelineIndexEntry();
			$entry->a = $a;
			$entry->b = $b;
			$entry->created_at = new Carbon($createdAt);
			$entry->id = $id;
			$entry->updated_at = new Carbon($updatedAt);
			$entries[] = $entry;
		}
		
		return $entries;
	}
	
	/**
	 * Scans the bucket for data based on A. This is the fastest way to read
	 * from an index and should be used to get chronologically ordered index
	 * entries by A.
	 * 
	 * @param int $a
	 * @param int $offset Entry offset (starts at 0).
	 * @param int $count Number of entries to return.
	 * @param boolean $reverse Get newest first, you probably want that.
	 * @return boolean|TimelineIndexEntry[] Returns an array of TimelineIndexEntry on success. False on failure.
	 */
	public function scanByA($a, $offset, $count, $reverse = false)
	{
		if ($reverse === true) {
			$cmd = $this->connection->prepare(
				"SELECT a, b, created_at, id, updated_at
				FROM {$this->shardName}.{$this->indexName}
				WHERE a = :a
				ORDER BY id DESC
				LIMIT :offset, :count");
		} else {
			$cmd = $this->connection->prepare(
				"SELECT a, b, created_at, id, updated_at
				FROM {$this->shardName}.{$this->indexName}
				WHERE a = :a
				LIMIT :offset, :count");
		}
		
		$cmd->bindParam(':a', $a, PDO::PARAM_STR);
		$cmd->bindParam(':offset', $offset, PDO::PARAM_INT);
		$cmd->bindParam(':count', $count, PDO::PARAM_INT);
		$cmd->execute();
		
		if ($cmd->errorCode() !== '00000') {
			$this->pushError($cmd->errorCode(), $cmd->errorInfo());
			return false;
		}
		
		$cmd->bindColumn(1, $a, PDO::PARAM_INT);
		$cmd->bindColumn(2, $b, PDO::PARAM_INT);
		$cmd->bindColumn(3, $createdAt, PDO::PARAM_STR);
		$cmd->bindColumn(4, $id, PDO::PARAM_INT);
		$cmd->bindColumn(5, $updatedAt, PDO::PARAM_STR);
		
		$entries = [];
		
		while ($cmd->fetch()) {
			$entry = new TimelineIndexEntry();
			$entry->a = $a;
			$entry->b = $b;
			$entry->created_at = new Carbon($createdAt);
			$entry->id = $id;
			$entry->updated_at = new Carbon($updatedAt);
			$entries[] = $entry;
		}
		
		return $entries;
	}
}