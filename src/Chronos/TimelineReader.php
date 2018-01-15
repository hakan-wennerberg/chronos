<?php

namespace Chronos;

use Chronos\Chronos;
use Chronos\TimelineIndex;
use Chronos\TimelineIndexId;

class TimelineReader
{
	private $batchSize = 0;
	private $chronos = null;
	private $index = null;
	private $lastId = 0;
	private $offset = 0;
	
	/**
	 * 
	 * @param Chronos $chronos Reference to Chronos object.
	 * @param TimelineIndex $index Reference to TimelineIndex object to read.
	 * @param int $offset Scan start offset. Use last offset value from any previous read to avoid useless scans.
	 * @param int $lastId Last id retrieved. Use to avoid duplicates.
	 * @param int $batchSize Number of additional index entries to get if initial request does not fill the requested scan count.
	 */
	public function __construct(Chronos &$chronos, TimelineIndex &$index, 
			$offset = 0, $lastId = 274877906944, $batchSize = 10)
	{
		$this->batchSize = $batchSize;
		$this->chronos = $chronos;
		$this->index = $index;
		$this->lastId = $lastId;
		$this->offset = $offset;
	}
	
	/**
	 * Returns the latest TimelineIndexEntry->id scanned. Get this value after
	 * a scan and use it as input to the next sequential scan using any new
	 * TimelineReader.
	 * 
	 * @return int
	 */
	public function getLastId()
	{
		return $this->lastId;
	}
	
	/**
	 * Returns the current TimelineIndex offset. Get this value after a scan and
	 * use it as input to the next sequential scan using any new TimelineReader.
	 * 
	 * @return int
	 */
	public function getOffset()
	{
		return $this->offset;
	}
	
	/**
	 * Read full index without criterias.
	 * 
	 * @param int $count Number of items to get.
	 * @return array An array of TimelineBucketEntry objects.
	 */
	public function read($count)
	{
		return $this->readIndex(false, $count);
	}
	
	/**
	 * Read using the "A" TimelineIndexId.
	 * 
	 * @param int $a
	 * @param int $count Number of items to get.
	 * @return array An array of TimelineBucketEntry objects.
	 */
	public function readA($a, $count)
	{
		return $this->readIndex($a, $count);
	}
	
	/**
	 * 
	 * @param int|boolean $a
	 * @param int $count Number of items to get.
	 * @return array An array of TimelineBucketEntry objects.
	 */
	private function readIndex($a, $count)
	{
		$entries = [];
		
		while (($c = count($entries)) < $count) {
			if ($c === 0) {
				$batchSize = $count;
			} else {
				$batchSize = $this->batchSize;
			}
			
			if ($a !== false) {
				$indexList = $this->index->scanByA($a, $this->offset, $batchSize, true);
			} else {
				$indexList = $this->index->scan($this->offset, $batchSize, true);
			}
			
			// Break if no more items exist in the index.
			if (count($indexList) === 0) {
				break;
			}
			
			foreach ($indexList as $indexEntry) {
				
				// Only get items older than last id fetched.
				if ($indexEntry->id < $this->lastId) {
					$this->lastId = $indexEntry->id;
					
					// Get actual item.
					$bucket = $this->chronos->getBucket(
							Chronos::BUCKET_TYPE_TIMELINE,
							TimelineIndexId::unpackShardId($indexEntry->b),
							TimelineIndexId::unpackBucketId($indexEntry->b)
						);
					
					// Get bucket issue.
					if ($bucket === false) {
						continue;
					}
					
					// Get entry.
					$entry = $bucket->get(TimelineIndexId::unpackEntryId($indexEntry->b));
					
					// Get entry issue (possibly deleted).
					if ($entry === false) {
						continue;
					}
					
					$entries[] = $entry;
					
					// Exit if we got all data requested.
					if (count($entries) == $count) {
						$this->offset++;
						break;
					}
				}
				
				$this->offset++;
			}
		}
		
		return $entries;
	}
	
	/**
	 * 
	 * @param int $lastId Set last TimelineIndex->id retrieved.
	 */
	public function setLastId($lastId)
	{
		$this->lastId = $lastId;
	}
	
	/**
	 * 
	 * @param int $offset Set offset to start at before scanning.
	 */
	public function setOffset($offset)
	{
		$this->offset = $offset;
	}
}