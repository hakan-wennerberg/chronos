<?php

namespace Chronos;

class TimelineIndexId
{
	public $entryId = 0;	// 38 bit unsigned
	public $bucketId = 0;	// 13 bit unsigned
	public $shardId = 0;	// 12 bit unsigned
	
	/**
	 * 
	 * @param int $shardId
	 * @param int $bucketId
	 * @param int $entryId
	 */
	public function __construct($shardId = 0, $bucketId = 0, $entryId = 0)
	{
		$this->entryId = $entryId;
		$this->bucketId = $bucketId;
		$this->shardId = $shardId;
	}
	
	/**
	 * Convenience method for TimelineIndexId::pack().
	 * 
	 * @return int
	 */
	public function p()
	{
		return self::pack($this);
	}
	
	/**
	 * Converts a TimelineIndexId to its integer representation.
	 * 
	 * @param TimelineIndexId $ab
	 * @return type
	 */
	public static function pack(TimelineIndexId $ab)
	{
		return self::packRaw($ab->shardId, $ab->bucketId, $ab->entryId);
	}
	
	/**
	 * Converts a TimelineIndexId to its integer representation.
	 * 
	 * @param int $shardId
	 * @param int $bucketId
	 * @param int $entryId
	 * @return int
	 */
	public static function packRaw($shardId = 0, $bucketId = 0, $entryId = 0)
	{
		$s = ((4095 & $shardId) << 51);
		$b = ((8191 & $bucketId) << 38);
		$e = (274877906943 & $entryId);
		return $s | $b | $e;
	}
	
	/**
	 * Converts an integer to a TimelineIndexId.
	 * 
	 * @param int $ab
	 * @return TimelineIndexId
	 */
	public static function unpack($ab)
	{
		return new TimelineIndexId(
				(($ab >> 51) & 4095),
				(($ab >> 38) & 8191),
				($ab & 274877906943)
			);
	}
	
	public static function unpackBucketId($ab)
	{
		return (($ab >> 38) & 8191);
	}
	
	public static function unpackEntryId($ab)
	{
		return ($ab & 274877906943);
	}
	
	public static function unpackShardId($ab)
	{
		return (($ab >> 51) & 4095);
	}
}