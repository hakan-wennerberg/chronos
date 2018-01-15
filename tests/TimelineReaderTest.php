<?php


use Chronos\Chronos;
use Chronos\TimelineBucketEntry;
use Chronos\TimelineIndexEntry;
use Chronos\TimelineIndexId;
use Chronos\TimelineReader;

class TimelineReaderTest extends TestFixture
{
	private $bucketId = 1;
	private $indexId = 1;
	private $shardId = 1;
	
	public static function setUpBeforeClass()
	{
		$c = self::getChronos();
		$c->createShard(1, 1);
	}

	public static function tearDownAfterClass()
	{
		$c = self::getChronos();
		$c->dropShard(1, 1);
	}
	
	public function test01SingleReadA()
	{
		$c = self::getChronos();
		
		// Create bucket and index.
		$b = $c->createBucket(
				Chronos::BUCKET_TYPE_TIMELINE,
				$this->shardId,
				$this->bucketId
			);
		$idx = $c->createIndex(
				Chronos::INDEX_TYPE_TIMELINE,
				$this->shardId,
				$this->indexId
			);
		
		// Check result.
		$this->assertEquals(get_class($b), 'Chronos\TimelineBucket');
		$this->assertEquals(get_class($idx), 'Chronos\TimelineIndex');
		
		// Create 20 bucket entries.
		for ($i = 1; $i <= 20; $i++) {
			$b->put(new TimelineBucketEntry(0, "bucket entry {$i}"));
		}
		
		// Create index entries spliting them up into 2 different A.
		for ($i = 1; $i <= 20; $i++) {
			$a = ($i % 2) ? 1 : 2;
			$idx->put(new TimelineIndexEntry(
					0, 
					$a,
					TimelineIndexId::packRaw(
							$this->shardId,
							$this->bucketId,
							$i
						)
				));
		}
		
		// Create 2 index entried pointing to non-existing bucket entries.
		$idx->put(new TimelineIndexEntry(
				0, 
				1,
				TimelineIndexId::packRaw(
						$this->shardId,
						$this->bucketId,
						54
					)
			));
		
		$idx->put(new TimelineIndexEntry(
				0, 
				2,
				TimelineIndexId::packRaw(
						$this->shardId,
						$this->bucketId,
						77
					)
			));
		
		// Get 5 newest item of A = 1.
		$reader = new TimelineReader($c, $idx);
		$entries = $reader->readA(1, 5);
		
		$this->assertEquals(19, $entries[0]->id);
		$this->assertEquals(17, $entries[1]->id);
		$this->assertEquals(15, $entries[2]->id);
		$this->assertEquals(13, $entries[3]->id);
		$this->assertEquals(11, $entries[4]->id);
		
		// We now know that index entry 21 to missing entry was successfully 
		// skipped and no entries of non $A = 1 was returned. We also know we
		// got the latest items first (in order of insertion).
	}
	
	public function test02SequentialReadsA()
	{
		$c = self::getChronos();
		
		// Test that we can perform two continious reads.
		$idx = $c->getIndex(
				Chronos::INDEX_TYPE_TIMELINE,
				$this->shardId,
				$this->indexId
			);
		
		$this->assertEquals(get_class($idx), 'Chronos\TimelineIndex');
		
		// First read.
		$reader = new TimelineReader($c, $idx);
		$entries = $reader->readA(2, 3);
		$this->assertEquals(3, Count($entries));
		$this->assertEquals(20, $entries[0]->id);
		$this->assertEquals(18, $entries[1]->id);
		$this->assertEquals(16, $entries[2]->id);
		
		// Second read.
		$entries = $reader->readA(2, 3);
		$this->assertEquals(3, Count($entries));
		$this->assertEquals(14, $entries[0]->id);
		$this->assertEquals(12, $entries[1]->id);
		$this->assertEquals(10, $entries[2]->id);
		
		// Third read.
		$entries = $reader->readA(2, 3);
		$this->assertEquals(3, Count($entries));
		$this->assertEquals(8, $entries[0]->id);
		$this->assertEquals(6, $entries[1]->id);
		$this->assertEquals(4, $entries[2]->id);
		
		// Fourth read.
		$entries = $reader->readA(2, 3);
		$this->assertEquals(1, Count($entries));
		$this->assertEquals(2, $entries[0]->id);
	}
	
	public function test03SequentialRequestsA()
	{
		$c = self::getChronos();
		
		// Test that we can perform two continious reads.
		$idx = $c->getIndex(
				Chronos::INDEX_TYPE_TIMELINE,
				$this->shardId,
				$this->indexId
			);
		
		$this->assertEquals(get_class($idx), 'Chronos\TimelineIndex');
		
		// First request.
		$reader = new TimelineReader($c, $idx);
		$entries = $reader->readA(2, 3);
		$this->assertEquals(3, Count($entries));
		$this->assertEquals(20, $entries[0]->id);
		$this->assertEquals(18, $entries[1]->id);
		$this->assertEquals(16, $entries[2]->id);
		$lastId = $reader->getLastId();
		$offset = $reader->getOffset();
		
		// Second request.
		$reader = new TimelineReader($c, $idx, $offset, $lastId);
		$entries = $reader->readA(2, 3);
		$this->assertEquals(3, Count($entries));
		$this->assertEquals(14, $entries[0]->id);
		$this->assertEquals(12, $entries[1]->id);
		$this->assertEquals(10, $entries[2]->id);
		$lastId = $reader->getLastId();
		$offset = $reader->getOffset();
		
		// Third request.
		$reader = new TimelineReader($c, $idx, $offset, $lastId);
		$entries = $reader->readA(2, 3);
		$this->assertEquals(3, Count($entries));
		$this->assertEquals(8, $entries[0]->id);
		$this->assertEquals(6, $entries[1]->id);
		$this->assertEquals(4, $entries[2]->id);
		$lastId = $reader->getLastId();
		$offset = $reader->getOffset();
		
		// Fourth request.
		$reader = new TimelineReader($c, $idx, $offset, $lastId);
		$entries = $reader->readA(2, 3);
		$this->assertEquals(1, Count($entries));
		$this->assertEquals(2, $entries[0]->id);
	}
	
	public function test04SequentialRequestsOffsetA()
	{
		$c = self::getChronos();
		
		// Test that we can perform two continious reads.
		$idx = $c->getIndex(
				Chronos::INDEX_TYPE_TIMELINE,
				$this->shardId,
				$this->indexId
			);
		
		$this->assertEquals(get_class($idx), 'Chronos\TimelineIndex');
		
		// First request.
		$reader = new TimelineReader($c, $idx);
		$entries = $reader->readA(2, 3);
		$this->assertEquals(3, Count($entries));
		$this->assertEquals(20, $entries[0]->id);
		$this->assertEquals(18, $entries[1]->id);
		$this->assertEquals(16, $entries[2]->id);
		$offset = $reader->getOffset();
		
		// Second request.
		$reader = new TimelineReader($c, $idx, $offset);
		$entries = $reader->readA(2, 3);
		$this->assertEquals(3, Count($entries));
		$this->assertEquals(14, $entries[0]->id);
		$this->assertEquals(12, $entries[1]->id);
		$this->assertEquals(10, $entries[2]->id);
		$offset = $reader->getOffset();
		
		// Third request.
		$reader = new TimelineReader($c, $idx, $offset);
		$entries = $reader->readA(2, 3);
		$this->assertEquals(3, Count($entries));
		$this->assertEquals(8, $entries[0]->id);
		$this->assertEquals(6, $entries[1]->id);
		$this->assertEquals(4, $entries[2]->id);
		$offset = $reader->getOffset();
		
		// Fourth request.
		$reader = new TimelineReader($c, $idx, $offset);
		$entries = $reader->readA(2, 3);
		$this->assertEquals(1, Count($entries));
		$this->assertEquals(2, $entries[0]->id);
	}
	
	public function test05SequentialRequestsLastIdA()
	{
		$c = self::getChronos();
		
		// Test that we can perform two continious reads.
		$idx = $c->getIndex(
				Chronos::INDEX_TYPE_TIMELINE,
				$this->shardId,
				$this->indexId
			);
		
		$this->assertEquals(get_class($idx), 'Chronos\TimelineIndex');
		
		// First request.
		$reader = new TimelineReader($c, $idx);
		$entries = $reader->readA(2, 3);
		$this->assertEquals(3, Count($entries));
		$this->assertEquals(20, $entries[0]->id);
		$this->assertEquals(18, $entries[1]->id);
		$this->assertEquals(16, $entries[2]->id);
		$lastId = $reader->getLastId();
		
		// Second request.
		$reader = new TimelineReader($c, $idx, 0, $lastId);
		$entries = $reader->readA(2, 3);
		$this->assertEquals(3, Count($entries));
		$this->assertEquals(14, $entries[0]->id);
		$this->assertEquals(12, $entries[1]->id);
		$this->assertEquals(10, $entries[2]->id);
		$lastId = $reader->getLastId();
		
		// Third request.
		$reader = new TimelineReader($c, $idx, 0, $lastId);
		$entries = $reader->readA(2, 3);
		$this->assertEquals(3, Count($entries));
		$this->assertEquals(8, $entries[0]->id);
		$this->assertEquals(6, $entries[1]->id);
		$this->assertEquals(4, $entries[2]->id);
		$lastId = $reader->getLastId();
		
		// Fourth request.
		$reader = new TimelineReader($c, $idx, 0, $lastId);
		$entries = $reader->readA(2, 3);
		$this->assertEquals(1, Count($entries));
		$this->assertEquals(2, $entries[0]->id);
	}
	
	public function test06SingleRead()
	{
		$c = self::getChronos();
		
		// Create bucket and index.
		$b = $c->getBucket(
				Chronos::BUCKET_TYPE_TIMELINE,
				$this->shardId,
				$this->bucketId
			);
		$idx = $c->getIndex(
				Chronos::INDEX_TYPE_TIMELINE,
				$this->shardId,
				$this->indexId
			);
		
		// Check result.
		$this->assertEquals(get_class($b), 'Chronos\TimelineBucket');
		$this->assertEquals(get_class($idx), 'Chronos\TimelineIndex');
		
		// Get 5 newest items of any A.
		$reader = new TimelineReader($c, $idx);
		$entries = $reader->read(5);
		
		$this->assertEquals(20, $entries[0]->id);
		$this->assertEquals(19, $entries[1]->id);
		$this->assertEquals(18, $entries[2]->id);
		$this->assertEquals(17, $entries[3]->id);
		$this->assertEquals(16, $entries[4]->id);
		
		// We now know that index entry 22 and 21 to missing entry was successfully 
		// skipped. We also know we got the latest items first (in order of insertion).
	}
	
	public function test07SequentialReads()
	{
		$c = self::getChronos();
		
		// Test that we can perform two continious reads.
		$idx = $c->getIndex(
				Chronos::INDEX_TYPE_TIMELINE,
				$this->shardId,
				$this->indexId
			);
		
		$this->assertEquals(get_class($idx), 'Chronos\TimelineIndex');
		
		// First read.
		$reader = new TimelineReader($c, $idx);
		$entries = $reader->read(3);
		$this->assertEquals(3, Count($entries));
		$this->assertEquals(20, $entries[0]->id);
		$this->assertEquals(19, $entries[1]->id);
		$this->assertEquals(18, $entries[2]->id);
		
		// Second read.
		$entries = $reader->read(3);
		$this->assertEquals(3, Count($entries));
		$this->assertEquals(17, $entries[0]->id);
		$this->assertEquals(16, $entries[1]->id);
		$this->assertEquals(15, $entries[2]->id);
		
		// Third read.
		$entries = $reader->read(3);
		$this->assertEquals(3, Count($entries));
		$this->assertEquals(14, $entries[0]->id);
		$this->assertEquals(13, $entries[1]->id);
		$this->assertEquals(12, $entries[2]->id);
		
		// Fourth read.
		$entries = $reader->read(40);
		$this->assertEquals(11, Count($entries));
		$this->assertEquals(11, $entries[0]->id);
	}
	
	public function test08SequentialRequests()
	{
		$c = self::getChronos();
		
		// Test that we can perform two continious reads.
		$idx = $c->getIndex(
				Chronos::INDEX_TYPE_TIMELINE,
				$this->shardId,
				$this->indexId
			);
		
		$this->assertEquals(get_class($idx), 'Chronos\TimelineIndex');
		
		// First request.
		$reader = new TimelineReader($c, $idx);
		$entries = $reader->read(3);
		$this->assertEquals(3, Count($entries));
		$this->assertEquals(20, $entries[0]->id);
		$this->assertEquals(19, $entries[1]->id);
		$this->assertEquals(18, $entries[2]->id);
		$lastId = $reader->getLastId();
		$offset = $reader->getOffset();
		
		// Second request.
		$reader = new TimelineReader($c, $idx, $offset, $lastId);
		$entries = $reader->read(3);
		$this->assertEquals(3, Count($entries));
		$this->assertEquals(17, $entries[0]->id);
		$this->assertEquals(16, $entries[1]->id);
		$this->assertEquals(15, $entries[2]->id);
		$lastId = $reader->getLastId();
		$offset = $reader->getOffset();
		
		// Third request.
		$reader = new TimelineReader($c, $idx, $offset, $lastId);
		$entries = $reader->read(3);
		$this->assertEquals(3, Count($entries));
		$this->assertEquals(14, $entries[0]->id);
		$this->assertEquals(13, $entries[1]->id);
		$this->assertEquals(12, $entries[2]->id);
		$lastId = $reader->getLastId();
		$offset = $reader->getOffset();
		
		// Fourth request.
		$reader = new TimelineReader($c, $idx, $offset, $lastId);
		$entries = $reader->read(40);
		$this->assertEquals(11, Count($entries));
		$this->assertEquals(11, $entries[0]->id);
	}
	
	public function test09SequentialRequestsOffset()
	{
		$c = self::getChronos();
		
		// Test that we can perform two continious reads.
		$idx = $c->getIndex(
				Chronos::INDEX_TYPE_TIMELINE,
				$this->shardId,
				$this->indexId
			);
		
		$this->assertEquals(get_class($idx), 'Chronos\TimelineIndex');
		
		// First request.
		$reader = new TimelineReader($c, $idx);
		$entries = $reader->read(3);
		$this->assertEquals(3, Count($entries));
		$this->assertEquals(20, $entries[0]->id);
		$this->assertEquals(19, $entries[1]->id);
		$this->assertEquals(18, $entries[2]->id);
		$offset = $reader->getOffset();
		
		// Second request.
		$reader = new TimelineReader($c, $idx, $offset);
		$entries = $reader->read(3);
		$this->assertEquals(3, Count($entries));
		$this->assertEquals(17, $entries[0]->id);
		$this->assertEquals(16, $entries[1]->id);
		$this->assertEquals(15, $entries[2]->id);
		$offset = $reader->getOffset();
		
		// Third request.
		$reader = new TimelineReader($c, $idx, $offset);
		$entries = $reader->read(3);
		$this->assertEquals(3, Count($entries));
		$this->assertEquals(14, $entries[0]->id);
		$this->assertEquals(13, $entries[1]->id);
		$this->assertEquals(12, $entries[2]->id);
		$offset = $reader->getOffset();
		
		// Fourth request.
		$reader = new TimelineReader($c, $idx, $offset);
		$entries = $reader->read(40);
		$this->assertEquals(11, Count($entries));
		$this->assertEquals(11, $entries[0]->id);
	}
	
	public function test10SequentialRequestsLastId()
	{
		$c = self::getChronos();
		
		// Test that we can perform two continious reads.
		$idx = $c->getIndex(
				Chronos::INDEX_TYPE_TIMELINE,
				$this->shardId,
				$this->indexId
			);
		
		$this->assertEquals(get_class($idx), 'Chronos\TimelineIndex');
		
		// First request.
		$reader = new TimelineReader($c, $idx);
		$entries = $reader->read(3);
		$this->assertEquals(3, Count($entries));
		$this->assertEquals(20, $entries[0]->id);
		$this->assertEquals(19, $entries[1]->id);
		$this->assertEquals(18, $entries[2]->id);
		$lastId = $reader->getLastId();
		
		// Second request.
		$reader = new TimelineReader($c, $idx, 0, $lastId);
		$entries = $reader->read(3);
		$this->assertEquals(3, Count($entries));
		$this->assertEquals(17, $entries[0]->id);
		$this->assertEquals(16, $entries[1]->id);
		$this->assertEquals(15, $entries[2]->id);
		$lastId = $reader->getLastId();
		
		// Third request.
		$reader = new TimelineReader($c, $idx, 0, $lastId);
		$entries = $reader->read(3);
		$this->assertEquals(3, Count($entries));
		$this->assertEquals(14, $entries[0]->id);
		$this->assertEquals(13, $entries[1]->id);
		$this->assertEquals(12, $entries[2]->id);
		$lastId = $reader->getLastId();
		
		// Fourth request.
		$reader = new TimelineReader($c, $idx, 0, $lastId);
		$entries = $reader->read(40);
		$this->assertEquals(11, Count($entries));
		$this->assertEquals(11, $entries[0]->id);
	}
}