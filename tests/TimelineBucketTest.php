<?php

use Chronos\Chronos;
use Chronos\TimelineBucketEntry;

class TimelineBucketTest extends TestFixture
{
	private $bucketId = 1;
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
	
    public function test01CreateTimelineBucket()
    {
		$c = self::getChronos();
		
		// Create bucket.
		$b = $c->createBucket(Chronos::BUCKET_TYPE_TIMELINE, $this->shardId, $this->bucketId);
		$this->assertEquals(get_class($b), 'Chronos\TimelineBucket');
		
		// Try creating same bucket again.
		$this->assertFalse($c->createBucket(Chronos::BUCKET_TYPE_TIMELINE, $this->shardId, $this->bucketId));
    }

    public function test02DropTimelineBucket()
    {
        $c = self::getChronos();
				
		// Drop User bucket.
		$this->assertTrue($c->dropBucket(Chronos::BUCKET_TYPE_TIMELINE, $this->shardId, $this->bucketId));
		
		// Drop User bucket again.
		$this->assertFalse($c->dropBucket(Chronos::BUCKET_TYPE_TIMELINE, $this->shardId, $this->bucketId));
    }
	
	public function test03GetTimelineBucket()
	{
		$c = self::getChronos();
		
		// Get User bucket (does not check its existance).
		$b = $c->getBucket(Chronos::BUCKET_TYPE_TIMELINE, $this->shardId, $this->bucketId);
		$this->assertEquals(get_class($b), 'Chronos\TimelineBucket');
	}

	public function test04GetPutDeleteTimeline()
	{
		$c = self::getChronos();
		
		// Create bucket.
		$b = $c->createBucket(Chronos::BUCKET_TYPE_TIMELINE, $this->shardId, $this->bucketId);
		$this->assertEquals(get_class($b), 'Chronos\TimelineBucket');
		
		// Get non existing entry.
		$this->assertFalse($b->get(1));
		
		// Put new entry,
		$e = new TimelineBucketEntry(0, 'payload', 1);
		$retVal = $b->put($e);
		$this->assertEquals(get_class($retVal), 'Chronos\TimelineBucketEntry');
		$this->assertEquals($e->id, 1);
		
		// Get entry.
		$e = $b->get(1);
		$this->assertEquals(get_class($e), 'Chronos\TimelineBucketEntry');
		$this->assertEquals($e->id, 1);
		$this->assertEquals($e->payload, 'payload');
		$this->assertEquals($e->version, 1);
		
		// Update entry.
		$e->payload = 'payload2';
		$e->version = 2;
		$retVal = $b->put($e);
		$this->assertEquals(get_class($retVal), 'Chronos\TimelineBucketEntry');
		
		// Get updated entry.
		$e = $b->get(1);
		$this->assertEquals(get_class($e), 'Chronos\TimelineBucketEntry');
		$this->assertEquals($e->id, 1);
		$this->assertEquals($e->payload, 'payload2');
		$this->assertEquals($e->version, 2);
		
		// Delete entry.
		$this->assertTrue($b->delete(1));
		
		// Delete entry again.
		$this->assertFalse($b->delete(1));
		
		// Drop User bucket.
		$this->assertTrue($c->dropBucket(Chronos::BUCKET_TYPE_TIMELINE, $this->shardId, $this->bucketId));
	}
	
	public function test05ScanTimeline()
	{
		$c = self::getChronos();
		
		// Create bucket.
		$b = $c->createBucket(Chronos::BUCKET_TYPE_TIMELINE, $this->shardId, $this->bucketId);
		$this->assertEquals(get_class($b), 'Chronos\TimelineBucket');
		
		// Add a list of entries,
		$e1 = new TimelineBucketEntry(0, 'payload1', 1);
		$b->put($e1);
		$e2 = new TimelineBucketEntry(0, 'payload2', 1);
		$b->put($e2);
		$e3 = new TimelineBucketEntry(0, 'payload3', 1);
		$b->put($e3);
		$e4 = new TimelineBucketEntry(0, 'payload4', 1);
		$b->put($e4);
		
		// Get first two entries.
		$entries = $b->scan(0, 2);
		$this->assertEquals($entries[0]->id, $e1->id);
		$this->assertEquals($entries[1]->id, $e2->id);
		$this->assertEquals(count($entries), 2);
		
		// Get next two entries.
		$entries = $b->scan(2, 2);
		$this->assertEquals($entries[0]->id, $e3->id);
		$this->assertEquals($entries[1]->id, $e4->id);
		$this->assertEquals(count($entries), 2);
	
		// Get two entries (reverse).
		$entries = $b->scan(0, 2, true);
		$this->assertEquals($entries[0]->id, $e4->id);
		$this->assertEquals($entries[1]->id, $e3->id);
		$this->assertEquals(count($entries), 2);
		
		// Get next two entries (reverse).
		$entries = $b->scan(2, 2, true);
		$this->assertEquals($entries[0]->id, $e2->id);
		$this->assertEquals($entries[1]->id, $e1->id);
		$this->assertEquals(count($entries), 2);
		
		// Drop User bucket.
		$this->assertTrue($c->dropBucket(Chronos::BUCKET_TYPE_TIMELINE, $this->shardId, $this->bucketId));
	}
}
