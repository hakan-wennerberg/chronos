<?php

use Chronos\Chronos;
use Chronos\UserBucketEntry;

class UsersBucketTest extends TestFixture
{
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
	
    public function test01CreateUserBucket()
    {
		$c = self::getChronos();
		
		// Create bucket.
		$b = $c->createBucket(Chronos::BUCKET_TYPE_USER, $this->shardId);
		$this->assertEquals(get_class($b), 'Chronos\UserBucket');
		
		// Try creating same bucket again.
		$this->assertFalse($c->createBucket(Chronos::BUCKET_TYPE_USER, $this->shardId));
    }

    public function test02DropUserBucket()
    {
        $c = self::getChronos();
				
		// Drop User bucket.
		$this->assertTrue($c->dropBucket(Chronos::BUCKET_TYPE_USER, $this->shardId));
		
		// Drop User bucket again.
		$this->assertFalse($c->dropBucket(Chronos::BUCKET_TYPE_USER, $this->shardId));
    }
	
	public function test03GetUserBucket()
	{
		$c = self::getChronos();
		
		// Get User bucket (does not check its existance).
		$b = $c->getBucket(Chronos::BUCKET_TYPE_USER, $this->shardId);
		$this->assertEquals(get_class($b), 'Chronos\UserBucket');
	}

	public function test04GetPutDeleteUser()
	{
		$c = self::getChronos();
		
		// Create bucket.
		$b = $c->createBucket(Chronos::BUCKET_TYPE_USER, $this->shardId);
		$this->assertEquals(get_class($b), 'Chronos\UserBucket');
		
		// Get non existing entry.
		$this->assertFalse($b->get(1));
		
		// Put new entry,
		$e = new UserBucketEntry(0, 'an@email.addr', 'payload', 1, 1);
		$retVal = $b->put($e);
		$this->assertEquals(get_class($retVal), 'Chronos\UserBucketEntry');
		$this->assertEquals($e->id, 1);
		
		// Get entry.
		$e = $b->get(1);
		$this->assertEquals(get_class($e), 'Chronos\UserBucketEntry');
		$this->assertEquals($e->id, 1);
		$this->assertEquals($e->email, 'an@email.addr');
		$this->assertEquals($e->payload, 'payload');
		$this->assertEquals($e->shard, 1);
		$this->assertEquals($e->version, 1);
		
		// Update entry.
		$e->email = 'new@email.addr';
		$e->shard = 2;
		$e->payload = 'payload2';
		$e->version = 2;
		$retVal = $b->put($e);
		$this->assertEquals(get_class($retVal), 'Chronos\UserBucketEntry');
		
		// Get updated entry.
		$e = $b->get(1);
		$this->assertEquals(get_class($e), 'Chronos\UserBucketEntry');
		$this->assertEquals($e->id, 1);
		$this->assertEquals($e->email, 'new@email.addr');
		$this->assertEquals($e->payload, 'payload2');
		$this->assertEquals($e->shard, 2);
		$this->assertEquals($e->version, 2);
		
		// Delete entry.
		$this->assertTrue($b->delete(1));
		
		// Delete entry again.
		$this->assertFalse($b->delete(1));
		
		// Drop User bucket.
		$this->assertTrue($c->dropBucket(Chronos::BUCKET_TYPE_USER, $this->shardId));
	}
	
	public function test05ScanUsers()
	{
		$c = self::getChronos();
		
		// Create bucket.
		$b = $c->createBucket(Chronos::BUCKET_TYPE_USER, $this->shardId);
		$this->assertEquals(get_class($b), 'Chronos\UserBucket');
		
		// Add a list of entries,
		$e1 = new UserBucketEntry(0, 'an@email1.addr', 'payload1', 1, 1);
		$b->put($e1);
		$e2 = new UserBucketEntry(0, 'an@email2.addr', 'payload2', 1, 1);
		$b->put($e2);
		$e3 = new UserBucketEntry(0, 'an@email3.addr', 'payload3', 1, 1);
		$b->put($e3);
		$e4 = new UserBucketEntry(0, 'an@email4.addr', 'payload4', 1, 1);
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
		$this->assertTrue($c->dropBucket(Chronos::BUCKET_TYPE_USER, $this->shardId));
	}
}
