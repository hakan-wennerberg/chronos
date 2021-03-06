<?php

use Chronos\Chronos;
use Chronos\KeyBucketEntry;

class KeyBucketTest extends TestFixture
{
	private $bucketId = 3;
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
	
    public function test01CreateKeyBucket()
    {
		$c = self::getChronos();
		
		// Create bucket.
		$b = $c->createBucket(Chronos::BUCKET_TYPE_KEY, $this->shardId, $this->bucketId);
		$this->assertEquals(get_class($b), 'Chronos\KeyBucket');
		
		// Try creating same bucket again.
		$this->assertFalse($c->createBucket(Chronos::BUCKET_TYPE_KEY, $this->shardId, $this->bucketId));
    }

    public function test02DropKeyBucket()
    {
        $c = self::getChronos();
				
		// Drop User bucket.
		$this->assertTrue($c->dropBucket(Chronos::BUCKET_TYPE_KEY, $this->shardId, $this->bucketId));
		
		// Drop User bucket again.
		$this->assertFalse($c->dropBucket(Chronos::BUCKET_TYPE_KEY, $this->shardId, $this->bucketId));
    }
	
	public function test03GetKeyBucket()
	{
		$c = self::getChronos();
		
		// Get User bucket (does not check its existance).
		$b = $c->getBucket(Chronos::BUCKET_TYPE_KEY, $this->shardId, $this->bucketId);
		$this->assertEquals(get_class($b), 'Chronos\KeyBucket');
	}

	public function test04GetPutDeleteKey()
	{
		$c = self::getChronos();
		
		// Create bucket.
		$b = $c->createBucket(Chronos::BUCKET_TYPE_KEY, $this->shardId, $this->bucketId);
		$this->assertEquals(get_class($b), 'Chronos\KeyBucket');
		
		// Get non existing entry.
		$this->assertFalse($b->get('myKey'));
		
		// Put new entry,
		$e = new KeyBucketEntry('myKey', 'payload', 1);
		$retVal = $b->put($e);
		$this->assertEquals(get_class($retVal), 'Chronos\KeyBucketEntry');
		$this->assertEquals($e->key, 'myKey');
		
		// Get entry.
		$e = $b->get('myKey');
		$this->assertEquals(get_class($e), 'Chronos\KeyBucketEntry');
		$this->assertEquals($e->key, 'myKey');
		$this->assertEquals($e->payload, 'payload');
		$this->assertEquals($e->version, 1);
		
		// Update entry.
		$e->payload = 'payload2';
		$e->version = 2;
		$retVal = $b->put($e);
		$this->assertEquals(get_class($retVal), 'Chronos\KeyBucketEntry');
		
		// Get updated entry.
		$e = $b->get($e->key);
		$this->assertEquals(get_class($e), 'Chronos\KeyBucketEntry');
		$this->assertEquals($e->key, 'myKey');
		$this->assertEquals($e->payload, 'payload2');
		$this->assertEquals($e->version, 2);
		
		// Delete entry.
		$this->assertTrue($b->delete('myKey'));
		
		// Delete entry again.
		$this->assertFalse($b->delete('myKey'));
		
		// Drop User bucket.
		$this->assertTrue($c->dropBucket(Chronos::BUCKET_TYPE_KEY, $this->shardId, $this->bucketId));
	}
	
	public function test05ScanKeys()
	{
		$c = self::getChronos();
		
		// Create bucket.
		$b = $c->createBucket(Chronos::BUCKET_TYPE_KEY, $this->shardId, $this->bucketId);
		$this->assertEquals(get_class($b), 'Chronos\KeyBucket');
		
		// Add a list of entries,
		$e1 = new KeyBucketEntry('one', 'payload1', 1);
		$b->put($e1);
		$e2 = new KeyBucketEntry('two', 'payload2', 1);
		$b->put($e2);
		$e3 = new KeyBucketEntry('three', 'payload3', 1);
		$b->put($e3);
		$e4 = new KeyBucketEntry('four', 'payload4', 1);
		$b->put($e4);
		$keys = [];
		$keys[$e1->key] = true;
		$keys[$e2->key] = true;
		$keys[$e3->key] = true;
		$keys[$e4->key] = true;
		
		// Get first two entries.
		$entries = $b->scan(0, 2);
		unset($keys[$entries[0]->key]);
		unset($keys[$entries[1]->key]);
		$this->assertEquals(count($entries), 2);
		$this->assertEquals(count($keys), 2);
		
		// Get next two entries.
		$entries = $b->scan(2, 2);
		unset($keys[$entries[0]->key]);
		unset($keys[$entries[1]->key]);
		$this->assertEquals(count($entries), 2);
		$this->assertEquals(count($keys), 0);
		
		// Drop User bucket.
		$this->assertTrue($c->dropBucket(Chronos::BUCKET_TYPE_KEY, $this->shardId, $this->bucketId));
	}
}
