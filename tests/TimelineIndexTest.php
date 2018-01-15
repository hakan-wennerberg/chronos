<?php

use Chronos\Chronos;
use Chronos\TimelineIndexEntry;

class TimelineIndexTest extends TestFixture
{
	private $indexId = 2;
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
	
    public function test01CreateTimelineIndex()
    {
		$c = self::getChronos();
		
		// Create bucket.
		$b = $c->createIndex(Chronos::INDEX_TYPE_TIMELINE, $this->shardId, $this->indexId);
		$this->assertEquals(get_class($b), 'Chronos\TimelineIndex');
		
		// Try creating same bucket again.
		$this->assertFalse($c->createIndex(Chronos::INDEX_TYPE_TIMELINE, 1, $this->indexId));
    }

    public function test02DropTimelineIndex()
    {
        $c = self::getChronos();
				
		// Drop User bucket.
		$this->assertTrue($c->dropIndex(Chronos::INDEX_TYPE_TIMELINE, $this->shardId, $this->indexId));
		
		// Drop User bucket again.
		$this->assertFalse($c->dropIndex(Chronos::INDEX_TYPE_TIMELINE, $this->shardId, $this->indexId));
    }
	
	public function test03GetTimelineIndex()
	{
		$c = self::getChronos();
		
		// Get User bucket (does not check its existance).
		$b = $c->getIndex(Chronos::INDEX_TYPE_TIMELINE, $this->shardId, $this->indexId);
		$this->assertEquals(get_class($b), 'Chronos\TimelineIndex');
	}

	public function test04GetPutDeleteTimelineIndex()
	{
		$c = self::getChronos();
		
		// Create bucket.
		$b = $c->createIndex(Chronos::INDEX_TYPE_TIMELINE, $this->shardId, $this->indexId);
		$this->assertEquals(get_class($b), 'Chronos\TimelineIndex');
		
		// Get non existing entry.
		$this->assertFalse($b->get(1));
		
		// Put new entry,
		$e = new TimelineIndexEntry(0, 5, 10);
		$retVal = $b->put($e);
		$this->assertEquals(get_class($retVal), 'Chronos\TimelineIndexEntry');
		$this->assertEquals($e->id, 1);
		
		// Get entry.
		$e = $b->get(1);
		$this->assertEquals(get_class($e), 'Chronos\TimelineIndexEntry');
		$this->assertEquals($e->id, 1);
		$this->assertEquals($e->a, 5);
		$this->assertEquals($e->b, 10);
		
		// Update entry.
		$e->a = 5; // Changing 5 to something else would create a new entry with same id due to compund primary key.
		$e->b = 11;
		$retVal = $b->put($e);
		$this->assertEquals(get_class($retVal), 'Chronos\TimelineIndexEntry');
				
		// Get updated entry.
		$e = $b->get(1);
		$this->assertEquals(get_class($e), 'Chronos\TimelineIndexEntry');
		$this->assertEquals(1, $e->id);
		$this->assertEquals(5, $e->a);
		$this->assertEquals(11, $e->b);
		
		// Update entry to get "duplicate" id.
		$e->a = 6;
		$b->put($e);
		
		// Get entry by compound key.
		$e = $b->getByIdA(1, 5);
		$this->assertEquals(get_class($e), 'Chronos\TimelineIndexEntry');
		$this->assertEquals(1, $e->id);
		$this->assertEquals(5, $e->a);
		
		$e = $b->getByIdA(1, 6);
		$this->assertEquals(get_class($e), 'Chronos\TimelineIndexEntry');
		$this->assertEquals(1, $e->id);
		$this->assertEquals(6, $e->a);
		
		// Delete all entries with id 1 (both currently in index).
		$this->assertTrue($b->delete(1));
		
		// No items left to delete.
		$this->assertFalse($b->delete(1));
		
		// Test deleting by A.
		$e1 = new TimelineIndexEntry(0, 50, 10);
		$b->put($e1);
		$e2 = new TimelineIndexEntry(0, 50, 11);
		$b->put($e2);
		$e3 = new TimelineIndexEntry(0, 60, 12);
		$b->put($e3);
		
		$this->assertTrue($b->deleteByA(50));
		$this->assertFalse($b->get(50));
		
		// Test delete by compound key.
		$e = $b->getByIdA($e3->id, 60);
		$this->assertEquals(get_class($e), 'Chronos\TimelineIndexEntry');
		$this->assertTrue($b->deleteByIdA($e3->id, 60));
		$this->assertFalse($b->getByIdA($e3->id, 60));
		
		// Drop User bucket.
		$this->assertTrue($c->dropIndex(Chronos::INDEX_TYPE_TIMELINE, $this->shardId, $this->indexId));
	}
	
	public function test05ScanTimeline()
	{
		$c = self::getChronos();
		
		// Create bucket.
		$b = $c->createIndex(Chronos::INDEX_TYPE_TIMELINE, $this->shardId, $this->indexId);
		$this->assertEquals(get_class($b), 'Chronos\TimelineIndex');
		
		// Add a list of entries,
		$e1 = new TimelineIndexEntry(0, 5, 10);
		$b->put($e1);
		$e2 = new TimelineIndexEntry(0, 6, 11);
		$b->put($e2);
		$e3 = new TimelineIndexEntry(0, 7, 12);
		$b->put($e3);
		$e4 = new TimelineIndexEntry(0, 8, 13);
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
		$this->assertTrue($c->dropIndex(Chronos::INDEX_TYPE_TIMELINE, $this->shardId, $this->indexId));
	}
	
	public function test06ScanTimelineA()
	{
		$c = self::getChronos();
		
		// Create bucket.
		$b = $c->createIndex(Chronos::INDEX_TYPE_TIMELINE, $this->shardId, $this->indexId);
		$this->assertEquals(get_class($b), 'Chronos\TimelineIndex');
		
		// Add a list of entries,
		$e1 = new TimelineIndexEntry(0, 1, 10);
		$b->put($e1);
		$e2 = new TimelineIndexEntry(0, 1, 11);
		$b->put($e2);
		$e3 = new TimelineIndexEntry(0, 1, 12);
		$b->put($e3);
		$e4 = new TimelineIndexEntry(0, 2, 13);
		$b->put($e4);
		$e5 = new TimelineIndexEntry(0, 1, 13);
		$b->put($e5);
		
		// Get first two entries.
		$entries = $b->scanByA(1, 0, 2);
		$this->assertEquals($entries[0]->id, $e1->id);
		$this->assertEquals($entries[1]->id, $e2->id);
		$this->assertEquals(count($entries), 2);
		
		// Get next two entries.
		$entries = $b->scanByA(1, 2, 2);
		$this->assertEquals($entries[0]->id, $e3->id);
		$this->assertEquals($entries[1]->id, $e5->id);
		$this->assertEquals(count($entries), 2);
	
		// Get two entries (reverse).
		$entries = $b->scanByA(1, 0, 2, true);
		$this->assertEquals($entries[0]->id, $e5->id);
		$this->assertEquals($entries[1]->id, $e3->id);
		$this->assertEquals(count($entries), 2);
		
		// Get next two entries (reverse).
		$entries = $b->scanByA(1, 2, 2, true);
		$this->assertEquals($entries[0]->id, $e2->id);
		$this->assertEquals($entries[1]->id, $e1->id);
		$this->assertEquals(count($entries), 2);
		
		// Drop User bucket.
		$this->assertTrue($c->dropIndex(Chronos::INDEX_TYPE_TIMELINE, $this->shardId, $this->indexId));
	}
}
