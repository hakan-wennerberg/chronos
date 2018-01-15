<?php

use Chronos\TimelineIndexId;

class TimelineIndexIdTest extends TestFixture
{
	public function test01PackRaw()
	{
		// Alignment.
		$this->assertEquals(2252074691592193, TimelineIndexId::packRaw(1, 1, 1));
		
		// Overflow shard id (max 12 bits kept).
		$this->assertEquals(9221120237041090560, TimelineIndexId::packRaw(0x7FFFFFFFFFFFFFFF, 0, 0));
		
		// Overflow bucket id (max 13 bits kept).
		$this->assertEquals(2251524935778304, TimelineIndexId::packRaw(0, 0x7FFFFFFFFFFFFFFF, 0));
		
		// Overflow entry id (max 38 bits kept).
		$this->assertEquals(274877906943, TimelineIndexId::packRaw(0, 0, 0x7FFFFFFFFFFFFFFF));
	}
	
	public function test02Unpack()
	{
		$id = TimelineIndexId::packRaw(1, 2, 3);
		$obj = TimelineIndexId::unpack($id);
		
		$this->assertEquals(1, $obj->shardId);
		$this->assertEquals(2, $obj->bucketId);
		$this->assertEquals(3, $obj->entryId);
		
		// 63 bit, all bits set. Text max values and bit masking.
		$id = TimelineIndexId::packRaw(4095, 8191, 274877906943);
		$obj = TimelineIndexId::unpack($id);
		
		$this->assertEquals(4095, $obj->shardId);
		$this->assertEquals(8191, $obj->bucketId);
		$this->assertEquals(274877906943, $obj->entryId);
	}
	
	public function test03Pack()
	{
		$obj = new TimelineIndexId(1, 2, 3);
		$this->assertEquals(2252349569499139, TimelineIndexId::pack($obj));
	}
	
	public function test04Pack()
	{
		$obj = new TimelineIndexId(1, 2, 3);
		$this->assertEquals(2252349569499139, $obj->p());
	}
}