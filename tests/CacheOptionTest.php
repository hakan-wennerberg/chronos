<?php

use Chronos\Chronos;
use Chronos\ArrayCache;
use Chronos\TimelineBucketEntry;

class CacheOptionTest extends TestFixture
{
	public function testCacheOption()
	{
		$c = new Chronos();
		$this->assertFalse($c->getOption('cache'));
		
		$c = new Chronos([
			'cache' => new ArrayCache()
		]);
		$this->assertEquals(get_class($c->getOption('cache')), 'Chronos\ArrayCache');
		
		$c = new Chronos([
			'cache' => new TimelineBucketEntry()
		]);
		$this->assertFalse($c->getOption('cache'));
	}
}