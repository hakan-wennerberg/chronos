<?php
require __DIR__.'/../vendor/autoload.php';

use Chronos\Chronos;

class TestFixture extends \PHPUnit_Framework_TestCase
{
	private static $chronos = null;
	
	protected static function getChronos($cacheObj = null)
	{
		if (is_null(self::$chronos)) {
			if (is_null($cacheObj)) {
				self::$chronos = new Chronos();
			} else {
				self::$chronos = new Chronos($cacheObj);
			}
			
			self::$chronos->registerServer([
				'id' => (int)$GLOBALS['SERVER1_ID'],
				'host' => $GLOBALS['SERVER1_HOST'],
				'password' => $GLOBALS['SERVER1_PASSWORD'],
				'shards' => [],
				'username' => $GLOBALS['SERVER1_USERNAME']
			]);
		}
		return self::$chronos;
	}
}
