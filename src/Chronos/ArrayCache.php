<?php

namespace Chronos;

use Chronos\ICache;

class ArrayCache implements ICache
{
	private $object = [];
	
	public function clear(&$meta = [])
	{
		$this->object = [];
		return true;
	}

	public function delete(&$meta = [])
	{
		if (isset($this->object[$meta['cacheKey']])) {
			unset($this->object[$meta['cacheKey']]);
			return true;
		} else {
			return false;
		}
	}

	public function get(&$meta = [])
	{
		if (isset($this->object[$meta['cacheKey']])) {
			return $this->object[$meta['cacheKey']];
		} else {
			return false;
		}
	}

	public function put(&$meta = [], &$object)
	{
		$this->object[$meta['cacheKey']] = $object;
		return true;
	}
}