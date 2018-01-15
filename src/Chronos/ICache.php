<?php

namespace Chronos;

/**
 * Interface that any cache driver need to implement. Objects passed to the
 * Chronos options that do not implement this interface will be ignored.
 */
interface ICache
{
	public function clear(&$meta = []);
	public function delete(&$meta = []);
	public function get(&$meta = []);
	public function put(&$meta = [], &$object);
}