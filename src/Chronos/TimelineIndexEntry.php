<?php

namespace Chronos;

class TimelineIndexEntry
{
	/** @var int */
	public $a = null;
	
	/** @var int */
	public $b = null;
	
	/** @var null|Carbon */
	public $created_at = null;
	
	/** @var int */
	public $id = 0;
	
	/**
	 * 
	 * @param int $id
	 * @param int $a
	 * @param int $b
	 */
	public function __construct($id = 0, $a = null, $b = null)
	{
		$this->id = $id;
		$this->a = $a;
		$this->b = $b;
	}
}