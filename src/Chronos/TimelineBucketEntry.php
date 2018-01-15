<?php

namespace Chronos;

class TimelineBucketEntry extends Entry
{
	/** @var null|Carbon */
	public $created_at = null;
	
	/** @var int */
	public $id = 0;
	
	/** @var null|mixed */
	public $payload = null;
	
	/** @var null|Carbon */
	public $updated_at = null;
	
	/** @var int */
	public $version = 1;
	
	/**
	 * 
	 * @param int $id
	 * @param null|mixed $payload
	 * @param int $version
	 */
	public function __construct($id = 0, $payload = null, $version = 1)
	{
		$this->id = $id;
		$this->payload = $payload;
		$this->version = $version;
	}
}