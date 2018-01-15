<?php

namespace Chronos;

class KeyBucketEntry extends Entry
{
	/** @var null|Carbon */
	public $created_at = null;
	
	/** @var string */
	public $key = '';
	
	/** @var null|mixed */
	public $payload = null;
	
	/** @var null|Carbon */
	public $updated_at = null;
	
	/** @var int */
	public $version = 1;
	
	/**
	 * 
	 * @param string $key
	 * @param null|mixed $payload
	 * @param int $version
	 */
	public function __construct($key = '', $payload = null, $version = 1)
	{
		$this->key = $key;
		$this->payload = $payload;
		$this->version = $version;
	}
}