<?php

namespace Chronos;

class UserBucketEntry extends Entry
{
	/** @var null|Carbon */
	public $created_at = null;
	
	/** @var null|Carbon */
	public $deleted_at = null;
	
	/** @var string */
	public $email = '';
	
	/** @var int */
	public $id = 0;
	
	/** @var null|mixed */
	public $payload = null;
	
	/** @var int */
	public $shard = 0;
	
	/** @var null|Carbon */
	public $updated_at = null;
	
	/** @var int */
	public $version = 1;
	
	/**
	 * 
	 * @param int $id
	 * @param string $email
	 * @param null|mixed $payload
	 * @param int $shard
	 * @param int $version
	 */
	public function __construct($id = 0, $email = '', $payload = null, $shard = 0, $version = 1)
	{
		$this->id = $id;
		$this->email = $email;
		$this->payload = $payload;
		$this->shard = $shard;
		$this->version = $version;
	}
}