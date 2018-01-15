<?php

namespace Chronos;


/**
 * Most functions return false on failure. To investigate the actual error use
 * the functions in this trait.
 */
trait Error
{
	/** @var array List of errors in the order they were raised (newest last) */
	public $errors = [];
	
	/**
	 * Clear all errors from the current index/bucket object.
	 */
	public function clearErrors()
	{
		$this->errors = [];
	}
	
	/**
	 * Ads an error to the errors list.
	 * 
	 * @param string $code Error code.
	 * @param string $info Error message.
	 */
	private function pushError($code, $info)
	{
		$this->errors[] = [$code, $info];
	}
	
	/**
	 * Gets the latest generated error.
	 * 
	 * @return boolean|array Array [code, info] or false if no errors in list.
	 */
	public function getLatestError()
	{
		if (count($this->errors) > 0) {
			return $this->errors[count($this->errors) - 1];
		} else {
			return false;
		}
	}
}
