<?php

namespace Chronos;

/**
 * The transaction trait is used to quickly insert data en masse. Its
 * only transactional for the specific index/bucket and should not be used in a
 * production context.
 */
trait Transaction
{
	private $transactionActive = false;
	
	public function beginTransaction()
	{
		if ($this->transactionActive === false
				&& $this->connection->beginTransaction() === true) {
			$this->transactionActive = true;
			return true;
		} else {
			return false;
		}
	}
	
	public function commitTransaction()
	{
		if ($this->transactionActive === true
				&& $this->connection->commit() === true) {
			$this->transactionActive = false;
			return true;
		} else {
			return false;
		}
	}
	
	public function rollbackTransaction()
	{
		if ($this->transactionActive === true
				&& $this->connection->rollBack() === true) {
			$this->transactionActive = false;
			return true;
		} else {
			return false;
		}
	}
}