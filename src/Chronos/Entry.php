<?php

namespace Chronos;


class Entry {
	
	public function loaded()
	{
	}
	
	public function loading()
	{
	}

	public function saved()
	{
	}
	
	public function saving()
	{
	}
	
	public function __toString() {
		return json_encode($this, JSON_NUMERIC_CHECK);
	}
}
