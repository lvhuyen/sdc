package com.nbnco.csa.analysis.copper.sdc

/**
  * Created by Huyen on 23/9/18.
  */
package object data {
	val PORT_PATTERNS = Array (
		"""R(\d)\.S(\d)\.LT(\d{1,2})\.P(\d{1,2})""".r,
		"""\/.*\/rackNr=(\d)\/subrackNr=(\d)\/slotNr=(\d{1,2})\/portNr=(\d{1,2}).*""".r
	)
	val PORT_PATTERN_UNKNOWN = -1
	val PORT_PATTERN_DEFAULT = 0
}
