package com.nbnco.csa.analysis.copper.sdc

/**
  * Created by Huyen on 23/9/18.
  */
package object data {
	/***
		* Each Port_Pattern is one tuple of
		*   - a regex to parse the raw port string, and
		*   - a function of (r,s,lt,p) to reconstruct the port to desired format.
		*     This function would help in case there is a need to manipulate value of the raw numbers
		*/

	val PORT_PATTERNS = Array (
		("""R(\d)\.S(\d)\.LT(\d{1,2})\.P(\d{1,2})""".r,
			(r: String, s: String, lt: String, p: String) => s"R$r.S$s.LT$lt.P$p"),

		("""\/.*\/rackNr=(\d)\/subrackNr=(\d)\/slotNr=(\d{1,2})\/portNr=(\d{1,2}).*""".r,
			(r: String, s: String, lt: String, p: String) => s"R$r.S$s.LT${lt.toInt - 2}.P$p")
	)
	val PORT_PATTERN_UNKNOWN = -1
	val PORT_PATTERN_DEFAULT = 0
}
