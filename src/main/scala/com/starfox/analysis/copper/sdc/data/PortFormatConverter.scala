package com.starfox.analysis.copper.sdc.data

import com.starfox.analysis.copper.sdc.utils.InvalidPortFormatException

import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex

object PortFormatConverter {
	/***
	  * Each Port_Pattern is one tuple of
	  *   - a regex to parse the raw port string, and
	  *   - a (String) => String function to convert the raw port into the desired format.
	  *   	Any manipulation to the port value can be done in this function
	  */

	val PATTERNS: Array[(String, (Regex, String) => String)] = Array (
		("""R\d\.S\d\.LT\d{1,2}\.P\d{1,2}$""", (_, rawPort) => rawPort),
		("""R\d\.S\d\.LT\d{1,2}\.P\d{1,2}\.ITF.*""", (_, rawPort) => rawPort.split(".ITF")(0)),
		("""\/.*\/rackNr=(\d)\/subrackNr=(\d)\/slotNr=(\d{1,2})\/portNr=(\d{1,2}).*""", (regex, rawPort) => {
			rawPort match {
				case regex(r, s, lt, p) => s"R$r.S$s.LT${lt.toInt - 2}.P$p"
				case _ => throw InvalidPortFormatException(rawPort)
			}
		})
	)

	def convertPortAndManipulateValue[DataType](raw: Map[String, DataType], valueManipulator: DataType => DataType = (raw: DataType) => raw): Try[Map[String, DataType]] = {
		if (raw.isEmpty)
			Success(raw)
		else {
			PATTERNS.find(pattern => raw.head._1.matches(pattern._1)) match {
				case Some((regex, converter)) => Success(raw.map{case (k, v) => (converter(regex.r, k), valueManipulator(v))})
				case _ => Failure(InvalidPortFormatException("Unknown Port Pattern"))
			}
		}
	}
}