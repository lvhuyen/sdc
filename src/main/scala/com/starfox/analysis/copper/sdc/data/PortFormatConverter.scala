package com.starfox.analysis.copper.sdc.data

import com.starfox.analysis.copper.sdc.flink.source.SdcTarInputFormat
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

	type ItemValueType = Int
	val SINGLE: ItemValueType = 1
	val LIST: ItemValueType = 2
	val CONCATENATED: ItemValueType = 3

	private type PATTERN = (String, ItemValueType, (Regex, String) => String)

	val PATTERNS: Array[PATTERN] = Array (
		("""R\d\.S\d\.LT\d{1,2}\.P\d{1,2}$""", SINGLE, (_, rawPort) => rawPort),
		("""R\d\.S\d\.LT\d{1,2}\.P\d{1,2}\.ITF.*""", CONCATENATED, (_, rawPort) => rawPort.split(".ITF")(0)),
		("""\/.*\/rackNr=(\d)\/subrackNr=(\d)\/slotNr=(\d{1,2})\/portNr=(\d{1,2}).*""", SINGLE
				, (regex, rawPort) => {
			rawPort match {
				case regex(r, s, lt, p) => s"R$r.S$s.LT${lt.toInt - 2}.P$p"
				case _ => throw InvalidPortFormatException(rawPort)
			}
		})
	)

	def convertPortAndManipulateValue(fileName: String, raw: List[(String, String)],
	                                            valueManipulator: String => String = (raw: String) => raw): Try[Map[String, String]] = {
		if (raw.isEmpty)
			Success(Map.empty[String, String])
		else {
			PATTERNS.find(pattern => raw.head._1.matches(pattern._1)) match {
				case Some((regex, valueType, converter)) =>
					val lst_records = raw.map{ case (k, v) => (converter(regex.r, k), valueManipulator(v)) }
					Success(valueType match {
						case SINGLE =>
							val records = lst_records.toMap
							if (records.size != lst_records.size) {
								val duplicated: Map[String, List[String]] = lst_records
										.groupBy(_._1).mapValues(l => l.map(_._2))
										.filter(_._2.size > 1)
								SdcTarInputFormat.LOG.warn(s"File $fileName has records with duplicated id (${records.size} vs ${lst_records.size}): $duplicated")
							}
							records
						case CONCATENATED =>
							lst_records.groupBy(_._1).mapValues(l => l.map(_._2).mkString("|"))
					})

				case _ => Failure(InvalidPortFormatException("Unknown Port Pattern"))
			}
		}
	}
}