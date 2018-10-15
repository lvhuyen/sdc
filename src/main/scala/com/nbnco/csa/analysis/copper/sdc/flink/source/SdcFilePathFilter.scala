package com.nbnco.csa.analysis.copper.sdc.flink.source

import java.util.{Date, TimeZone}

import org.apache.flink.api.common.io.FilePathFilter
import org.apache.flink.core.fs.Path
import org.slf4j.LoggerFactory

/**
  * Created by Huyen on 9/9/18.
  */

object SdcFilePathFilter {
	private val LOG = LoggerFactory.getLogger(classOf[SdcFilePathFilter])
}

class SdcFilePathFilter(lookBackPeriod: Long) extends FilePathFilter {
	private val TIME_FORMAT = new java.text.SimpleDateFormat("yyyyMMdd HHmm")
	TIME_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"))

	override def filterPath(filePath: Path): Boolean = {
		filePath == null ||
				filePath.getName.startsWith(".") ||
				filePath.getName.startsWith("_") ||
				filePath.getName.contains(FilePathFilter.HADOOP_COPYING) ||
						!(filePath.getName.endsWith(".tar.gz") ||
									filePath.getName.matches("""^\d{8}$""") ||
									(filePath.getName.matches("""^\d{4}$""") && {
										try {
											this.TIME_FORMAT.parse(s"${filePath.getParent.getName} ${filePath.getName}").getTime >
													new Date().getTime - lookBackPeriod
										} catch {
											case _: java.text.ParseException => false
											case e: Throwable =>
												SdcFilePathFilter.LOG.warn("Unknown exception happens while checking folder eligibility: {}\n {}", Array(filePath.getPath, e.getStackTrace))
												false
										}
									}))
	}
}

//				{
//					try {
//						val fileStatus = filePath.getFileSystem.getFileStatus(filePath)
//						if (!fileStatus.isDir) {
//							fileStatus.getLen == 0
//						} else {
//							(filePath.depth() == homeDepth + 2) && {
//									val paths = filePath.getPath.split('/')
//									SdcFilePathFilter.TIME_FORMAT.parse(s"${paths(paths.length - 2)} ${paths(paths.length - 1)}").getTime <
//											new Date().getTime - lookBackPeriod
//							}
//						}
//					} catch {
//						case _: java.text.ParseException =>
//							SdcFilePathFilter.LOG.warn("Invalid path format: {}. Still read.", filePath.getPath)
//							false
//						case _: java.io.IOException =>
//							SdcFilePathFilter.LOG.warn("Error listing file: {}. Ignored.", filePath.getPath)
//							true
//								case e: Throwable =>
//									SdcFilePathFilter.LOG.warn("Unknown exception happens while checking folder eligibility: {}", e.getStackTrace)
//									true
//					}
//				}
