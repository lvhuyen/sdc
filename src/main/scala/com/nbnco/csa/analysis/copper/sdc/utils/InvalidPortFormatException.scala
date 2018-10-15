package com.nbnco.csa.analysis.copper.sdc.utils

/**
  * Created by Huyen on 9/8/18.
  */
final case class InvalidPortFormatException(private val message: String = "Unknown dslam port format.",
                                            private val cause: Throwable = None.orNull)
		extends IllegalArgumentException(message, cause)