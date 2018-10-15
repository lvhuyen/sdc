package com.nbnco.csa.analysis.copper.sdc.utils

/**
  * Created by Huyen on 9/8/18.
  */
final case class InvalidDataException(private val message: String = "",
                                      private val cause: Throwable = None.orNull)
		extends Exception(message, cause)