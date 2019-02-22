package com.nbnco.csa.analysis.copper.sdc.flink.operator

import com.nbnco.csa.analysis.copper.sdc.data._
import scala.math.pow

/**
  * Created by Huyen on 22/02/19.
  */

object CalculatePercentiles {
	/***
	  *
	  * @param techType
	  * @param attenIdx
	  * @param tc4_ds
	  * @param tc4_us
	  * @return: 4 string keys, to be used to get data (Array of percentiles from 0.55% to 99.5% in 3.0%-step)
	  *         from the global percentiles table
	  */
	def buildKeysForGettingPctlsTable(techType: TechType.TechType, attenIdx: Short, tc4_ds: String, tc4_us: String) = {
		techType match {
			case TechType.FTTN =>
				(s"FTTN,${tc4_ds},${attenIdx}",
						s"FTTN,${tc4_us},${attenIdx}",
						s"FTTN,100,${attenIdx}",
						s"FTTN,40,${attenIdx}")
			case TechType.FTTB =>
				(s"FTTB,${tc4_ds},${attenIdx}",
						s"FTTB,${tc4_us},${attenIdx}",
						s"FTTB,50,${attenIdx}",
						s"FTTB,20,${attenIdx}")
			case _ =>
				("", "", "", "")
		}
	}

	// Here below is the calculation of correctedAttNDR, copied from cuperf
	val NO_POWER: Short = -200
	private val FIRST_PERCENTILE = 0.5
	private val PERCENTILE_STEP = 3.0
	private val NUM_PERCENTILES = 34
	private val MAX_DS = 175000
	private val MAX_US = 70000
	private val MAX_TIER_RATE = Map(
		"100" -> 107000,
		"50" ->   54900,
		"25" ->   27950,
		"12" ->   13910,
		"40" ->   39900,
		"20" ->   22500,
		"10" ->   11750,
		"5" ->     6330,
		"1" ->     2050
	)
	private val Tilt_DS = Array(2.18, 1.25, 1.066, 1.053, 1.04, 1.03, 1.025, 1.02, 1.015, 1.012, 1.011, 1.01, 1.009, 1.008, 1.007,
		1.006, 1.005, 1.004, 1.003, 1.002, 1.001, 1, 0.999, 0.998, 0.996, 0.993, 0.989, 0.98, 0.965, 0.948, 0.9225, 0.875, 0.69)
	private val Tilt_US = Array(2.18, 1.25, 1.066, 1.053, 1.04, 1.03, 1.025, 1.02, 1.015, 1.012, 1.011, 1.01, 1.009, 1.008, 1.007,
		1.006, 1.005, 1.004, 1.003, 1.002, 1.001, 1, 0.999, 0.998, 0.996, 0.993, 0.989, 0.98, 0.965, 0.948, 0.9225, 0.875, 0.69)
	private val Peak = Array(
			// ds_max, us_max
			(161322,  60000), // 0 dB
			(161322,  60000), // 1 dB
			(161322,  60000), // 2 dB
			(161322,  60000), // 3 dB
			(161322,  60000), // 4 dB
			(161322,  60000), // 5 dB
			(161322,  60000), // 6 dB
			(161322,  60000), // 7 dB
			(161322,  60000), // 8 dB
			(161322,  60000), // 9 dB
			(161322,  60000), // 10 dB
			(161322,  60000), // 11 dB
			(159983,  60000), // 12 dB
			(158102,  60000), // 13 dB
			(154277,  60000), // 14 dB
			(151727,  60000), // 15 dB
			(148380,  60000), // 16 dB
			(145352,  60000), // 17 dB
			(141718,  60000), // 18 dB
			(138276,  58594), // 19 dB
			(134833,  57188), // 20 dB
			(130594,  55781), // 21 dB
			(126482,  54375), // 22 dB
			(122402,  52969), // 23 dB
			(118099,  51562), // 24 dB
			(113317,  50156), // 25 dB
			(108887,  48750), // 26 dB
			(104424,  47344), // 27 dB
			( 99866,  45938), // 28 dB
			( 95722,  44531), // 29 dB
			( 91036,  43125), // 30 dB
			( 86255,  41719), // 31 dB
			( 81697,  40312), // 32 dB
			( 78095,  38906), // 33 dB
			( 74270,  37500), // 34 dB
			( 71401,  36094), // 35 dB
			( 68851,  34688), // 36 dB
			( 66843,  33281), // 37 dB
			( 65122,  31875), // 38 dB
			( 63687,  30469), // 39 dB
			( 62221,  29062), // 40 dB
			( 60691,  27656), // 41 dB
			( 59161,  26250), // 42 dB
			( 57376,  24844), // 43 dB
			( 55846,  23438), // 44 dB
			( 54443,  22031), // 45 dB
			( 52658,  20625), // 46 dB
			( 50873,  19219), // 47 dB
			( 49088,  17812), // 48 dB
			( 47526,  16406), // 49 dB
			( 46283,  15000), // 50 dB
			( 44944,  14650), // 51 dB
			( 43606,  14300), // 52 dB
			( 42458,  13950), // 53 dB
			( 41311,  13600), // 54 dB
			( 40163,  13250), // 55 dB
			( 39366,  12900), // 56 dB
			( 38569,  12550), // 57 dB
			( 37613,  12200), // 58 dB
			( 36816,  11850), // 59 dB
			( 36019,  11500), // 60 dB
			( 35382,  11150), // 61 dB
			( 34744,  10800), // 62 dB
			( 34107,  10450), // 63 dB
			( 33629,  10100), // 64 dB
			( 32991,   9750), // 65 dB
			( 32354,   9400), // 66 dB
			( 31875,   9050), // 67 dB
			( 31493,   8700), // 68 dB
			( 30855,   8350), // 69 dB
			( 32194,   8000)  // 70 dB
	)

	def getAttenIndex(atten: Float): Short =
		if (atten > 70.0f) 70
		else if (atten < 0.0f) 0
		else atten.round.toShort


	def choose_scale_power (p: Int, doing_us: Boolean) = if (doing_us) Tilt_US(p) else Tilt_DS(p)

	/***
	  *
	  * @param atten_index	: attenuation
	  * @param r			: attainable NDR
	  * @param max			: Maximum speed
	  * @param per			: percentiles
	  * @param doing_us		: is doing Downstream or Upstream
	  * @return
	  */
	def Choose_Percentile (atten_index: Short, r: Int, max: Int, per: Array[JavaFloat], doing_us: Boolean): Double = {
		val d = r

		if (r >= max)
			return 0.0f

		// If zero bit rate, return a null value (can't determine percentile)
		if (r <= 0)
			return NO_POWER

		//- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
		// per[0]=99.5th percentile...
		if (d < per(0)) { // the rate is below the 100-FIRST_PERCENTILE line in table
			// If the 99th percentile is 0, or if rate is below half the 99th, then return 100th
			if ((per(0) <= 0.0) || (d <= (per(0) / 2.0))) return 100.0
			// Determine the proportion between half of 99th and 99th
			val fract = pow((d - (per(0) / 2.0)) / (per(0) / 2.0), choose_scale_power(0, doing_us))
			// Return something between 99th and 100th
			return 100.0 - (FIRST_PERCENTILE * (if (fract > 1.0) 1.0 else if (fract < 0.0) 0.0 else fract))
		}

		//- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
		if (d > per(NUM_PERCENTILES - 1)) { // the rate is better than the FIRST_PERCENTILE line in table
			// Note: max is never going to be as much as per(NUM_PERCENTILES-1)
			val fract = pow ({
				if (!doing_us) { // If we exceed the 0th percentile limit, or the 1st percentile exceeds that, then return 0th percentile
					if ((d >= Peak(atten_index)._1) || (per(NUM_PERCENTILES - 1) >= Peak(atten_index)._1)) return 0.0
					// Determine the proportion between 1st and 0th
					(d - per(NUM_PERCENTILES - 1)) / (Peak(atten_index)._1 - per(NUM_PERCENTILES - 1))
				}
				else {
					if ((d >= Peak(atten_index)._2) || (per(NUM_PERCENTILES - 1) >= Peak(atten_index)._2)) return 0.0
					(d - per(NUM_PERCENTILES - 1)) / (Peak(atten_index)._2 - per(NUM_PERCENTILES - 1))
				}
			}, choose_scale_power(NUM_PERCENTILES - 2, doing_us))
			return FIRST_PERCENTILE * (if (fract > 1.0) 1.0 else if (fract < 0.0) 0.0 else fract)
		}

		// The rate is somewhere between FIRST_PERCENTILE and 100-FIRST_PERCENTILE
		// first find the upper bound and then interpolate between the two
		var p = 1
		while (p < NUM_PERCENTILES - 1 && d >= per(p)) p += 1

		// the integer p refers to the percentile function ABOVE the current rate - now interpolate
		// Cater for equal percentiles to avoid the divide by zero
		if (per(p) == per(p-1))
			return 100.0 - FIRST_PERCENTILE - ((p-0.5) * PERCENTILE_STEP)

		// fract is the linearly interpolated position between the percentile gradient lines
		// recalculate fract -by scaling - this essentially is a non-linear interpolation between the percentile gradients
		val fract = pow ((d - per(p-1)) / (per(p) - per(p-1)), choose_scale_power (p-1, doing_us))

		// finally return a more accurate percentile based on the non-linear interpolated fraction between gradients
		return 100.0 - FIRST_PERCENTILE - ((fract + (p-1)) * PERCENTILE_STEP)
	}

	/***
	  *
	  * @param per			: percentiles table
	  * @param atten_index	: attenuation
	  * @param att_rate		: attenable_rate
	  * @param percentile	: percentile value, received from @Choose_Percentile
	  * @param tier			: tier (in short form, e.g: "100", "25", "1"
	  * @param doing_us		: is doing upstream
	  * @return				: corrected AttNDR
	  */
	def Choose_New_Rate_From_Percentile (per: Array[JavaFloat], atten_index: Short, att_rate: Int, percentile: Double, tier: String, doing_us: Boolean): Int =
	{
		// If percentile has not been set, return a null value
		if (percentile < 0.0) return -1

		// Set limits based on speed tier and transmission direction
		val max_tier_rate: Int = MAX_TIER_RATE(tier)
		val max_rate = if (doing_us) 45000 else 130000

		// Check whether current rate is below the top of the present speed tier. If so, don't correct the rate! It is already doing as good as it can!
		if (att_rate < max_tier_rate)
			return -1

		val v = if (percentile >= (100.0 - FIRST_PERCENTILE)) {
			// Interpolate between 0 and first line in the table. We need fraction of this
			val fract = (100.0 - percentile) / FIRST_PERCENTILE

			val linear = pow (if (fract > 1.0) 1.0 else if (fract <= 0.0) 0.000001 else fract,
				1.0 / choose_scale_power (0, doing_us))

			// Now reverse the hokey interpolation - the minimum is half per_atten_index[0], which is in kbps
			((per(0) / 2.0) + (linear * per(0) / 2.0)).toInt
		} else if (percentile <= FIRST_PERCENTILE) {
			// Interpolate between last line in the table and max_rate
			val fract = 1.0 - (percentile / FIRST_PERCENTILE) ;

			// if the current attainable rate exceeds a certain threshold, limit the amount of the correction
			val linear = (if (att_rate > max_rate) 0.3333 else 1.0) *
					pow (if (fract > 1.0) 1.0 else if (fract <= 0.0) 0.000001 else fract,
						1.0 / choose_scale_power (NUM_PERCENTILES-2, doing_us))

			// The rate is over and above the 1st percentile line, up to the maximum
			(per(NUM_PERCENTILES-1) + (linear * (max_rate - (per(NUM_PERCENTILES-1))))).toInt
		} else {
			// The rate is somewhere betweeh FIRST_PERCENTILE and 100-FIRST_PERCENTILE
			// find the upper bound and then interpolate between the two
			// p will be our pointer to the percentile contour. Actual rate will lie between [p-1] and [p]
			val steps = ((100.0 - FIRST_PERCENTILE) - percentile) / PERCENTILE_STEP
			val p = steps.toInt
			val fract = steps - p

			// reverse the non-linear percentile scaling as best we can. It won't be perfect
			val linear = pow(if (fract > 1.0) 1.0 else if (fract <= 0.0) 0.000001 else fract,
				1.0 / choose_scale_power (p-1, doing_us))

			(per(p-1) + (linear * (per(p) - per(p-1)))).toInt
		}

		if (v < max_tier_rate || v < att_rate) -1 else v
	}

	def calculate_corrected_attndr(service_type: TechType.TechType, best_atten_375: Short, max_add_NM: Short,
								   tier: String, att_ndr: Int, dpbo_profile: Byte,
								   current_pctls: Array[JavaFloat], expected_pctls: Array[JavaFloat], doing_us: Boolean): Int = {
		if (current_pctls.isEmpty || expected_pctls.isEmpty
			|| (service_type == TechType.FTTN && Seq("100", "40").contains(tier) && max_add_NM >= 30)
			|| (service_type == TechType.FTTB && Seq("100", "40", "50", "20").contains(tier)))
			return -1

		val percentile = Choose_Percentile(best_atten_375, att_ndr, if (doing_us) MAX_US else MAX_DS, current_pctls, doing_us)
		val corrected = Choose_New_Rate_From_Percentile(expected_pctls, best_atten_375, att_ndr, percentile, tier, doing_us)

		if (corrected < att_ndr) -1
		else if ((!doing_us) && dpbo_profile == 0)
			(corrected - att_ndr) / 2 + att_ndr
		else corrected
	}
}
