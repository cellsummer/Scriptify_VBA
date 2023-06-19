WITH pre_pv_factors AS (
	SELECT 
		scn,
		t,
		discount_rate,
		COALESCE(t - LAG(t, 1) OVER (PARTITION BY scn ORDER BY scn),0) AS delta_t,
		power(1 + discount_rate, -1.0/12) AS single_month_pv_rate,
		power(
			1 + discount_rate,
			- 1.0/12 * COALESCE(t - LAG(t, 1) OVER (PARTITION BY scn ORDER BY scn), 0)
		) 	AS single_period_pv_rate,
		power(
			1 + discount_rate,
			- 1.0/24 * COALESCE(t - LAG(t, 1) OVER (PARTITION BY scn ORDER BY scn), 0)
		) 	AS half_period_pv_rate
	FROM cfs
)
SELECT 
*,
EXP(sum(log(single_period_pv_rate)) OVER (PARTITION BY scn ORDER BY t)) AS pv_factor_eop,
EXP(sum(log(single_period_pv_rate)) OVER (PARTITION BY scn ORDER BY t)) / half_period_pv_rate AS pv_factor_mop
FROM pre_pv_factors
