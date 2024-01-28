/*
Description: MLVLI extended CFILE for stat/tax reporting
History:
	* (4/3/2023) Initial script 
Version: 1.0.0
Author: AXIS Model Development 
*/

-- variables (to be replaced)
:setvar valdate  202206
:setvar tableid  202206_test_reporting
:setvar RESULTSDB AXIS_RESULTS_EQUITY

-- CTE: create stat_code mapping tables
delcare @mortality_basis_mapping table
(
  plan_code varchar(3), 
  moses_plan_code varchar(3),
  med_flag varchar(5),
  era varchar(1),
  lapse_status varchar(40),
  mortality_basis varchar(10)
) ;
INSERT INTO @mortality_basis_mapping  
VALUES 
  ('MSA', 'PRM5', 'P', '_', 'regular', 'CET80'),
  ('P6S', 'PRM6', 'P', '0', 'regular', 'CET80'),
  ('P6J', 'PRM6', 'P', '0', 'regular', 'CET80'),
  ('P7S', 'PRM6', 'P', '0', 'regular', 'CET80'),
  ('P7J', 'PRM6', 'P', '0', 'regular', 'CET80'),
  ('P8S', 'PRM6', 'P', '0', 'regular', 'CET80'),
  ('P8J', 'PRM6', 'P', '0', 'regular', 'CET80'),


CREATE FUNCTION map_stat_code(
	@plan_code varchar(10),
	@moses_plan_code varchar(10),
	@company_code varchar(1),
	@issue_year Integer,
	@med_flag varchar(5),
	@era varchar(5),
	@lapse_status varchar(40)
)
RETURNS varchar(255) AS

BEGIN
	DECLARE @stat_code_tlic varchar(255);
	DECLARE @stat_code_tflic varchar(255);
	DECLARE @stat_code varchar(255);
	DECLARE @mortality_basis varchar(255);
	-- first map mortality basis
	SET @mortality_basis = CASE 
		WHEN @moses_plan_code = 'PRM5' AND @med_flag = 'P' THEN 'CET80'
		WHEN @moses_plan_code = 'PRM6' AND @med_flag = 'P' AND @era = '0' THEN 'CET80'
		WHEN @moses_plan_code IN ('PRM3', 'PRM4') AND @plan_code <> 'SCHD' THEN
			CASE
				WHEN @lapse_status = 'eti' THEN 'CET58'
				ELSE 'CSO58'
			END
		ELSE
			'CSO80'
		END;
	-- map the stat code	
	SET @stat_code_tlic = CASE 
		-- MIPS
		WHEN @plan_code IN ('7PY', 'L83', 'L84', 'V81', 'V83', 'V84', 'LPE', 'VPE') AND @lapse_status = 'regular' THEN
			'58 CSO 4.5% MOD CRVM CNF (81-88)'
		WHEN @plan_code IN ('7PY', 'L83', 'L84', 'V81', 'V83', 'V84', 'LPE', 'VPE') AND @lapse_status = 'eti' THEN
			'58 CET 4.5% CNF (81-88)'
		WHEN @plan_code IN ('7PY', 'L83', 'L84', 'V81', 'V83', 'V84', 'LPE', 'VPE') AND @lapse_status = 'rpu' THEN
			'58 CSO 4.5% CNF (81-88)'
		WHEN @plan_code IN ('P83', 'PP3', 'PP4', 'S81', 'S83') THEN
			'58 CSO 4.5% CNF (81-88)'
		-- SCHD
		WHEN @plan_code IN ('SP1') AND @lapse_status = 'regular' THEN
			'80 CSO 4% MOD CRVM CNF (88-91)'
		WHEN @plan_code IN ('SP1') AND @lapse_status = 'eti' THEN
			'80 CET 4% NLP CNF (86-89)'
		WHEN @plan_code IN ('SP1') AND @lapse_status = 'rpu' THEN
			'80 CSO 4% MOD CRVM CNF (88-91)'
		-- FLEX
		WHEN @plan_code IN ('P1J','P1S', 'P2J', 'P2S', 'MSA', 'P6S', 'P6J', 'P7S', 'P7J', 'P8S', 'P8J','A1S') AND @mortality_basis = 'CSO80' THEN
			'80 CSO 4% CNF (86-03)'
		WHEN @plan_code IN ('P1J','P1S', 'P2J', 'P2S', 'MSA', 'P6S', 'P6J', 'P7S', 'P7J', 'P8S', 'P8J','A1S') AND @mortality_basis = 'CET80' THEN
			'80 CET 4% NLP CNF (86-89)'
		-- ESTI
		WHEN @plan_code IN ('EIS','EIJ') AND @issue_year<1995 THEN
			'80 CSO 5% CNF (93-94)'
		WHEN @plan_code IN ('EIS','EIJ') AND @issue_year>=1995 THEN
			'80 CSO 4.5% CNF (95-01)'

	END; 
	SET @stat_code_tflic = CASE
		WHEN @stat_code_tlic = '58 CET 4.5% CNF (81-88)' THEN '58 CET 4.5% ANB NL CNF (81-88)'
		WHEN @stat_code_tlic = '58 CSO 4.5% CNF (81-88)' THEN '58 CSO 4.5% ANB NL CNF (81-88)'
		WHEN @stat_code_tlic = '58 CSO 4.5% MOD CRVM CNF (81-88)' THEN '58 CSO 4.5% ANB MOD CRVM CNF (81-88)'
		WHEN @stat_code_tlic = '80 CET 4% NLP CNF (86-89)' THEN '80 CET 4% ANB NL CNF (86-89)'
		WHEN @stat_code_tlic = '80 CSO 4% CNF (86-03)' THEN '80 CSO 4% ANB NL CNF (86-01)'
		WHEN @stat_code_tlic = '80 CSO 4% MOD CRVM CNF (88-91)' THEN '80 CSO 4% ANB MOD CRVM CNF (88-91)'
	END;
	
	SET @stat_code = CASE
		WHEN @company_code IN ('2','3') THEN @stat_code_tlic
		WHEN @company_code IN ('4','5') THEN @stat_code_tflic
	END;
	RETURN @stat_code
	
END;
GO
-- SELECT dbo.map_stat_code('MSA', 'PRM5', '2', 1989, 'P', '_', 'regular') ;



WITH raw_tbl AS (
	SELECT * FROM VLI_AXIS_VALUATION_ALL_$(tableid)
),

add_mapping AS (
SELECT
a.*,
-- lapse status: regular/eit/rpu
CASE 
	WHEN SUBSTRING([Cell Name ALPHA],1,2) = 'U_' THEN 'rpu'
	WHEN SUBSTRING([Cell Name ALPHA],1,2) = 'I_' THEN 'eti'
	ELSE 'regular'
END AS lapse_status,
-- Company Code: 2/3: TLIC; 4/5: TFLIC
CASE
	WHEN [Statutory Company] IN (2,3) THEN 'TLIC'
	WHEN [Statutory Company] IN (4,5) THEN 'TFLIC'
	ELSE 'undefined'
END AS legal_entity,
-- ex5 stat code: look up from a static mapping file 
--b.stat_code AS stat_code,
-- investment base: equal to separate account value
[UL Seg Fund/ Sep Acct Balance] AS investment_base,
-- gid: equal to udv31_GID
[UDV31_GID] AS gid,
-- unmodeled reserve (SA): accrued charges
CASE
	WHEN [Plan Code] IN ('EIS', 'EIJ') THEN
		[Accrued Admin Charge] + [Accrued Asset Credit] + [Accrued Net Loan Cost]
	ELSE 
		[Accrued Admin Charge] + [Accrued Asset Credit] + [Accrued Net Loan Cost] + [Accrued COI] + [Accrued Load]
END AS accrued_charges,
-- unmodeled reserve (GA): unearned COI
CASE
	WHEN [Plan Code] IN ('EIS', 'EIJ') THEN
		-[Accrued COI] - [Accrued Load]
	ELSE 
		0
END AS unearned_coi,
-- gmdb reserves
CASE WHEN
	[Reserve details - AG37 AALR] > [Reserve details - AG37 OYT Res] THEN
		[Reserve details - AG37 AALR]
	ELSE
		[Reserve details - AG37 OYT Res]
END AS stat_gmdb_reserve,
CASE WHEN
	[Reserve details - AG37 AALR] > [Reserve details - AG37 OYT Res] THEN
		[Reserve details - AG37 AALR] * 0.9281
	ELSE
		[Reserve details - AG37 OYT Res] * 0.9281
END AS tax_gmdb_reserve

FROM raw_tbl a
--LEFT JOIN axis_inforce.dbo.vli_stat_code_mapping b
--ON a.policyId = b.PolicyId
),
reserve_calc_1 AS (
SELECT 
*,
dbo.map_stat_code([Plan Code], [MoSes Plan Code], [Statutory Company], [Iss Year], [med_flag], [era], [lapse_status]) AS stat_code,
-- separate account reserve
[investment_base] - [accrued_charges] AS stat_sa_reserve,
[investment_base] - [accrued_charges] AS tax_sa_reserve,
-- eti reserve
CASE WHEN lapse_status = 'eti' THEN [Stat Res - Gross reserve]
ELSE 0
END AS stat_eti_reserve,
0 AS tax_eti_reserve,
-- rpu reserve
CASE WHEN lapse_status = 'rpu' THEN [Stat Res - Gross reserve]
ELSE 0
END AS stat_rpu_reserve,
0 AS tax_rpu_reserve,
-- deficiency reserv
[Stat Res - Gross deficiency reserve] AS stat_def_reserve,
0 AS tax_def_reserve

FROM add_mapping
),
reserve_calc_2 AS ( 
SELECT 
*,
[Stat Res - Gross reserve] + gid + unearned_coi - accrued_charges AS stat_total_liability,
[Tax Res - Gross reserve] + gid + unearned_coi - accrued_charges AS tax_total_liability, 
[Stat Res - Gross reserve] - investment_base - stat_gmdb_reserve - stat_def_reserve + unearned_coi + gid AS stat_ga_reserve,
[Tax Res - Gross reserve] - investment_base - tax_gmdb_reserve - tax_def_reserve + unearned_coi + gid AS tax_ga_reserve
FROM reserve_calc_1
),

Extended_Cfile AS (
SELECT 
*,
-- JE fields
'101M0000' AS JE_center,
stat_ga_reserve AS JE_2001D1R,
0 AS JE_2001C1R,
stat_gmdb_reserve AS JE_200DD1R,
stat_def_reserve AS JE_200AD1R,
0 AS JE_2008D1R,
0 AS JE_150211R,
0 AS JE_151211R,
0 AS JE_2061D1R,
-- Ex5 fields
stat_code AS ex5_stat_code,
stat_ga_reserve AS ex5_A_direct,
0 AS ex5_A_ceded,
stat_gmdb_reserve AS ex5_G_gmdb,
stat_def_reserve AS ex5_G_def,
0 AS ex5_G_ceded,
0 AS ex5_E_alr,
0 AS ex5_F_dlr,
-- Tax fields
'101M0000' AS Tax_center,
tax_ga_reserve AS Tax_2001D1R,
0 AS Tax_2001C1R,
tax_gmdb_reserve AS Tax_200DD1R,
tax_def_reserve AS Tax_200AD1R,
0 AS Tax_2008D1R,
0 AS Tax_150211R,
0 AS Tax_151211R,
0 AS Tax_2061D1R,
-- KC4
stat_total_liability AS KC4_total_stat,
tax_total_liability AS KC4_total_tax,
stat_sa_reserve AS KC4_stat_sa,
tax_sa_reserve AS KC4_tax_sa,
stat_ga_reserve AS KC4_stat_ga,
tax_ga_reserve AS KC4_tax_ga,
stat_gmdb_reserve AS KC4_stat_gmdb_reserve,
tax_gmdb_reserve AS KC4_tax_gmdb_reserve,
stat_def_reserve AS KC4_stat_def_reserve,
tax_def_reserve AS KC4_tax_def_reserve,
0 AS KC4_stat_ceded_reserve,
0 AS KC4_tax_ceded_reserve

FROM reserve_calc_2
),

cfile_summary AS (

SELECT
legal_entity,
sum(JE_2001D1R) AS JE_2001D1R,
sum(JE_2001C1R) AS JE_2001C1R,
sum(JE_200DD1R) AS JE_200DD1R,
sum(JE_200AD1R) AS JE_200AD1R,
sum(JE_2008D1R) AS JE_2008D1R,
sum(JE_150211R) AS JE_150211R,
sum(JE_151211R) AS JE_151211R,
sum(JE_2061D1R) AS JE_2061D1R,
-- Ex5 fields
sum(ex5_A_direct) AS ex5_A_direct,
sum(ex5_A_ceded) AS ex5_A_ceded,
sum(ex5_G_gmdb) AS ex5_G_gmdb,
sum(ex5_G_def) AS ex5_G_def,
sum(ex5_G_ceded) AS ex5_G_ceded,
sum(ex5_E_alr) AS ex5_E_alr,
sum(ex5_F_dlr) AS ex5_F_dlr,
-- Tax fields
sum(Tax_2001D1R) AS Tax_2001D1R,
sum(Tax_2001C1R) AS Tax_2001C1R,
sum(Tax_200DD1R) AS Tax_200DD1R,
sum(Tax_200AD1R) AS Tax_200AD1R,
sum(Tax_2008D1R) AS Tax_2008D1R,
sum(Tax_150211R) AS Tax_150211R,
sum(Tax_151211R) AS Tax_151211R,
sum(Tax_2061D1R) AS Tax_2061D1R,
-- KC4
sum(KC4_total_stat) AS KC4_total_stat,
sum(KC4_total_tax) AS KC4_total_tax,
sum(KC4_stat_sa) AS KC4_stat_sa,
sum(KC4_tax_sa) AS KC4_tax_sa,
sum(KC4_stat_ga) AS KC4_stat_ga,
sum(KC4_tax_ga) AS KC4_tax_ga,
sum(KC4_stat_gmdb_reserve) AS KC4_stat_gmdb_reserve,
sum(KC4_tax_gmdb_reserve) AS KC4_tax_gmdb_reserve,
sum(KC4_stat_def_reserve) AS KC4_stat_def_reserve,
sum(KC4_tax_def_reserve) AS KC4_tax_def_reserve,
sum(KC4_stat_ceded_reserve) AS KC4_stat_ceded_reserve,
sum(KC4_tax_ceded_reserve) AS KC4_tax_ceded_reserve
FROM extended_cfile 
GROUP BY legal_entity
)
SELECT * 
FROM cfile_summary
ORDER BY legal_entity DESC;
GO
DROP FUNCTION map_stat_code;
GO
