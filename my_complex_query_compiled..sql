SET NOCOUNT ON;

WITH cashflows AS (
    SELECT *
    FROM [AXIS_RESULTS_EQUITY].dbo.[IFRS17_FLVUL_202206_7]
),
discount AS (
    SELECT ScnNumber,
        ProjMonth,
        Discount_Factor
    FROM [AXIS_INFORCE].dbo.[IFRS17_SCEN_202206_7]

)

Select a.*, b.*
From cashflows a
INNER JOIN discount b
ON a.ScnNumber = b.ScnNumber
AND a.RowNo = b.ProjMonth
