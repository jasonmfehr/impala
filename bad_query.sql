DROP TABLE IF EXISTS test_left_fact;
CREATE TABLE test_left_fact (
    product_sk BIGINT,
    drv_event_type STRING
);

DROP TABLE IF EXISTS test_right_cte;
CREATE TABLE test_right_cte (
    product_sk BIGINT,
    product_type STRING
);

-- Create a temporary view of 10 numbers
WITH ten AS (
  SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
  SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL 
  SELECT 8 UNION ALL SELECT 9
)

-- 1. Insert 1,024 rows into the Left Fact table (Exactly 1 Impala Batch)
-- All rows share the same join key (999)
INSERT INTO test_left_fact
SELECT 
    999 AS product_sk, 
    'fail_event' AS drv_event_type 
FROM ten a CROSS JOIN ten b CROSS JOIN ten c 
LIMIT 1024;

-- 2. Insert 100,000 rows into the Right CTE table
-- All rows share the same join key (999)
--WITH ten AS (
--  SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
--  SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL 
--  SELECT 8 UNION ALL SELECT 9
--)
--INSERT INTO test_right_cte
--SELECT 
--    999 AS product_sk, 
--    'wrong_type' AS product_type 
--FROM ten a CROSS JOIN ten b CROSS JOIN ten c CROSS JOIN ten d CROSS JOIN ten e;

WITH ten AS (
  SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
  SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL 
  SELECT 8 UNION ALL SELECT 9
)

-- Insert 1,000,000 rows into the Right CTE table
-- All 1 Million rows share the exact same join key (999)
INSERT INTO test_right_cte
SELECT 
    999 AS product_sk, 
    'wrong_type' AS product_type 
FROM ten a 
CROSS JOIN ten b 
CROSS JOIN ten c 
CROSS JOIN ten d 
CROSS JOIN ten e
CROSS JOIN ten f;


select count(*) from test_right_cte;

set MEM_LIMIT_EXECUTORS=185mb;
SELECT STRAIGHT_JOIN count(1)
FROM test_left_fact f
LEFT JOIN test_right_cte p2 
  ON f.product_sk = p2.product_sk
WHERE (
  -- Condition 1: Evaluates to FALSE
  UPPER(f.drv_event_type) LIKE '%_CONF' 
  OR 
  -- Condition 2: The Memory Trap
  -- Because Condition 1 is FALSE, Impala MUST evaluate this entire block for all 100,000 matches.
  -- Every UPPER() and CONCAT() asks the OS for fresh memory.
  (
    CASE 
      WHEN UPPER(p2.product_type) = 'COMMODITY-SWAP' 
        THEN UPPER(CONCAT(f.drv_event_type, '_SWAP_1'))
        
      WHEN UPPER(p2.product_type) = 'METAL-FUTURE' 
        THEN UPPER(CONCAT(f.drv_event_type, '_METAL_2'))
        
      WHEN UPPER(p2.product_type) = 'ENERGY-FUTURE' 
        THEN UPPER(CONCAT(f.drv_event_type, '_ENERGY_3'))
        
      WHEN UPPER(p2.product_type) = 'AGRICULTURAL-FUTURE' 
        THEN UPPER(CONCAT(f.drv_event_type, '_AGRI_4'))
        
      WHEN UPPER(p2.product_type) = 'SOFT-FUTURE' 
        THEN UPPER(CONCAT(f.drv_event_type, '_SOFT_5'))
        
      WHEN UPPER(p2.product_type) = 'OTHER-FUTURE' 
        THEN UPPER(CONCAT(f.drv_event_type, '_OTHER_6'))
        
      WHEN UPPER(p2.product_type) = 'METAL-OPTION' 
        THEN UPPER(CONCAT(f.drv_event_type, '_MOPT_7'))
        
      WHEN UPPER(p2.product_type) = 'ENERGY-OPTION' 
        THEN UPPER(CONCAT(f.drv_event_type, '_EOPT_8'))
        
      WHEN UPPER(p2.product_type) = 'SPREAD-OPTION' 
        THEN UPPER(CONCAT(f.drv_event_type, '_SOPT_9'))
        
      WHEN UPPER(p2.product_type) = 'COMMODITY-INDEX' 
        THEN UPPER(CONCAT(f.drv_event_type, '_INDEX_10'))
        
      -- The fallback generates two more uppercase string allocations
      ELSE UPPER(CONCAT(UPPER(f.drv_event_type), '_UNKNOWN')) 
      
    END = 'COMMODITY-FORWARD'
  )
);
