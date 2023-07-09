--test to see how sqlglot parses views with different source schemas
--CREATE SCHEMA RESERVATIONS_SCHEMA;

CREATE VIEW RESERVATIONS_SCHEMA.TEST_CROSS_SCHEMA AS 
SELECT
  H.NAME,
  R.TYPE,
  R.FREE,
  COUNT(R.FREE) AS FREE_RM_COUNT
FROM
  HOTEL_SCHEMA.ROOM AS R
  LEFT JOIN
  HOTEL_SCHEMA.HOTEL AS H
  ON H.HNO = R.HNO
GROUP BY 
  H.NAME,
  R.TYPE, 
  R.FREE;


