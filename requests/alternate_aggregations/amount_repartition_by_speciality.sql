DROP TABLE IF EXISTS total_amount_repartition_by_speciality;
CREATE TABLE total_amount_repartition_by_speciality
AS
SELECT 
  speciality, 
  (
    SUM(reimbursement_amount)/(SELECT SUM(reimbursement_amount) FROM reimbursements_201807) * 100
  ) as total_amount_portion
FROM reimbursements_201807
GROUP BY speciality;
