DROP TABLE IF EXISTS average_amount_by_speciality;
CREATE TABLE average_amount_by_speciality
AS
SELECT speciality as speciality, AVG(reimbursement_amount)
FROM reimbursements_201807
WHERE reimbursement_amount != 0
GROUP BY speciality;