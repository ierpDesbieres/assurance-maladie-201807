DROP TABLE IF EXISTS average_amount_by_speciality;
CREATE TABLE average_amount_by_speciality
(
  speciality VARCHAR(128) PRIMARY KEY,
  average_reimbursement FLOAT
);
COPY average_amount_by_speciality(speciality, average_reimbursement)
FROM '{csv_path}' DELIMITER ',' CSV HEADER;