DROP TABLE IF EXISTS total_amount_repartition_by_speciality;
CREATE TABLE total_amount_repartition_by_speciality
(
  speciality VARCHAR(128) PRIMARY KEY,
  total_amount_portion FLOAT
);
COPY total_amount_repartition_by_speciality(speciality, total_amount_portion)
FROM '{csv_path}' DELIMITER ',' CSV HEADER;