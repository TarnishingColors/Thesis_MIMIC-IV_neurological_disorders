SELECT * FROM mimic_data.diagnoses_icd
WHERE (
      icd_version = 9
      AND icd_code ~ '^\d'
      AND cast(substring(icd_code, 0, 4) AS INTEGER)
              BETWEEN 320 AND 389
) OR (
    icd_code LIKE 'G%'
    AND icd_version = 10
)