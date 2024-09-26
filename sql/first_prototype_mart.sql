SELECT p.subject_id
    , i.hadm_id
    , i.stay_id
    , i.first_careunit
    , i.last_careunit
    , i.intime
    , i.outtime
    , i.los
    , a.admittime
    , a.dischtime
    , a.deathtime
    , a.admission_type
    , a.admit_provider_id
    , a.admission_location
    , a.discharge_location
    , a.insurance
    , a.language
    , a.marital_status
    , a.race
    , a.edregtime
    , a.edouttime
    , a.hospital_expire_flag
    , p.gender
    , p.anchor_year
    , p.anchor_year_group
    , p.dod
    , d.icd_code
    , d.icd_version
    , EXTRACT(DAYS FROM a.dischtime - a.admittime)        AS days_admitted
    , CASE WHEN a.deathtime IS NOT NULL THEN 1 ELSE 0 END AS died
FROM mimic_data.icustays i
JOIN mimic_data.admissions a ON i.hadm_id = a.hadm_id
JOIN mimic_data.patients p ON p.subject_id = a.subject_id
JOIN mimic_data.diagnoses_icd d ON p.subject_id = d.subject_id AND a.hadm_id = d.hadm_id
WHERE d.icd_version = 10
AND (
    d.icd_code LIKE 'I61%' OR
    d.icd_code LIKE 'I63%' OR
    d.icd_code LIKE 'G41%'
);