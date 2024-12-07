"""Module to create admissions mart"""

import configparser
from ..data_transfer.utils import ConnectionDetails, DataTransfer

# pylint: disable=duplicate-code
config = configparser.ConfigParser()
config.read('config.ini')

file_folder = config['raw_data']['file_folder']
# pylint: enable=duplicate-code

dt = DataTransfer(ConnectionDetails(*(x[1] for x in config.items('database'))))

dt.run_query(
    """
    DROP TABLE IF EXISTS transformed_labevents;
    CREATE TEMPORARY TABLE transformed_labevents AS (
    SELECT DISTINCT ON (l.subject_id, l.hadm_id, dl.category, dl.label)
        l.subject_id,
        l.hadm_id,
        dl.category,
        dl.label,
        MIN(l.valuenum) OVER w AS min_value,
        MAX(l.valuenum) OVER w AS max_value,
        AVG(l.valuenum) OVER w AS avg_value,
        l.ref_range_lower,
        l.ref_range_upper,
        l.valueuom,
        MIN(charttime) OVER w AS first_test_charttime,
        MAX(charttime) OVER w AS last_test_charttime,
        MIN(charttime) FILTER (WHERE l.flag = 'abnormal') OVER w AS first_abnormal_charttime,
        CASE
            WHEN SUM(CASE WHEN l.flag IS NOT NULL THEN 1 ELSE 0 END) OVER w > 0
            THEN TRUE
            ELSE FALSE
        END AS was_abnormal,
        ROUND(CAST(SUM(CASE WHEN l.flag IS NOT NULL THEN 1 ELSE 0 END) OVER w AS DECIMAL) /
            COUNT(*) OVER w, 2) AS ratio_abnormal
    FROM raw.labevents l
    JOIN mart.d_labitems dl
        ON l.itemid = dl.itemid
    WINDOW w AS (PARTITION BY l.subject_id, l.hadm_id, dl.category, dl.label)
    );

    DROP TABLE IF EXISTS labevents_to_admissions;
    CREATE TEMPORARY TABLE labevents_to_admissions AS (    
        SELECT hadm_id
        
            , SUM(CASE WHEN label = 'Sodium' THEN min_value END) AS sodium_min_value
            , SUM(CASE WHEN label = 'Sodium' THEN max_value END) AS sodium_max_value
            , SUM(CASE WHEN label = 'Sodium' THEN avg_value END) AS sodium_avg_value
            , SUM(CASE WHEN label = 'Sodium' THEN ref_range_lower END) AS sodium_ref_range_lower
            , SUM(CASE WHEN label = 'Sodium' THEN ref_range_upper END) AS sodium_ref_range_upper
            , MIN(CASE WHEN label = 'Sodium' THEN first_abnormal_charttime END) AS sodium_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Sodium' THEN first_test_charttime END) AS sodium_first_test_charttime
            , MAX(CASE WHEN label = 'Sodium' THEN last_test_charttime END) AS sodium_last_test_charttime
            --, 'mEq/L' AS sodium_valueuom
            , BOOL_OR(CASE WHEN label = 'Sodium' THEN was_abnormal END) AS sodium_was_abnormal
            , SUM(CASE WHEN label = 'Sodium' THEN ratio_abnormal END) AS sodium_ratio_abnormal
        
            -- not applicable for the current selection of data
            /*
            , SUM(CASE WHEN label = 'Sodium, CSF' THEN min_value END) AS sodium_csf_min_value
            , SUM(CASE WHEN label = 'Sodium, CSF' THEN max_value END) AS sodium_csf_max_value
            , SUM(CASE WHEN label = 'Sodium, CSF' THEN avg_value END) AS sodium_csf_avg_value
            , SUM(CASE WHEN label = 'Sodium, CSF' THEN ref_range_lower END) AS sodium_csf_ref_range_lower
            , SUM(CASE WHEN label = 'Sodium, CSF' THEN ref_range_upper END) AS sodium_csf_ref_range_upper
            , SUM(CASE WHEN label = 'Sodium, CSF' THEN valueuom END) AS sodium_csf_valueuom
            , SUM(CASE WHEN label = 'Sodium, CSF' THEN was_abnormal END) AS sodium_csf_was_abnormal
            , SUM(CASE WHEN label = 'Sodium, CSF' THEN ratio_abnormal END) AS sodium_csf_ratio_abnormal
            */
        
            , SUM(CASE WHEN label = 'Sodium, Urine' THEN min_value END) AS sodium_urine_min_value
            , SUM(CASE WHEN label = 'Sodium, Urine' THEN max_value END) AS sodium_urine_max_value
            , SUM(CASE WHEN label = 'Sodium, Urine' THEN avg_value END) AS sodium_urine_avg_value
            , SUM(CASE WHEN label = 'Sodium, Urine' THEN ref_range_lower END) AS sodium_urine_ref_range_lower
            , SUM(CASE WHEN label = 'Sodium, Urine' THEN ref_range_upper END) AS sodium_urine_ref_range_upper
            , MIN(CASE WHEN label = 'Sodium, Urine' THEN first_abnormal_charttime END) AS sodium_urine_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Sodium, Urine' THEN first_test_charttime END) AS sodium_urine_first_test_charttime
            , MAX(CASE WHEN label = 'Sodium, Urine' THEN last_test_charttime END) AS sodium_urine_last_test_charttime
            --, 'mEq/L' AS sodium_urine_valueuom
            , BOOL_OR(CASE WHEN label = 'Sodium, Urine' THEN was_abnormal END) AS sodium_urine_was_abnormal
            , SUM(CASE WHEN label = 'Sodium, Urine' THEN ratio_abnormal END) AS sodium_urine_ratio_abnormal
        
            , SUM(CASE WHEN label = 'Sodium, Whole Blood' THEN min_value END) AS sodium_whole_blood_min_value
            , SUM(CASE WHEN label = 'Sodium, Whole Blood' THEN max_value END) AS sodium_whole_blood_max_value
            , SUM(CASE WHEN label = 'Sodium, Whole Blood' THEN avg_value END) AS sodium_whole_blood_avg_value
            , SUM(CASE WHEN label = 'Sodium, Whole Blood' THEN ref_range_lower END) AS sodium_whole_blood_ref_range_lower
            , SUM(CASE WHEN label = 'Sodium, Whole Blood' THEN ref_range_upper END) AS sodium_whole_blood_ref_range_upper
            , MIN(CASE WHEN label = 'Sodium, Whole Blood' THEN first_abnormal_charttime END) AS sodium_whole_blood_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Sodium, Whole Blood' THEN first_test_charttime END) AS sodium_whole_blood_first_test_charttime
            , MAX(CASE WHEN label = 'Sodium, Whole Blood' THEN last_test_charttime END) AS sodium_whole_blood_last_test_charttime
            --, 'mEq/L' AS sodium_whole_blood_valueuom
            , BOOL_OR(CASE WHEN label = 'Sodium, Whole Blood' THEN was_abnormal END) AS sodium_whole_blood_was_abnormal
            , SUM(CASE WHEN label = 'Sodium, Whole Blood' THEN ratio_abnormal END) AS sodium_whole_blood_ratio_abnormal
        
            , SUM(CASE WHEN label = 'Potassium' THEN min_value END) AS potassium_min_value
            , SUM(CASE WHEN label = 'Potassium' THEN max_value END) AS potassium_max_value
            , SUM(CASE WHEN label = 'Potassium' THEN avg_value END) AS potassium_avg_value
            , SUM(CASE WHEN label = 'Potassium' THEN ref_range_lower END) AS potassium_ref_range_lower
            , SUM(CASE WHEN label = 'Potassium' THEN ref_range_upper END) AS potassium_ref_range_upper
            , MIN(CASE WHEN label = 'Potassium' THEN first_abnormal_charttime END) AS potassium_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Potassium' THEN first_test_charttime END) AS potassium_first_test_charttime
            , MAX(CASE WHEN label = 'Potassium' THEN last_test_charttime END) AS potassium_last_test_charttime
            --, 'mEq/L' AS potassium_valueuom
            , BOOL_OR(CASE WHEN label = 'Potassium' THEN was_abnormal END) AS potassium_was_abnormal
            , SUM(CASE WHEN label = 'Potassium' THEN ratio_abnormal END) AS potassium_ratio_abnormal
        
             -- not applicable for the current selection of data
            /*
            , SUM(CASE WHEN label = 'Potassium, CSF' THEN min_value END) AS potassium_csf_min_value
            , SUM(CASE WHEN label = 'Potassium, CSF' THEN max_value END) AS potassium_csf_max_value
            , SUM(CASE WHEN label = 'Potassium, CSF' THEN avg_value END) AS potassium_csf_avg_value
            , SUM(CASE WHEN label = 'Potassium, CSF' THEN ref_range_lower END) AS potassium_csf_ref_range_lower
            , SUM(CASE WHEN label = 'Potassium, CSF' THEN ref_range_upper END) AS potassium_csf_ref_range_upper
            , SUM(CASE WHEN label = 'Potassium, CSF' THEN valueuom END) AS potassium_csf_valueuom
            , SUM(CASE WHEN label = 'Potassium, CSF' THEN was_abnormal END) AS potassium_csf_was_abnormal
            , SUM(CASE WHEN label = 'Potassium, CSF' THEN ratio_abnormal END) AS potassium_csf_ratio_abnormal
             */
        
            , SUM(CASE WHEN label = 'Potassium, Whole Blood' THEN min_value END) AS potassium_whole_blood_min_value
            , SUM(CASE WHEN label = 'Potassium, Whole Blood' THEN max_value END) AS potassium_whole_blood_max_value
            , SUM(CASE WHEN label = 'Potassium, Whole Blood' THEN avg_value END) AS potassium_whole_blood_avg_value
            , SUM(CASE WHEN label = 'Potassium, Whole Blood' THEN ref_range_lower END) AS potassium_whole_blood_ref_range_lower
            , SUM(CASE WHEN label = 'Potassium, Whole Blood' THEN ref_range_upper END) AS potassium_whole_blood_ref_range_upper
            , MIN(CASE WHEN label = 'Potassium, Whole Blood' THEN first_abnormal_charttime END) AS potassium_whole_blood_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Potassium, Whole Blood' THEN first_test_charttime END) AS potassium_whole_blood_first_test_charttime
            , MAX(CASE WHEN label = 'Potassium, Whole Blood' THEN last_test_charttime END) AS potassium_whole_blood_last_test_charttime
            --, 'mEq/L' AS potassium_whole_blood_valueuom
            , BOOL_OR(CASE WHEN label = 'Potassium, Whole Blood' THEN was_abnormal END) AS potassium_whole_blood_was_abnormal
            , SUM(CASE WHEN label = 'Potassium, Whole Blood' THEN ratio_abnormal END) AS potassium_whole_blood_ratio_abnormal
        
            , SUM(CASE WHEN label = 'Calcium, Total' THEN min_value END) AS calcium_total_min_value
            , SUM(CASE WHEN label = 'Calcium, Total' THEN max_value END) AS calcium_total_max_value
            , SUM(CASE WHEN label = 'Calcium, Total' THEN avg_value END) AS calcium_total_avg_value
            , SUM(CASE WHEN label = 'Calcium, Total' THEN ref_range_lower END) AS calcium_total_ref_range_lower
            , SUM(CASE WHEN label = 'Calcium, Total' THEN ref_range_upper END) AS calcium_total_ref_range_upper
            , MIN(CASE WHEN label = 'Calcium, Total' THEN first_abnormal_charttime END) AS calcium_total_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Calcium, Total' THEN first_test_charttime END) AS calcium_total_first_test_charttime
            , MAX(CASE WHEN label = 'Calcium, Total' THEN last_test_charttime END) AS calcium_total_last_test_charttime
            --, 'mg/dL' AS calcium_total_valueuom
            , BOOL_OR(CASE WHEN label = 'Calcium, Total' THEN was_abnormal END) AS calcium_total_was_abnormal
            , SUM(CASE WHEN label = 'Calcium, Total' THEN ratio_abnormal END) AS calcium_total_ratio_abnormal
        
            , SUM(CASE WHEN label = 'Free Calcium' THEN min_value END) AS free_calcium_min_value
            , SUM(CASE WHEN label = 'Free Calcium' THEN max_value END) AS free_calcium_max_value
            , SUM(CASE WHEN label = 'Free Calcium' THEN avg_value END) AS free_calcium_avg_value
            , SUM(CASE WHEN label = 'Free Calcium' THEN ref_range_lower END) AS free_calcium_ref_range_lower
            , SUM(CASE WHEN label = 'Free Calcium' THEN ref_range_upper END) AS free_calcium_ref_range_upper
            , MIN(CASE WHEN label = 'Free Calcium' THEN first_abnormal_charttime END) AS free_calcium_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Free Calcium' THEN first_test_charttime END) AS free_calcium_first_test_charttime
            , MAX(CASE WHEN label = 'Free Calcium' THEN last_test_charttime END) AS free_calcium_last_test_charttime
            --, 'mmol/L' AS free_calcium_valueuom
            , BOOL_OR(CASE WHEN label = 'Free Calcium' THEN was_abnormal END) AS free_calcium_was_abnormal
            , SUM(CASE WHEN label = 'Free Calcium' THEN ratio_abnormal END) AS free_calcium_ratio_abnormal
        
            -- not applicable for the current selection of data
            /*
            , SUM(CASE WHEN label = '% Ionized Calcium' THEN min_value END) AS ratio_ionized_calcium_min_value
            , SUM(CASE WHEN label = '% Ionized Calcium' THEN max_value END) AS ratio_ionized_calcium_max_value
            , SUM(CASE WHEN label = '% Ionized Calcium' THEN avg_value END) AS ratio_ionized_calcium_avg_value
            , SUM(CASE WHEN label = '% Ionized Calcium' THEN ref_range_lower END) AS ratio_ionized_calcium_ref_range_lower
            , SUM(CASE WHEN label = '% Ionized Calcium' THEN ref_range_upper END) AS ratio_ionized_calcium_ref_range_upper
            , SUM(CASE WHEN label = '% Ionized Calcium' THEN valueuom END) AS ratio_ionized_calcium_valueuom
            , BOOL_OR(CASE WHEN label = '% Ionized Calcium' THEN was_abnormal END) AS ratio_ionized_calcium_was_abnormal
            , SUM(CASE WHEN label = '% Ionized Calcium' THEN ratio_abnormal END) AS ratio_ionized_calcium_ratio_abnormal
             */
        
            -- not applicable for the current selection of data
            /*
            , SUM(CASE WHEN label = 'Total Calcium' THEN min_value END) AS total_calcium_min_value
            , SUM(CASE WHEN label = 'Total Calcium' THEN max_value END) AS total_calcium_max_value
            , SUM(CASE WHEN label = 'Total Calcium' THEN avg_value END) AS total_calcium_avg_value
            , SUM(CASE WHEN label = 'Total Calcium' THEN ref_range_lower END) AS total_calcium_ref_range_lower
            , SUM(CASE WHEN label = 'Total Calcium' THEN ref_range_upper END) AS total_calcium_ref_range_upper
            , SUM(CASE WHEN label = 'Total Calcium' THEN valueuom END) AS total_calcium_valueuom
            , BOOL_OR(CASE WHEN label = 'Total Calcium' THEN was_abnormal END) AS total_calcium_was_abnormal
            , SUM(CASE WHEN label = 'Total Calcium' THEN ratio_abnormal END) AS total_calcium_ratio_abnormal
             */
        
            , SUM(CASE WHEN label = 'Magnesium' THEN min_value END) AS magnesium_min_value
            , SUM(CASE WHEN label = 'Magnesium' THEN max_value END) AS magnesium_max_value
            , SUM(CASE WHEN label = 'Magnesium' THEN avg_value END) AS magnesium_avg_value
            , SUM(CASE WHEN label = 'Magnesium' THEN ref_range_lower END) AS magnesium_ref_range_lower
            , SUM(CASE WHEN label = 'Magnesium' THEN ref_range_upper END) AS magnesium_ref_range_upper
            , MIN(CASE WHEN label = 'Magnesium' THEN first_abnormal_charttime END) AS magnesium_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Magnesium' THEN first_test_charttime END) AS magnesium_first_test_charttime
            , MAX(CASE WHEN label = 'Magnesium' THEN last_test_charttime END) AS magnesium_last_test_charttime
            --, 'mg/dL' AS magnesium_valueuom
            , BOOL_OR(CASE WHEN label = 'Magnesium' THEN was_abnormal END) AS magnesium_was_abnormal
            , SUM(CASE WHEN label = 'Magnesium' THEN ratio_abnormal END) AS magnesium_ratio_abnormal
        
            -- not applicable for the current selection of data
            /*
            , SUM(CASE WHEN label = 'Magnesium, Body Fluid' THEN min_value END) AS magnesium_body_fluid_min_value
            , SUM(CASE WHEN label = 'Magnesium, Body Fluid' THEN max_value END) AS magnesium_body_fluid_max_value
            , SUM(CASE WHEN label = 'Magnesium, Body Fluid' THEN avg_value END) AS magnesium_body_fluid_avg_value
            , SUM(CASE WHEN label = 'Magnesium, Body Fluid' THEN ref_range_lower END) AS magnesium_body_fluid_ref_range_lower
            , SUM(CASE WHEN label = 'Magnesium, Body Fluid' THEN ref_range_upper END) AS magnesium_body_fluid_ref_range_upper
            , SUM(CASE WHEN label = 'Magnesium, Body Fluid' THEN valueuom END) AS magnesium_body_fluid_valueuom
            , BOOL_OR(CASE WHEN label = 'Magnesium, Body Fluid' THEN was_abnormal END) AS magnesium_body_fluid_was_abnormal
            , SUM(CASE WHEN label = 'Magnesium, Body Fluid' THEN ratio_abnormal END) AS magnesium_body_fluid_ratio_abnormal
             */
        
            , SUM(CASE WHEN label = 'Glucose' THEN min_value END) AS glucose_min_value
            , SUM(CASE WHEN label = 'Glucose' THEN max_value END) AS glucose_max_value
            , SUM(CASE WHEN label = 'Glucose' THEN avg_value END) AS glucose_avg_value
            , SUM(CASE WHEN label = 'Glucose' THEN ref_range_lower END) AS glucose_ref_range_lower
            , SUM(CASE WHEN label = 'Glucose' THEN ref_range_upper END) AS glucose_ref_range_upper
            , MIN(CASE WHEN label = 'Glucose' THEN first_abnormal_charttime END) AS glucose_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Glucose' THEN first_test_charttime END) AS glucose_first_test_charttime
            , MAX(CASE WHEN label = 'Glucose' THEN last_test_charttime END) AS glucose_last_test_charttime
            --, 'mg/dL' AS glucose_valueuom
            , BOOL_OR(CASE WHEN label = 'Glucose' THEN was_abnormal END) AS glucose_was_abnormal
            , SUM(CASE WHEN label = 'Glucose' THEN ratio_abnormal END) AS glucose_ratio_abnormal
        
            , SUM(CASE WHEN label = 'Glucose, Ascites' THEN min_value END) AS glucose_ascites_min_value
            , SUM(CASE WHEN label = 'Glucose, Ascites' THEN max_value END) AS glucose_ascites_max_value
            , SUM(CASE WHEN label = 'Glucose, Ascites' THEN avg_value END) AS glucose_ascites_avg_value
            , SUM(CASE WHEN label = 'Glucose, Ascites' THEN ref_range_lower END) AS glucose_ascites_ref_range_lower
            , SUM(CASE WHEN label = 'Glucose, Ascites' THEN ref_range_upper END) AS glucose_ascites_ref_range_upper
            , MIN(CASE WHEN label = 'Glucose, Ascites' THEN first_abnormal_charttime END) AS glucose_ascites_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Glucose, Ascites' THEN first_test_charttime END) AS glucose_ascites_first_test_charttime
            , MAX(CASE WHEN label = 'Glucose, Ascites' THEN last_test_charttime END) AS glucose_ascites_last_test_charttime
            --, 'mg/dL' AS glucose_ascites_valueuom
            , BOOL_OR(CASE WHEN label = 'Glucose, Ascites' THEN was_abnormal END) AS glucose_ascites_was_abnormal
            , SUM(CASE WHEN label = 'Glucose, Ascites' THEN ratio_abnormal END) AS glucose_ascites_ratio_abnormal
        
            , SUM(CASE WHEN label = 'Glucose, Body Fluid' THEN min_value END) AS glucose_body_fluid_min_value
            , SUM(CASE WHEN label = 'Glucose, Body Fluid' THEN max_value END) AS glucose_body_fluid_max_value
            , SUM(CASE WHEN label = 'Glucose, Body Fluid' THEN avg_value END) AS glucose_body_fluid_avg_value
            , SUM(CASE WHEN label = 'Glucose, Body Fluid' THEN ref_range_lower END) AS glucose_body_fluid_ref_range_lower
            , SUM(CASE WHEN label = 'Glucose, Body Fluid' THEN ref_range_upper END) AS glucose_body_fluid_ref_range_upper
            , MIN(CASE WHEN label = 'Glucose, Body Fluid' THEN first_abnormal_charttime END) AS glucose_body_fluid_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Glucose, Body Fluid' THEN first_test_charttime END) AS glucose_body_fluid_first_test_charttime
            , MAX(CASE WHEN label = 'Glucose, Body Fluid' THEN last_test_charttime END) AS glucose_body_fluid_last_test_charttime
            --, 'mg/dL' AS glucose_body_fluid_valueuom
            , BOOL_OR(CASE WHEN label = 'Glucose, Body Fluid' THEN was_abnormal END) AS glucose_body_fluid_was_abnormal
            , SUM(CASE WHEN label = 'Glucose, Body Fluid' THEN ratio_abnormal END) AS glucose_body_fluid_ratio_abnormal
        
            , SUM(CASE WHEN label = 'Glucose, CSF' THEN min_value END) AS glucose_csf_min_value
            , SUM(CASE WHEN label = 'Glucose, CSF' THEN max_value END) AS glucose_csf_max_value
            , SUM(CASE WHEN label = 'Glucose, CSF' THEN avg_value END) AS glucose_csf_avg_value
            , SUM(CASE WHEN label = 'Glucose, CSF' THEN ref_range_lower END) AS glucose_csf_ref_range_lower
            , SUM(CASE WHEN label = 'Glucose, CSF' THEN ref_range_upper END) AS glucose_csf_ref_range_upper
            , MIN(CASE WHEN label = 'Glucose, CSF' THEN first_abnormal_charttime END) AS glucose_csf_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Glucose, CSF' THEN first_test_charttime END) AS glucose_csf_first_test_charttime
            , MAX(CASE WHEN label = 'Glucose, CSF' THEN last_test_charttime END) AS glucose_csf_last_test_charttime
            --, 'mg/dL' AS glucose_csf_valueuom
            , BOOL_OR(CASE WHEN label = 'Glucose, CSF' THEN was_abnormal END) AS glucose_csf_was_abnormal
            , SUM(CASE WHEN label = 'Glucose, CSF' THEN ratio_abnormal END) AS glucose_csf_ratio_abnormal
        
            , SUM(CASE WHEN label = 'Lactate' THEN min_value END) AS lactate_min_value
            , SUM(CASE WHEN label = 'Lactate' THEN max_value END) AS lactate_max_value
            , SUM(CASE WHEN label = 'Lactate' THEN avg_value END) AS lactate_avg_value
            , SUM(CASE WHEN label = 'Lactate' THEN ref_range_lower END) AS lactate_ref_range_lower
            , SUM(CASE WHEN label = 'Lactate' THEN ref_range_upper END) AS lactate_ref_range_upper
            , MIN(CASE WHEN label = 'Lactate' THEN first_abnormal_charttime END) AS lactate_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Lactate' THEN first_test_charttime END) AS lactate_first_test_charttime
            , MAX(CASE WHEN label = 'Lactate' THEN last_test_charttime END) AS lactate_last_test_charttime
            --, 'mmol/L' AS lactate_valueuom
            , BOOL_OR(CASE WHEN label = 'Lactate' THEN was_abnormal END) AS lactate_was_abnormal
            , SUM(CASE WHEN label = 'Lactate' THEN ratio_abnormal END) AS lactate_ratio_abnormal
        
            , SUM(CASE WHEN label = 'Lactate Dehydrogenase, CSF' THEN min_value END) AS lactate_dehydrogenase_csf_min_value
            , SUM(CASE WHEN label = 'Lactate Dehydrogenase, CSF' THEN max_value END) AS lactate_dehydrogenase_csf_max_value
            , SUM(CASE WHEN label = 'Lactate Dehydrogenase, CSF' THEN avg_value END) AS lactate_dehydrogenase_csf_avg_value
            , SUM(CASE WHEN label = 'Lactate Dehydrogenase, CSF' THEN ref_range_lower END) AS lactate_dehydrogenase_csf_ref_range_lower
            , SUM(CASE WHEN label = 'Lactate Dehydrogenase, CSF' THEN ref_range_upper END) AS lactate_dehydrogenase_csf_ref_range_upper
            , MIN(CASE WHEN label = 'Lactate Dehydrogenase, CSF' THEN first_abnormal_charttime END) AS lactate_dehydrogenase_csf_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Lactate Dehydrogenase, CSF' THEN first_test_charttime END) AS lactate_dehydrogenase_csf_first_test_charttime
            , MAX(CASE WHEN label = 'Lactate Dehydrogenase, CSF' THEN last_test_charttime END) AS lactate_dehydrogenase_csf_last_test_charttime
            --, 'IU/L' AS lactate_dehydrogenase_csf_valueuom
            , BOOL_OR(CASE WHEN label = 'Lactate Dehydrogenase, CSF' THEN was_abnormal END) AS lactate_dehydrogenase_csf_was_abnormal
            , SUM(CASE WHEN label = 'Lactate Dehydrogenase, CSF' THEN ratio_abnormal END) AS lactate_dehydrogenase_csf_ratio_abnormal
        
            , SUM(CASE WHEN label = 'Lactate Dehydrogenase (LD)' THEN min_value END) AS lactate_dehydrogenase_ld_min_value
            , SUM(CASE WHEN label = 'Lactate Dehydrogenase (LD)' THEN max_value END) AS lactate_dehydrogenase_ld_max_value
            , SUM(CASE WHEN label = 'Lactate Dehydrogenase (LD)' THEN avg_value END) AS lactate_dehydrogenase_ld_avg_value
            , SUM(CASE WHEN label = 'Lactate Dehydrogenase (LD)' THEN ref_range_lower END) AS lactate_dehydrogenase_ld_ref_range_lower
            , SUM(CASE WHEN label = 'Lactate Dehydrogenase (LD)' THEN ref_range_upper END) AS lactate_dehydrogenase_ld_ref_range_upper
            , MIN(CASE WHEN label = 'Lactate Dehydrogenase (LD)' THEN first_abnormal_charttime END) AS lactate_dehydrogenase_ld_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Lactate Dehydrogenase (LD)' THEN first_test_charttime END) AS lactate_dehydrogenase_ld_first_test_charttime
            , MAX(CASE WHEN label = 'Lactate Dehydrogenase (LD)' THEN last_test_charttime END) AS lactate_dehydrogenase_ld_last_test_charttime
            --, 'IU/L' AS lactate_dehydrogenase_ld_valueuom
            , BOOL_OR(CASE WHEN label = 'Lactate Dehydrogenase (LD)' THEN was_abnormal END) AS lactate_dehydrogenase_ld_was_abnormal
            , SUM(CASE WHEN label = 'Lactate Dehydrogenase (LD)' THEN ratio_abnormal END) AS lactate_dehydrogenase_ld_ratio_abnormal
        
            , SUM(CASE WHEN label = 'pH' THEN min_value END) AS ph_min_value
            , SUM(CASE WHEN label = 'pH' THEN max_value END) AS ph_max_value
            , SUM(CASE WHEN label = 'pH' THEN avg_value END) AS ph_avg_value
            , SUM(CASE WHEN label = 'pH' THEN ref_range_lower END) AS ph_ref_range_lower
            , SUM(CASE WHEN label = 'pH' THEN ref_range_upper END) AS ph_ref_range_upper
            , MIN(CASE WHEN label = 'pH' THEN first_abnormal_charttime END) AS ph_first_abnormal_charttime
            , MAX(CASE WHEN label = 'pH' THEN first_test_charttime END) AS ph_first_test_charttime
            , MAX(CASE WHEN label = 'pH' THEN last_test_charttime END) AS ph_last_test_charttime
            --, 'units' AS ph_valueuom
            , BOOL_OR(CASE WHEN label = 'pH' THEN was_abnormal END) AS ph_was_abnormal
            , SUM(CASE WHEN label = 'pH' THEN ratio_abnormal END) AS ph_ratio_abnormal
        
            -- not applicable for the current selection of data
            /*
            , SUM(CASE WHEN label = 'pH, Urine' THEN min_value END) AS ph_urine_min_value
            , SUM(CASE WHEN label = 'pH, Urine' THEN max_value END) AS ph_urine_max_value
            , SUM(CASE WHEN label = 'pH, Urine' THEN avg_value END) AS ph_urine_avg_value
            , SUM(CASE WHEN label = 'pH, Urine' THEN ref_range_lower END) AS ph_urine_ref_range_lower
            , SUM(CASE WHEN label = 'pH, Urine' THEN ref_range_upper END) AS ph_urine_ref_range_upper
            , SUM(CASE WHEN label = 'pH, Urine' THEN valueuom END) AS ph_urine_valueuom
            , BOOL_OR(CASE WHEN label = 'pH, Urine' THEN was_abnormal END) AS ph_urine_was_abnormal
            , SUM(CASE WHEN label = 'pH, Urine' THEN ratio_abnormal END) AS ph_urine_ratio_abnormal
             */
        
            , SUM(CASE WHEN label = 'pO2' THEN min_value END) AS po2_min_value
            , SUM(CASE WHEN label = 'pO2' THEN max_value END) AS po2_max_value
            , SUM(CASE WHEN label = 'pO2' THEN avg_value END) AS po2_avg_value
            , SUM(CASE WHEN label = 'pO2' THEN ref_range_lower END) AS po2_ref_range_lower
            , SUM(CASE WHEN label = 'pO2' THEN ref_range_upper END) AS po2_ref_range_upper
            , MIN(CASE WHEN label = 'pO2' THEN first_abnormal_charttime END) AS pO2_first_abnormal_charttime
            , MAX(CASE WHEN label = 'pO2' THEN first_test_charttime END) AS pO2_first_test_charttime
            , MAX(CASE WHEN label = 'pO2' THEN last_test_charttime END) AS pO2_last_test_charttime
            --, 'mm Hg' AS po2_valueuom
            , BOOL_OR(CASE WHEN label = 'pO2' THEN was_abnormal END) AS po2_was_abnormal
            , SUM(CASE WHEN label = 'pO2' THEN ratio_abnormal END) AS po2_ratio_abnormal
        
            -- not applicable for the current selection of data
            /*
            , SUM(CASE WHEN label = 'pO2, Body Fluid' THEN min_value END) AS po2_body_fluid_min_value
            , SUM(CASE WHEN label = 'pO2, Body Fluid' THEN max_value END) AS po2_body_fluid_max_value
            , SUM(CASE WHEN label = 'pO2, Body Fluid' THEN avg_value END) AS po2_body_fluid_avg_value
            , SUM(CASE WHEN label = 'pO2, Body Fluid' THEN ref_range_lower END) AS po2_body_fluid_ref_range_lower
            , SUM(CASE WHEN label = 'pO2, Body Fluid' THEN ref_range_upper END) AS po2_body_fluid_ref_range_upper
            , SUM(CASE WHEN label = 'pO2, Body Fluid' THEN valueuom END) AS po2_body_fluid_valueuom
            , BOOL_OR(CASE WHEN label = 'pO2, Body Fluid' THEN was_abnormal END) AS po2_body_fluid_was_abnormal
            , SUM(CASE WHEN label = 'pO2, Body Fluid' THEN ratio_abnormal END) AS po2_body_fluid_ratio_abnormal
             */
        
            -- not applicable for the current selection of data
            /*
            , SUM(CASE WHEN label = 'pCO2, Body Fluid' THEN min_value END) AS pco2_body_fluid_min_value
            , SUM(CASE WHEN label = 'pCO2, Body Fluid' THEN max_value END) AS pco2_body_fluid_max_value
            , SUM(CASE WHEN label = 'pCO2, Body Fluid' THEN avg_value END) AS pco2_body_fluid_avg_value
            , SUM(CASE WHEN label = 'pCO2, Body Fluid' THEN ref_range_lower END) AS pco2_body_fluid_ref_range_lower
            , SUM(CASE WHEN label = 'pCO2, Body Fluid' THEN ref_range_upper END) AS pco2_body_fluid_ref_range_upper
            , SUM(CASE WHEN label = 'pCO2, Body Fluid' THEN valueuom END) AS pco2_body_fluid_valueuom
            , SUM(CASE WHEN label = 'pCO2, Body Fluid' THEN was_abnormal END) AS pco2_body_fluid_was_abnormal
            , SUM(CASE WHEN label = 'pCO2, Body Fluid' THEN ratio_abnormal END) AS pco2_body_fluid_ratio_abnormal
             */
        
            , SUM(CASE WHEN label = 'Bicarbonate' THEN min_value END) AS bicarbonate_min_value
            , SUM(CASE WHEN label = 'Bicarbonate' THEN max_value END) AS bicarbonate_max_value
            , SUM(CASE WHEN label = 'Bicarbonate' THEN avg_value END) AS Bicarbonate_avg_value
            , SUM(CASE WHEN label = 'Bicarbonate' THEN ref_range_lower END) AS bicarbonate_ref_range_lower
            , SUM(CASE WHEN label = 'Bicarbonate' THEN ref_range_upper END) AS bicarbonate_ref_range_upper
            , MIN(CASE WHEN label = 'Bicarbonate' THEN first_abnormal_charttime END) AS bicarbonate_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Bicarbonate' THEN first_test_charttime END) AS bicarbonate_first_test_charttime
            , MAX(CASE WHEN label = 'Bicarbonate' THEN last_test_charttime END) AS bicarbonate_last_test_charttime
            --, 'mEq/L' AS bicarbonate_valueuom
            , BOOL_OR(CASE WHEN label = 'Bicarbonate' THEN was_abnormal END) AS bicarbonate_was_abnormal
            , SUM(CASE WHEN label = 'Bicarbonate' THEN ratio_abnormal END) AS bicarbonate_ratio_abnormal
        
            -- not applicable for the current selection of data
            /*
            , SUM(CASE WHEN label = 'Bicarbonate, CSF' THEN min_value END) AS bicarbonate_csf_min_value
            , SUM(CASE WHEN label = 'Bicarbonate, CSF' THEN max_value END) AS bicarbonate_csf_max_value
            , SUM(CASE WHEN label = 'Bicarbonate, CSF' THEN avg_value END) AS bicarbonate_csf_avg_value
            , SUM(CASE WHEN label = 'Bicarbonate, CSF' THEN ref_range_lower END) AS bicarbonate_csf_ref_range_lower
            , SUM(CASE WHEN label = 'Bicarbonate, CSF' THEN ref_range_upper END) AS bicarbonate_csf_ref_range_upper
            , SUM(CASE WHEN label = 'Bicarbonate, CSF' THEN valueuom END) AS bicarbonate_csf_valueuom
            , BOOL_OR(CASE WHEN label = 'Bicarbonate, CSF' THEN was_abnormal END) AS bicarbonate_csf_was_abnormal
            , SUM(CASE WHEN label = 'Bicarbonate, CSF' THEN ratio_abnormal END) AS bicarbonate_csf_ratio_abnormal
             */
        
            , SUM(CASE WHEN label = 'Bicarbonate, Urine' THEN min_value END) AS bicarbonate_urine_min_value
            , SUM(CASE WHEN label = 'Bicarbonate, Urine' THEN max_value END) AS bicarbonate_urine_max_value
            , SUM(CASE WHEN label = 'Bicarbonate, Urine' THEN avg_value END) AS bicarbonate_urine_avg_value
            , SUM(CASE WHEN label = 'Bicarbonate, Urine' THEN ref_range_lower END) AS bicarbonate_urine_ref_range_lower
            , SUM(CASE WHEN label = 'Bicarbonate, Urine' THEN ref_range_upper END) AS bicarbonate_urine_ref_range_upper
            , MIN(CASE WHEN label = 'Bicarbonate, Urine' THEN first_abnormal_charttime END) AS bicarbonate_urine_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Bicarbonate, Urine' THEN first_test_charttime END) AS bicarbonate_urine_first_test_charttime
            , MAX(CASE WHEN label = 'Bicarbonate, Urine' THEN last_test_charttime END) AS bicarbonate_urine_last_test_charttime
            --, 'mEq/L' AS bicarbonate_urine_valueuom
            , BOOL_OR(CASE WHEN label = 'Bicarbonate, Urine' THEN was_abnormal END) AS bicarbonate_urine_was_abnormal
            , SUM(CASE WHEN label = 'Bicarbonate, Urine' THEN ratio_abnormal END) AS bicarbonate_urine_ratio_abnormal
        
            -- not applicable for the current selection of data
            /*
            , SUM(CASE WHEN label = 'Calculated Bicarbonate' THEN min_value END) AS calculated_bicarbonate_min_value
            , SUM(CASE WHEN label = 'Calculated Bicarbonate' THEN max_value END) AS calculated_bicarbonate_max_value
            , SUM(CASE WHEN label = 'Calculated Bicarbonate' THEN avg_value END) AS calculated_bicarbonate_avg_value
            , SUM(CASE WHEN label = 'Calculated Bicarbonate' THEN ref_range_lower END) AS calculated_bicarbonate_ref_range_lower
            , SUM(CASE WHEN label = 'Calculated Bicarbonate' THEN ref_range_upper END) AS calculated_bicarbonate_ref_range_upper
            , SUM(CASE WHEN label = 'Calculated Bicarbonate' THEN valueuom END) AS calculated_bicarbonate_valueuom
            , BOOL_OR(CASE WHEN label = 'Calculated Bicarbonate' THEN was_abnormal END) AS calculated_bicarbonate_was_abnormal
            , SUM(CASE WHEN label = 'Calculated Bicarbonate' THEN ratio_abnormal END) AS calculated_bicarbonate_ratio_abnormal
             */
        
            , SUM(CASE WHEN label = 'Calculated Bicarbonate, Whole Blood' THEN min_value END) AS calculated_bicarbonate_whole_blood_min_value
            , SUM(CASE WHEN label = 'Calculated Bicarbonate, Whole Blood' THEN max_value END) AS calculated_bicarbonate_whole_blood_max_value
            , SUM(CASE WHEN label = 'Calculated Bicarbonate, Whole Blood' THEN avg_value END) AS calculated_bicarbonate_whole_blood_avg_value
            , SUM(CASE WHEN label = 'Calculated Bicarbonate, Whole Blood' THEN ref_range_lower END) AS calculated_bicarbonate_whole_blood_ref_range_lower
            , SUM(CASE WHEN label = 'Calculated Bicarbonate, Whole Blood' THEN ref_range_upper END) AS calculated_bicarbonate_whole_blood_ref_range_upper
            , MIN(CASE WHEN label = 'Calculated Bicarbonate, Whole Blood' THEN first_abnormal_charttime END) AS calculated_bicarbonate_whole_blood_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Calculated Bicarbonate, Whole Blood' THEN first_test_charttime END) AS calculated_bicarbonate_whole_blood_first_test_charttime
            , MAX(CASE WHEN label = 'Calculated Bicarbonate, Whole Blood' THEN last_test_charttime END) AS calculated_bicarbonate_whole_blood_last_test_charttime
            --, 'mEq/L' AS calculated_bicarbonate_whole_blood_valueuom
            , BOOL_OR(CASE WHEN label = 'Calculated Bicarbonate, Whole Blood' THEN was_abnormal END) AS calculated_bicarbonate_whole_blood_was_abnormal
            , SUM(CASE WHEN label = 'Calculated Bicarbonate, Whole Blood' THEN ratio_abnormal END) AS calculated_bicarbonate_whole_blood_ratio_abnormal
        
            , SUM(CASE WHEN label = 'INR(PT)' THEN min_value END) AS inr_pt_min_value
            , SUM(CASE WHEN label = 'INR(PT)' THEN max_value END) AS inr_pt_max_value
            , SUM(CASE WHEN label = 'INR(PT)' THEN avg_value END) AS inr_pt_avg_value
            , SUM(CASE WHEN label = 'INR(PT)' THEN ref_range_lower END) AS inr_pt_ref_range_lower
            , SUM(CASE WHEN label = 'INR(PT)' THEN ref_range_upper END) AS inr_pt_ref_range_upper
            , MIN(CASE WHEN label = 'INR(PT)' THEN first_abnormal_charttime END) AS inr_pt_first_abnormal_charttime
            , MAX(CASE WHEN label = 'INR(PT)' THEN first_test_charttime END) AS inr_pt_first_test_charttime
            , MAX(CASE WHEN label = 'INR(PT)' THEN last_test_charttime END) AS inr_pt_last_test_charttime
            --, NULL AS inr_pt_valueuom
            , BOOL_OR(CASE WHEN label = 'INR(PT)' THEN was_abnormal END) AS inr_pt_was_abnormal
            , SUM(CASE WHEN label = 'INR(PT)' THEN ratio_abnormal END) AS inr_pt_ratio_abnormal
        
            , SUM(CASE WHEN label = 'PT' THEN min_value END) AS pt_min_value
            , SUM(CASE WHEN label = 'PT' THEN max_value END) AS pt_max_value
            , SUM(CASE WHEN label = 'PT' THEN avg_value END) AS pt_avg_value
            , SUM(CASE WHEN label = 'PT' THEN ref_range_lower END) AS pt_ref_range_lower
            , SUM(CASE WHEN label = 'PT' THEN ref_range_upper END) AS pt_ref_range_upper
            , MIN(CASE WHEN label = 'PT' THEN first_abnormal_charttime END) AS pt_first_abnormal_charttime
            , MAX(CASE WHEN label = 'PT' THEN first_test_charttime END) AS pt_first_test_charttime
            , MAX(CASE WHEN label = 'PT' THEN last_test_charttime END) AS pt_last_test_charttime
            --, 'sec' AS pt_valueuom
            , BOOL_OR(CASE WHEN label = 'PT' THEN was_abnormal END) AS pt_was_abnormal
            , SUM(CASE WHEN label = 'PT' THEN ratio_abnormal END) AS pt_ratio_abnormal
        
            , SUM(CASE WHEN label = 'PTT' THEN min_value END) AS ptt_min_value
            , SUM(CASE WHEN label = 'PTT' THEN max_value END) AS ptt_max_value
            , SUM(CASE WHEN label = 'PTT' THEN avg_value END) AS ptt_avg_value
            , SUM(CASE WHEN label = 'PTT' THEN ref_range_lower END) AS ptt_ref_range_lower
            , SUM(CASE WHEN label = 'PTT' THEN ref_range_upper END) AS ptt_ref_range_upper
            , MIN(CASE WHEN label = 'PTT' THEN first_abnormal_charttime END) AS ptt_first_abnormal_charttime
            , MAX(CASE WHEN label = 'PTT' THEN first_test_charttime END) AS ptt_first_test_charttime
            , MAX(CASE WHEN label = 'PTT' THEN last_test_charttime END) AS ptt_last_test_charttime
            --, 'sec' AS ptt_valueuom
            , BOOL_OR(CASE WHEN label = 'PTT' THEN was_abnormal END) AS ptt_was_abnormal
            , SUM(CASE WHEN label = 'PTT' THEN ratio_abnormal END) AS ptt_ratio_abnormal
        
            , SUM(CASE WHEN label = 'C-Reactive Protein' THEN min_value END) AS crp_min_value
            , SUM(CASE WHEN label = 'C-Reactive Protein' THEN max_value END) AS crp_max_value
            , SUM(CASE WHEN label = 'C-Reactive Protein' THEN avg_value END) AS crp_avg_value
            , SUM(CASE WHEN label = 'C-Reactive Protein' THEN ref_range_lower END) AS crp_ref_range_lower
            , SUM(CASE WHEN label = 'C-Reactive Protein' THEN ref_range_upper END) AS crp_ref_range_upper
            , MIN(CASE WHEN label = 'C-Reactive Protein' THEN first_abnormal_charttime END) AS crp_first_abnormal_charttime
            , MAX(CASE WHEN label = 'C-Reactive Protein' THEN first_test_charttime END) AS crp_first_test_charttime
            , MAX(CASE WHEN label = 'C-Reactive Protein' THEN last_test_charttime END) AS crp_last_test_charttime
            --, 'mg/L' AS crp_valueuom
            , BOOL_OR(CASE WHEN label = 'C-Reactive Protein' THEN was_abnormal END) AS crp_was_abnormal
            , SUM(CASE WHEN label = 'C-Reactive Protein' THEN ratio_abnormal END) AS crp_ratio_abnormal
        
            , SUM(CASE WHEN label = 'White Blood Cells' THEN min_value END) AS white_blood_cells_min_value
            , SUM(CASE WHEN label = 'White Blood Cells' THEN max_value END) AS white_blood_cells_max_value
            , SUM(CASE WHEN label = 'White Blood Cells' THEN avg_value END) AS white_blood_cells_avg_value
            , SUM(CASE WHEN label = 'White Blood Cells' THEN ref_range_lower END) AS white_blood_cells_ref_range_lower
            , SUM(CASE WHEN label = 'White Blood Cells' THEN ref_range_upper END) AS white_blood_cells_ref_range_upper
            , MIN(CASE WHEN label = 'White Blood Cells' THEN first_abnormal_charttime END) AS white_blood_cells_first_abnormal_charttime
            , MAX(CASE WHEN label = 'White Blood Cells' THEN first_test_charttime END) AS white_blood_cells_first_test_charttime
            , MAX(CASE WHEN label = 'White Blood Cells' THEN last_test_charttime END) AS white_blood_cells_last_test_charttime
            --, 'K/uL' AS white_blood_cells_valueuom
            , BOOL_OR(CASE WHEN label = 'White Blood Cells' THEN was_abnormal END) AS white_blood_cells_was_abnormal
            , SUM(CASE WHEN label = 'White Blood Cells' THEN ratio_abnormal END) AS white_blood_cells_ratio_abnormal
        
            , SUM(CASE WHEN label = 'Ammonia' THEN min_value END) AS ammonia_min_value
            , SUM(CASE WHEN label = 'Ammonia' THEN max_value END) AS ammonia_max_value
            , SUM(CASE WHEN label = 'Ammonia' THEN avg_value END) AS ammonia_avg_value
            , SUM(CASE WHEN label = 'Ammonia' THEN ref_range_lower END) AS ammonia_ref_range_lower
            , SUM(CASE WHEN label = 'Ammonia' THEN ref_range_upper END) AS ammonia_ref_range_upper
            , MIN(CASE WHEN label = 'Ammonia' THEN first_abnormal_charttime END) AS ammonia_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Ammonia' THEN first_test_charttime END) AS ammonia_first_test_charttime
            , MAX(CASE WHEN label = 'Ammonia' THEN last_test_charttime END) AS ammonia_last_test_charttime
            --, 'umol/L' AS ammonia_valueuom
            , BOOL_OR(CASE WHEN label = 'Ammonia' THEN was_abnormal END) AS ammonia_was_abnormal
            , SUM(CASE WHEN label = 'Ammonia' THEN ratio_abnormal END) AS ammonia_ratio_abnormal
        
            , SUM(CASE WHEN label = 'Albumin' THEN min_value END) AS albumin_min_value
            , SUM(CASE WHEN label = 'Albumin' THEN max_value END) AS albumin_max_value
            , SUM(CASE WHEN label = 'Albumin' THEN avg_value END) AS albumin_avg_value
            , SUM(CASE WHEN label = 'Albumin' THEN ref_range_lower END) AS albumin_ref_range_lower
            , SUM(CASE WHEN label = 'Albumin' THEN ref_range_upper END) AS albumin_ref_range_upper
            , MIN(CASE WHEN label = 'Albumin' THEN first_abnormal_charttime END) AS albumin_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Albumin' THEN first_test_charttime END) AS albumin_first_test_charttime
            , MAX(CASE WHEN label = 'Albumin' THEN last_test_charttime END) AS albumin_last_test_charttime
            --, 'g/dL' AS albumin_valueuom
            , BOOL_OR(CASE WHEN label = 'Albumin' THEN was_abnormal END) AS albumin_was_abnormal
            , SUM(CASE WHEN label = 'Albumin' THEN ratio_abnormal END) AS albumin_ratio_abnormal
        
            -- not applicable for the current selection of data
            /*
            , SUM(CASE WHEN label = 'Albumin, Blood' THEN min_value END) AS albumin_blood_min_value
            , SUM(CASE WHEN label = 'Albumin, Blood' THEN max_value END) AS albumin_blood_max_value
            , SUM(CASE WHEN label = 'Albumin, Blood' THEN avg_value END) AS albumin_blood_avg_value
            , SUM(CASE WHEN label = 'Albumin, Blood' THEN ref_range_lower END) AS albumin_blood_ref_range_lower
            , SUM(CASE WHEN label = 'Albumin, Blood' THEN ref_range_upper END) AS albumin_blood_ref_range_upper
            , SUM(CASE WHEN label = 'Albumin, Blood' THEN valueuom END) AS albumin_blood_valueuom
            , BOOL_OR(CASE WHEN label = 'Albumin, Blood' THEN was_abnormal END) AS albumin_blood_was_abnormal
            , SUM(CASE WHEN label = 'Albumin, Blood' THEN ratio_abnormal END) AS albumin_blood_ratio_abnormal
             */
        
            , SUM(CASE WHEN label = 'Albumin/Creatinine, Urine' THEN min_value END) AS albumin_creatine_urine_min_value
            , SUM(CASE WHEN label = 'Albumin/Creatinine, Urine' THEN max_value END) AS albumin_creatine_urine_max_value
            , SUM(CASE WHEN label = 'Albumin/Creatinine, Urine' THEN avg_value END) AS albumin_creatine_urine_avg_value
            , SUM(CASE WHEN label = 'Albumin/Creatinine, Urine' THEN ref_range_lower END) AS albumin_creatine_urine_ref_range_lower
            , SUM(CASE WHEN label = 'Albumin/Creatinine, Urine' THEN ref_range_upper END) AS albumin_creatine_urine_ref_range_upper
            , MIN(CASE WHEN label = 'Albumin/Creatinine, Urine' THEN first_abnormal_charttime END) AS albumin_creatine_urine_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Albumin/Creatinine, Urine' THEN first_test_charttime END) AS albumin_creatine_urine_first_test_charttime
            , MAX(CASE WHEN label = 'Albumin/Creatinine, Urine' THEN last_test_charttime END) AS albumin_creatine_urine_last_test_charttime
            --, 'mg/g' AS albumin_creatine_urine_valueuom
            , BOOL_OR(CASE WHEN label = 'Albumin/Creatinine, Urine' THEN was_abnormal END) AS albumin_creatine_urine_was_abnormal
            , SUM(CASE WHEN label = 'Albumin/Creatinine, Urine' THEN ratio_abnormal END) AS albumin_creatine_urine_ratio_abnormal
        
            -- not applicable for the current selection of data
            /*
            , SUM(CASE WHEN label = 'Albumin, CSF' THEN min_value END) AS albumin_csf_min_value
            , SUM(CASE WHEN label = 'Albumin, CSF' THEN max_value END) AS albumin_csf_max_value
            , SUM(CASE WHEN label = 'Albumin, CSF' THEN avg_value END) AS albumin_csf_avg_value
            , SUM(CASE WHEN label = 'Albumin, CSF' THEN ref_range_lower END) AS albumin_csf_ref_range_lower
            , SUM(CASE WHEN label = 'Albumin, CSF' THEN ref_range_upper END) AS albumin_csf_ref_range_upper
            , SUM(CASE WHEN label = 'Albumin, CSF' THEN valueuom END) AS albumin_csf_valueuom
            , BOOL_OR(CASE WHEN label = 'Albumin, CSF' THEN was_abnormal END) AS albumin_csf_was_abnormal
            , SUM(CASE WHEN label = 'Albumin, CSF' THEN ratio_abnormal END) AS albumin_csf_ratio_abnormal
             */
        
            , SUM(CASE WHEN label = 'Creatinine' THEN min_value END) AS creatinine_min_value
            , SUM(CASE WHEN label = 'Creatinine' THEN max_value END) AS creatinine_max_value
            , SUM(CASE WHEN label = 'Creatinine' THEN avg_value END) AS creatinine_avg_value
            , SUM(CASE WHEN label = 'Creatinine' THEN ref_range_lower END) AS creatinine_ref_range_lower
            , SUM(CASE WHEN label = 'Creatinine' THEN ref_range_upper END) AS creatinine_ref_range_upper
            , MIN(CASE WHEN label = 'Creatinine' THEN first_abnormal_charttime END) AS creatinine_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Creatinine' THEN first_test_charttime END) AS creatinine_first_test_charttime
            , MAX(CASE WHEN label = 'Creatinine' THEN last_test_charttime END) AS creatinine_last_test_charttime
            --, 'mg/dL' AS creatinine_valueuom
            , BOOL_OR(CASE WHEN label = 'Creatinine' THEN was_abnormal END) AS creatinine_was_abnormal
            , SUM(CASE WHEN label = 'Creatinine' THEN ratio_abnormal END) AS creatinine_ratio_abnormal
        
            -- not applicable for the current selection of data
            /*
            , SUM(CASE WHEN label = 'Creatinine, Blood' THEN min_value END) AS creatinine_blood_min_value
            , SUM(CASE WHEN label = 'Creatinine, Blood' THEN max_value END) AS creatinine_blood_max_value
            , SUM(CASE WHEN label = 'Creatinine, Blood' THEN avg_value END) AS creatinine_blood_avg_value
            , SUM(CASE WHEN label = 'Creatinine, Blood' THEN ref_range_lower END) AS creatinine_blood_ref_range_lower
            , SUM(CASE WHEN label = 'Creatinine, Blood' THEN ref_range_upper END) AS creatinine_blood_ref_range_upper
            , SUM(CASE WHEN label = 'Creatinine, Blood' THEN valueuom END) AS creatinine_blood_valueuom
            , BOOL_OR(CASE WHEN label = 'Creatinine, Blood' THEN was_abnormal END) AS creatinine_blood_was_abnormal
            , SUM(CASE WHEN label = 'Creatinine, Blood' THEN ratio_abnormal END) AS creatinine_blood_ratio_abnormal
             */
        
            , SUM(CASE WHEN label = 'Creatinine Clearance' THEN min_value END) AS creatinine_clearance_min_value
            , SUM(CASE WHEN label = 'Creatinine Clearance' THEN max_value END) AS creatinine_clearance_max_value
            , SUM(CASE WHEN label = 'Creatinine Clearance' THEN avg_value END) AS creatinine_clearance_avg_value
            , SUM(CASE WHEN label = 'Creatinine Clearance' THEN ref_range_lower END) AS creatinine_clearance_ref_range_lower
            , SUM(CASE WHEN label = 'Creatinine Clearance' THEN ref_range_upper END) AS creatinine_clearance_ref_range_upper
            , MIN(CASE WHEN label = 'Creatinine Clearance' THEN first_abnormal_charttime END) AS creatinine_clearance_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Creatinine Clearance' THEN first_test_charttime END) AS creatinine_clearance_first_test_charttime
            , MAX(CASE WHEN label = 'Creatinine Clearance' THEN last_test_charttime END) AS creatinine_clearance_last_test_charttime
            --, 'mL/min' AS creatinine_clearance_valueuom
            , BOOL_OR(CASE WHEN label = 'Creatinine Clearance' THEN was_abnormal END) AS creatinine_clearance_was_abnormal
            , SUM(CASE WHEN label = 'Creatinine Clearance' THEN ratio_abnormal END) AS creatinine_clearance_ratio_abnormal
        
            -- not applicable for the current selection of data
            /*
            , SUM(CASE WHEN label = 'Creatinine, CSF' THEN min_value END) AS creatinine_csf_min_value
            , SUM(CASE WHEN label = 'Creatinine, CSF' THEN max_value END) AS creatinine_csf_max_value
            , SUM(CASE WHEN label = 'Creatinine, CSF' THEN avg_value END) AS creatinine_csf_avg_value
            , SUM(CASE WHEN label = 'Creatinine, CSF' THEN ref_range_lower END) AS creatinine_csf_ref_range_lower
            , SUM(CASE WHEN label = 'Creatinine, CSF' THEN ref_range_upper END) AS creatinine_csf_ref_range_upper
            , SUM(CASE WHEN label = 'Creatinine, CSF' THEN valueuom END) AS creatinine_csf_valueuom
            , BOOL_OR(CASE WHEN label = 'Creatinine, CSF' THEN was_abnormal END) AS creatinine_csf_was_abnormal
            , SUM(CASE WHEN label = 'Creatinine, CSF' THEN ratio_abnormal END) AS creatinine_csf_ratio_abnormal
             */
        
            , SUM(CASE WHEN label = 'Creatinine, Serum' THEN min_value END) AS creatinine_serum_min_value
            , SUM(CASE WHEN label = 'Creatinine, Serum' THEN max_value END) AS creatinine_serum_max_value
            , SUM(CASE WHEN label = 'Creatinine, Serum' THEN avg_value END) AS creatinine_serum_avg_value
            , SUM(CASE WHEN label = 'Creatinine, Serum' THEN ref_range_lower END) AS creatinine_serum_ref_range_lower
            , SUM(CASE WHEN label = 'Creatinine, Serum' THEN ref_range_upper END) AS creatinine_serum_ref_range_upper
            , MIN(CASE WHEN label = 'Creatinine, Serum' THEN first_abnormal_charttime END) AS creatinine_serum_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Creatinine, Serum' THEN first_test_charttime END) AS creatinine_serum_first_test_charttime
            , MAX(CASE WHEN label = 'Creatinine, Serum' THEN last_test_charttime END) AS creatinine_serum_last_test_charttime
            --, 'mg/dL' AS creatinine_serum_valueuom
            , BOOL_OR(CASE WHEN label = 'Creatinine, Serum' THEN was_abnormal END) AS creatinine_serum_was_abnormal
            , SUM(CASE WHEN label = 'Creatinine, Serum' THEN ratio_abnormal END) AS creatinine_serum_ratio_abnormal
        
            , SUM(CASE WHEN label = 'Urea Nitrogen' THEN min_value END) AS urea_nitrogen_min_value
            , SUM(CASE WHEN label = 'Urea Nitrogen' THEN max_value END) AS urea_nitrogen_max_value
            , SUM(CASE WHEN label = 'Urea Nitrogen' THEN avg_value END) AS urea_nitrogen_avg_value
            , SUM(CASE WHEN label = 'Urea Nitrogen' THEN ref_range_lower END) AS urea_nitrogen_ref_range_lower
            , SUM(CASE WHEN label = 'Urea Nitrogen' THEN ref_range_upper END) AS urea_nitrogen_ref_range_upper
            , MIN(CASE WHEN label = 'Urea Nitrogen' THEN first_abnormal_charttime END) AS urea_nitrogen_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Urea Nitrogen' THEN first_test_charttime END) AS urea_nitrogen_first_test_charttime
            , MAX(CASE WHEN label = 'Urea Nitrogen' THEN last_test_charttime END) AS urea_nitrogen_last_test_charttime
            --, 'mg/dL' AS urea_nitrogen_valueuom
            , BOOL_OR(CASE WHEN label = 'Urea Nitrogen' THEN was_abnormal END) AS urea_nitrogen_was_abnormal
            , SUM(CASE WHEN label = 'Urea Nitrogen' THEN ratio_abnormal END) AS urea_nitrogen_ratio_abnormal
        
            -- not applicable for the current selection of data
            /*
            , SUM(CASE WHEN label = 'Urea Nitrogen, CSF' THEN min_value END) AS urea_nitrogen_csf_min_value
            , SUM(CASE WHEN label = 'Urea Nitrogen, CSF' THEN max_value END) AS urea_nitrogen_csf_max_value
            , SUM(CASE WHEN label = 'Urea Nitrogen, CSF' THEN avg_value END) AS urea_nitrogen_csf_avg_value
            , SUM(CASE WHEN label = 'Urea Nitrogen, CSF' THEN ref_range_lower END) AS urea_nitrogen_csf_ref_range_lower
            , SUM(CASE WHEN label = 'Urea Nitrogen, CSF' THEN ref_range_upper END) AS urea_nitrogen_csf_ref_range_upper
            , SUM(CASE WHEN label = 'Urea Nitrogen, CSF' THEN valueuom END) AS urea_nitrogen_csf_valueuom
            , BOOL_OR(CASE WHEN label = 'Urea Nitrogen, CSF' THEN was_abnormal END) AS urea_nitrogen_csf_was_abnormal
            , SUM(CASE WHEN label = 'Urea Nitrogen, CSF' THEN ratio_abnormal END) AS urea_nitrogen_csf_ratio_abnormal
             */
        
            -- not applicable for the current selection of data
            /*
            , SUM(CASE WHEN label = 'Alanine' THEN min_value END) AS alanine_min_value
            , SUM(CASE WHEN label = 'Alanine' THEN max_value END) AS alanine_max_value
            , SUM(CASE WHEN label = 'Alanine' THEN avg_value END) AS alanine_avg_value
            , SUM(CASE WHEN label = 'Alanine' THEN ref_range_lower END) AS alanine_ref_range_lower
            , SUM(CASE WHEN label = 'Alanine' THEN ref_range_upper END) AS alanine_ref_range_upper
            , SUM(CASE WHEN label = 'Alanine' THEN valueuom END) AS alanine_valueuom
            , BOOL_OR(CASE WHEN label = 'Alanine' THEN was_abnormal END) AS alanine_was_abnormal
            , SUM(CASE WHEN label = 'Alanine' THEN ratio_abnormal END) AS alanine_ratio_abnormal
             */
        
            , SUM(CASE WHEN label = 'Alanine Aminotransferase (ALT)' THEN min_value END) AS alt_min_value
            , SUM(CASE WHEN label = 'Alanine Aminotransferase (ALT)' THEN max_value END) AS alt_max_value
            , SUM(CASE WHEN label = 'Alanine Aminotransferase (ALT)' THEN avg_value END) AS alt_avg_value
            , SUM(CASE WHEN label = 'Alanine Aminotransferase (ALT)' THEN ref_range_lower END) AS alt_ref_range_lower
            , SUM(CASE WHEN label = 'Alanine Aminotransferase (ALT)' THEN ref_range_upper END) AS alt_ref_range_upper
            , MIN(CASE WHEN label = 'Alanine Aminotransferase (ALT)' THEN first_abnormal_charttime END) AS alt_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Alanine Aminotransferase (ALT)' THEN first_test_charttime END) AS alt_first_test_charttime
            , MAX(CASE WHEN label = 'Alanine Aminotransferase (ALT)' THEN last_test_charttime END) AS alt_last_test_charttime
            --, 'IU/L' AS alt_valueuom
            , BOOL_OR(CASE WHEN label = 'Alanine Aminotransferase (ALT)' THEN was_abnormal END) AS alt_was_abnormal
            , SUM(CASE WHEN label = 'Alanine Aminotransferase (ALT)' THEN ratio_abnormal END) AS alt_ratio_abnormal
        
            , SUM(CASE WHEN label = 'Asparate Aminotransferase (AST)' THEN min_value END) AS ast_min_value
            , SUM(CASE WHEN label = 'Asparate Aminotransferase (AST)' THEN max_value END) AS ast_max_value
            , SUM(CASE WHEN label = 'Asparate Aminotransferase (AST)' THEN avg_value END) AS ast_avg_value
            , SUM(CASE WHEN label = 'Asparate Aminotransferase (AST)' THEN ref_range_lower END) AS ast_ref_range_lower
            , SUM(CASE WHEN label = 'Asparate Aminotransferase (AST)' THEN ref_range_upper END) AS ast_ref_range_upper
            , MIN(CASE WHEN label = 'Asparate Aminotransferase (AST)' THEN first_abnormal_charttime END) AS ast_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Asparate Aminotransferase (AST)' THEN first_test_charttime END) AS ast_first_test_charttime
            , MAX(CASE WHEN label = 'Asparate Aminotransferase (AST)' THEN last_test_charttime END) AS ast_last_test_charttime
            --, 'IU/L' AS ast_valueuom
            , BOOL_OR(CASE WHEN label = 'Asparate Aminotransferase (AST)' THEN was_abnormal END) AS ast_was_abnormal
            , SUM(CASE WHEN label = 'Asparate Aminotransferase (AST)' THEN ratio_abnormal END) AS ast_ratio_abnormal
        
            , SUM(CASE WHEN label = 'Bilirubin' THEN min_value END) AS bilirubin_min_value
            , SUM(CASE WHEN label = 'Bilirubin' THEN max_value END) AS bilirubin_max_value
            , SUM(CASE WHEN label = 'Bilirubin' THEN avg_value END) AS bilirubin_avg_value
            , SUM(CASE WHEN label = 'Bilirubin' THEN ref_range_lower END) AS bilirubin_ref_range_lower
            , SUM(CASE WHEN label = 'Bilirubin' THEN ref_range_upper END) AS bilirubin_ref_range_upper
            , MIN(CASE WHEN label = 'Bilirubin' THEN first_abnormal_charttime END) AS bilirubin_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Bilirubin' THEN first_test_charttime END) AS bilirubin_first_test_charttime
            , MAX(CASE WHEN label = 'Bilirubin' THEN last_test_charttime END) AS bilirubin_last_test_charttime
            --, 'mg/dL' AS bilirubin_valueuom
            , BOOL_OR(CASE WHEN label = 'Bilirubin' THEN was_abnormal END) AS bilirubin_was_abnormal
            , SUM(CASE WHEN label = 'Bilirubin' THEN ratio_abnormal END) AS bilirubin_ratio_abnormal
        
            , SUM(CASE WHEN label = 'Bilirubin, Direct' THEN min_value END) AS bilirubin_direct_min_value
            , SUM(CASE WHEN label = 'Bilirubin, Direct' THEN max_value END) AS bilirubin_direct_max_value
            , SUM(CASE WHEN label = 'Bilirubin, Direct' THEN avg_value END) AS bilirubin_direct_avg_value
            , SUM(CASE WHEN label = 'Bilirubin, Direct' THEN ref_range_lower END) AS bilirubin_direct_ref_range_lower
            , SUM(CASE WHEN label = 'Bilirubin, Direct' THEN ref_range_upper END) AS bilirubin_direct_ref_range_upper
            , MIN(CASE WHEN label = 'Bilirubin, Direct' THEN first_abnormal_charttime END) AS bilirubin_direct_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Bilirubin, Direct' THEN first_test_charttime END) AS bilirubin_direct_first_test_charttime
            , MAX(CASE WHEN label = 'Bilirubin, Direct' THEN last_test_charttime END) AS bilirubin_direct_last_test_charttime
            --, 'mg/dL' AS bilirubin_direct_valueuom
            , BOOL_OR(CASE WHEN label = 'Bilirubin, Direct' THEN was_abnormal END) AS bilirubin_direct_was_abnormal
            , SUM(CASE WHEN label = 'Bilirubin, Direct' THEN ratio_abnormal END) AS bilirubin_direct_ratio_abnormal
        
            , SUM(CASE WHEN label = 'Bilirubin, Indirect' THEN min_value END) AS bilirubin_indirect_min_value
            , SUM(CASE WHEN label = 'Bilirubin, Indirect' THEN max_value END) AS bilirubin_indirect_max_value
            , SUM(CASE WHEN label = 'Bilirubin, Indirect' THEN avg_value END) AS bilirubin_indirect_avg_value
            , SUM(CASE WHEN label = 'Bilirubin, Indirect' THEN ref_range_lower END) AS bilirubin_indirect_ref_range_lower
            , SUM(CASE WHEN label = 'Bilirubin, Indirect' THEN ref_range_upper END) AS bilirubin_indirect_ref_range_upper
            , MIN(CASE WHEN label = 'Bilirubin, Indirect' THEN first_abnormal_charttime END) AS bilirubin_indirect_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Bilirubin, Indirect' THEN first_test_charttime END) AS bilirubin_indirect_first_test_charttime
            , MAX(CASE WHEN label = 'Bilirubin, Indirect' THEN last_test_charttime END) AS bilirubin_indirect_last_test_charttime
            --, 'mg/dL' AS bilirubin_indirect_valueuom
            , BOOL_OR(CASE WHEN label = 'Bilirubin, Indirect' THEN was_abnormal END) AS bilirubin_indirect_was_abnormal
            , SUM(CASE WHEN label = 'Bilirubin, Indirect' THEN ratio_abnormal END) AS bilirubin_indirect_ratio_abnormal
        
            , SUM(CASE WHEN label = 'Bilirubin, Total' THEN min_value END) AS bilirubin_total_min_value
            , SUM(CASE WHEN label = 'Bilirubin, Total' THEN max_value END) AS bilirubin_total_max_value
            , SUM(CASE WHEN label = 'Bilirubin, Total' THEN avg_value END) AS bilirubin_total_avg_value
            , SUM(CASE WHEN label = 'Bilirubin, Total' THEN ref_range_lower END) AS bilirubin_total_ref_range_lower
            , SUM(CASE WHEN label = 'Bilirubin, Total' THEN ref_range_upper END) AS bilirubin_total_ref_range_upper
            , MIN(CASE WHEN label = 'Bilirubin, Total' THEN first_abnormal_charttime END) AS bilirubin_total_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Bilirubin, Total' THEN first_test_charttime END) AS bilirubin_total_first_test_charttime
            , MAX(CASE WHEN label = 'Bilirubin, Total' THEN last_test_charttime END) AS bilirubin_total_last_test_charttime
            --, 'mg/dL' AS bilirubin_total_valueuom
            , BOOL_OR(CASE WHEN label = 'Bilirubin, Total' THEN was_abnormal END) AS bilirubin_total_was_abnormal
            , SUM(CASE WHEN label = 'Bilirubin, Total' THEN ratio_abnormal END) AS bilirubin_total_ratio_abnormal
        
            -- not applicable for the current selection of data
            /*
            , SUM(CASE WHEN label = 'Bilirubin, Total, CSF' THEN min_value END) AS bilirubin_total_csf_min_value
            , SUM(CASE WHEN label = 'Bilirubin, Total, CSF' THEN max_value END) AS bilirubin_total_csf_max_value
            , SUM(CASE WHEN label = 'Bilirubin, Total, CSF' THEN avg_value END) AS bilirubin_total_csf_avg_value
            , SUM(CASE WHEN label = 'Bilirubin, Total, CSF' THEN ref_range_lower END) AS bilirubin_total_csf_ref_range_lower
            , SUM(CASE WHEN label = 'Bilirubin, Total, CSF' THEN ref_range_upper END) AS bilirubin_total_csf_ref_range_upper
            , SUM(CASE WHEN label = 'Bilirubin, Total, CSF' THEN valueuom END) AS bilirubin_total_csf_valueuom
            , BOOL_OR(CASE WHEN label = 'Bilirubin, Total, CSF' THEN was_abnormal END) AS bilirubin_total_csf_was_abnormal
            , SUM(CASE WHEN label = 'Bilirubin, Total, CSF' THEN ratio_abnormal END) AS bilirubin_total_csf_ratio_abnormal
             */
        
            , SUM(CASE WHEN label = '% Hemoglobin A1c' THEN min_value END) AS ratio_hemoglobin_a1c_min_value
            , SUM(CASE WHEN label = '% Hemoglobin A1c' THEN max_value END) AS ratio_hemoglobin_a1c_max_value
            , SUM(CASE WHEN label = '% Hemoglobin A1c' THEN avg_value END) AS ratio_hemoglobin_a1c_avg_value
            , SUM(CASE WHEN label = '% Hemoglobin A1c' THEN ref_range_lower END) AS ratio_hemoglobin_a1c_ref_range_lower
            , SUM(CASE WHEN label = '% Hemoglobin A1c' THEN ref_range_upper END) AS ratio_hemoglobin_a1c_ref_range_upper
            , MIN(CASE WHEN label = '% Hemoglobin A1c' THEN first_abnormal_charttime END) AS ratio_hemoglobin_a1c_first_abnormal_charttime
            , MAX(CASE WHEN label = '% Hemoglobin A1c' THEN first_test_charttime END) AS ratio_hemoglobin_a1c_first_test_charttime
            , MAX(CASE WHEN label = '% Hemoglobin A1c' THEN last_test_charttime END) AS ratio_hemoglobin_a1c_last_test_charttime
            --, '%' AS ratio_hemoglobin_a1c_valueuom
            , BOOL_OR(CASE WHEN label = '% Hemoglobin A1c' THEN was_abnormal END) AS ratio_hemoglobin_a1c_was_abnormal
            , SUM(CASE WHEN label = '% Hemoglobin A1c' THEN ratio_abnormal END) AS ratio_hemoglobin_a1c_ratio_abnormal
        
            , SUM(CASE WHEN label = 'Hemoglobin' THEN min_value END) AS hemoglobin_min_value
            , SUM(CASE WHEN label = 'Hemoglobin' THEN max_value END) AS hemoglobin_max_value
            , SUM(CASE WHEN label = 'Hemoglobin' THEN avg_value END) AS hemoglobin_avg_value
            , SUM(CASE WHEN label = 'Hemoglobin' THEN ref_range_lower END) AS hemoglobin_ref_range_lower
            , SUM(CASE WHEN label = 'Hemoglobin' THEN ref_range_upper END) AS hemoglobin_ref_range_upper
            , MIN(CASE WHEN label = 'Hemoglobin' THEN first_abnormal_charttime END) AS hemoglobin_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Hemoglobin' THEN first_test_charttime END) AS hemoglobin_first_test_charttime
            , MAX(CASE WHEN label = 'Hemoglobin' THEN last_test_charttime END) AS hemoglobin_last_test_charttime
            --, 'g/dL' AS hemoglobin_valueuom
            , BOOL_OR(CASE WHEN label = 'Hemoglobin' THEN was_abnormal END) AS hemoglobin_was_abnormal
            , SUM(CASE WHEN label = 'Hemoglobin' THEN ratio_abnormal END) AS hemoglobin_ratio_abnormal
        
             -- not applicable for the current selection of data
            /*
            , SUM(CASE WHEN label = 'Plasma Hemoglobin' THEN min_value END) AS plasma_hemoglobin_min_value
            , SUM(CASE WHEN label = 'Plasma Hemoglobin' THEN max_value END) AS plasma_hemoglobin_max_value
            , SUM(CASE WHEN label = 'Plasma Hemoglobin' THEN avg_value END) AS plasma_hemoglobin_avg_value
            , SUM(CASE WHEN label = 'Plasma Hemoglobin' THEN ref_range_lower END) AS plasma_hemoglobin_ref_range_lower
            , SUM(CASE WHEN label = 'Plasma Hemoglobin' THEN ref_range_upper END) AS plasma_hemoglobin_ref_range_upper
            , SUM(CASE WHEN label = 'Plasma Hemoglobin' THEN valueuom END) AS plasma_hemoglobin_valueuom
            , SUM(CASE WHEN label = 'Plasma Hemoglobin' THEN was_abnormal END) AS plasma_hemoglobin_was_abnormal
            , SUM(CASE WHEN label = 'Plasma Hemoglobin' THEN ratio_abnormal END) AS plasma_hemoglobin_ratio_abnormal
             */
        
            , SUM(CASE WHEN label = 'Hematocrit' THEN min_value END) AS hematocrit_min_value
            , SUM(CASE WHEN label = 'Hematocrit' THEN max_value END) AS hematocrit_max_value
            , SUM(CASE WHEN label = 'Hematocrit' THEN avg_value END) AS hematocrit_avg_value
            , SUM(CASE WHEN label = 'Hematocrit' THEN ref_range_lower END) AS hematocrit_ref_range_lower
            , SUM(CASE WHEN label = 'Hematocrit' THEN ref_range_upper END) AS hematocrit_ref_range_upper
            , MIN(CASE WHEN label = 'Hematocrit' THEN first_abnormal_charttime END) AS hematocrit_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Hematocrit' THEN first_test_charttime END) AS hematocrit_first_test_charttime
            , MAX(CASE WHEN label = 'Hematocrit' THEN last_test_charttime END) AS hematocrit_last_test_charttime
            --, '%' AS hematocrit_valueuom
            , BOOL_OR(CASE WHEN label = 'Hematocrit' THEN was_abnormal END) AS hematocrit_was_abnormal
            , SUM(CASE WHEN label = 'Hematocrit' THEN ratio_abnormal END) AS hematocrit_ratio_abnormal
        
            , SUM(CASE WHEN label = 'Hematocrit, CSF' THEN min_value END) AS hematocrit_csf_min_value
            , SUM(CASE WHEN label = 'Hematocrit, CSF' THEN max_value END) AS hematocrit_csf_max_value
            , SUM(CASE WHEN label = 'Hematocrit, CSF' THEN avg_value END) AS hematocrit_csf_avg_value
            , SUM(CASE WHEN label = 'Hematocrit, CSF' THEN ref_range_lower END) AS hematocrit_csf_ref_range_lower
            , SUM(CASE WHEN label = 'Hematocrit, CSF' THEN ref_range_upper END) AS hematocrit_csf_ref_range_upper
            , MIN(CASE WHEN label = 'Hematocrit, CSF' THEN first_abnormal_charttime END) AS hematocrit_csf_first_abnormal_charttime
            , MAX(CASE WHEN label = 'Hematocrit, CSF' THEN first_test_charttime END) AS hematocrit_csf_first_test_charttime
            , MAX(CASE WHEN label = 'Hematocrit, CSF' THEN last_test_charttime END) AS hematocrit_csf_last_test_charttime
            --, '%' AS hematocrit_csf_valueuom
            , BOOL_OR(CASE WHEN label = 'Hematocrit, CSF' THEN was_abnormal END) AS hematocrit_csf_was_abnormal
            , SUM(CASE WHEN label = 'Hematocrit, CSF' THEN ratio_abnormal END) AS hematocrit_csf_ratio_abnormal
        
        FROM transformed_labevents
        GROUP BY hadm_id
    );
    
    DROP TABLE IF EXISTS mart.admissions;
    CREATE TABLE mart.admissions AS (    
        SELECT a.subject_id
            , a.hadm_id
            , a.admittime
            , CASE WHEN EXTRACT(HOUR FROM a.admittime) BETWEEN 6 AND 11 THEN 'morning'
                WHEN EXTRACT(HOUR FROM a.admittime) BETWEEN 12 AND 17 THEN 'day'
                WHEN EXTRACT(HOUR FROM a.admittime) BETWEEN 18 AND 21 THEN 'evening'
                ELSE 'night' END AS admit_daytime
            , EXTRACT(MONTH FROM a.admittime) AS admission_month
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
            , i.stay_id
            , i.first_careunit
            , i.last_careunit
            , i.intime
            , i.outtime
            , i.los
            , p.gender
            , p.anchor_year
            , p.anchor_year_group
            , p.dod
            , d.icd_code
            , d.icd_version
            , lta.sodium_min_value
            , lta.sodium_max_value
            , lta.sodium_avg_value
            , lta.sodium_ref_range_lower
            , lta.sodium_ref_range_upper
            , lta.sodium_was_abnormal
            , lta.sodium_ratio_abnormal
            , lta.sodium_first_abnormal_charttime
            , lta.sodium_first_test_charttime
            , lta.sodium_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.sodium_first_abnormal_charttime)) / 3600 AS sodium_abnormal_hrs_until_discharge
            , lta.sodium_urine_min_value
            , lta.sodium_urine_max_value
            , lta.sodium_urine_avg_value
            , lta.sodium_urine_ref_range_lower
            , lta.sodium_urine_ref_range_upper
            , lta.sodium_urine_was_abnormal
            , lta.sodium_urine_ratio_abnormal
            , lta.sodium_urine_first_abnormal_charttime
            , lta.sodium_urine_first_test_charttime
            , lta.sodium_urine_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.sodium_urine_first_abnormal_charttime)) / 3600 AS sodium_urine_abnormal_hrs_until_discharge
            , lta.sodium_whole_blood_min_value
            , lta.sodium_whole_blood_max_value
            , lta.sodium_whole_blood_avg_value
            , lta.sodium_whole_blood_ref_range_lower
            , lta.sodium_whole_blood_ref_range_upper
            , lta.sodium_whole_blood_was_abnormal
            , lta.sodium_whole_blood_ratio_abnormal            
            , lta.sodium_whole_blood_first_abnormal_charttime
            , lta.sodium_whole_blood_first_test_charttime
            , lta.sodium_whole_blood_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.sodium_whole_blood_first_abnormal_charttime)) / 3600 AS sodium_whole_blood_abnormal_hrs_until_discharge
            , lta.potassium_min_value
            , lta.potassium_max_value
            , lta.potassium_avg_value
            , lta.potassium_ref_range_lower
            , lta.potassium_ref_range_upper
            , lta.potassium_was_abnormal
            , lta.potassium_ratio_abnormal            
            , lta.potassium_first_abnormal_charttime
            , lta.potassium_first_test_charttime
            , lta.potassium_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.potassium_first_abnormal_charttime)) / 3600 AS potassium_abnormal_hrs_until_discharge
            , lta.potassium_whole_blood_min_value
            , lta.potassium_whole_blood_max_value
            , lta.potassium_whole_blood_avg_value
            , lta.potassium_whole_blood_ref_range_lower
            , lta.potassium_whole_blood_ref_range_upper
            , lta.potassium_whole_blood_was_abnormal
            , lta.potassium_whole_blood_ratio_abnormal            
            , lta.potassium_whole_blood_first_abnormal_charttime
            , lta.potassium_whole_blood_first_test_charttime
            , lta.potassium_whole_blood_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.potassium_whole_blood_first_abnormal_charttime)) / 3600 AS potassium_whole_blood_abnormal_hrs_until_discharge
            , lta.calcium_total_min_value
            , lta.calcium_total_max_value
            , lta.calcium_total_avg_value
            , lta.calcium_total_ref_range_lower
            , lta.calcium_total_ref_range_upper
            , lta.calcium_total_was_abnormal
            , lta.calcium_total_ratio_abnormal            
            , lta.calcium_total_first_abnormal_charttime
            , lta.calcium_total_first_test_charttime
            , lta.calcium_total_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.calcium_total_first_abnormal_charttime)) / 3600 AS calcium_total_abnormal_hrs_until_discharge
            , lta.free_calcium_min_value
            , lta.free_calcium_max_value
            , lta.free_calcium_avg_value
            , lta.free_calcium_ref_range_lower
            , lta.free_calcium_ref_range_upper
            , lta.free_calcium_was_abnormal
            , lta.free_calcium_ratio_abnormal            
            , lta.free_calcium_first_abnormal_charttime
            , lta.free_calcium_first_test_charttime
            , lta.free_calcium_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.free_calcium_first_abnormal_charttime)) / 3600 AS free_calcium_abnormal_hrs_until_discharge
            , lta.magnesium_min_value
            , lta.magnesium_max_value
            , lta.magnesium_avg_value
            , lta.magnesium_ref_range_lower
            , lta.magnesium_ref_range_upper
            , lta.magnesium_was_abnormal
            , lta.magnesium_ratio_abnormal            
            , lta.magnesium_first_abnormal_charttime
            , lta.magnesium_first_test_charttime
            , lta.magnesium_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.magnesium_first_abnormal_charttime)) / 3600 AS magnesium_abnormal_hrs_until_discharge
            , lta.glucose_min_value
            , lta.glucose_max_value
            , lta.glucose_avg_value
            , lta.glucose_ref_range_lower
            , lta.glucose_ref_range_upper
            , lta.glucose_was_abnormal
            , lta.glucose_ratio_abnormal            
            , lta.glucose_first_abnormal_charttime
            , lta.glucose_first_test_charttime
            , lta.glucose_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.glucose_first_abnormal_charttime)) / 3600 AS glucose_abnormal_hrs_until_discharge
            , lta.glucose_ascites_min_value
            , lta.glucose_ascites_max_value
            , lta.glucose_ascites_avg_value
            , lta.glucose_ascites_ref_range_lower
            , lta.glucose_ascites_ref_range_upper
            , lta.glucose_ascites_was_abnormal
            , lta.glucose_ascites_ratio_abnormal            
            , lta.glucose_ascites_first_abnormal_charttime
            , lta.glucose_ascites_first_test_charttime
            , lta.glucose_ascites_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.glucose_ascites_first_abnormal_charttime)) / 3600 AS glucose_ascites_abnormal_hrs_until_discharge
            , lta.glucose_body_fluid_min_value
            , lta.glucose_body_fluid_max_value
            , lta.glucose_body_fluid_avg_value
            , lta.glucose_body_fluid_ref_range_lower
            , lta.glucose_body_fluid_ref_range_upper
            , lta.glucose_body_fluid_was_abnormal
            , lta.glucose_body_fluid_ratio_abnormal            
            , lta.glucose_body_fluid_first_abnormal_charttime
            , lta.glucose_body_fluid_first_test_charttime
            , lta.glucose_body_fluid_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.glucose_body_fluid_first_abnormal_charttime)) / 3600 AS glucose_body_fluid_abnormal_hrs_until_discharge
            , lta.glucose_csf_min_value
            , lta.glucose_csf_max_value
            , lta.glucose_csf_avg_value
            , lta.glucose_csf_ref_range_lower
            , lta.glucose_csf_ref_range_upper
            , lta.glucose_csf_was_abnormal
            , lta.glucose_csf_ratio_abnormal            
            , lta.glucose_csf_first_abnormal_charttime
            , lta.glucose_csf_first_test_charttime
            , lta.glucose_csf_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.glucose_csf_first_abnormal_charttime)) / 3600 AS glucose_csf_abnormal_hrs_until_discharge
            , lta.lactate_min_value
            , lta.lactate_max_value
            , lta.lactate_avg_value
            , lta.lactate_ref_range_lower
            , lta.lactate_ref_range_upper
            , lta.lactate_was_abnormal
            , lta.lactate_ratio_abnormal            
            , lta.lactate_first_abnormal_charttime
            , lta.lactate_first_test_charttime
            , lta.lactate_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.lactate_first_abnormal_charttime)) / 3600 AS lactate_abnormal_hrs_until_discharge
            , lta.lactate_dehydrogenase_csf_min_value
            , lta.lactate_dehydrogenase_csf_max_value
            , lta.lactate_dehydrogenase_csf_avg_value
            , lta.lactate_dehydrogenase_csf_ref_range_lower
            , lta.lactate_dehydrogenase_csf_ref_range_upper
            , lta.lactate_dehydrogenase_csf_was_abnormal
            , lta.lactate_dehydrogenase_csf_ratio_abnormal            
            , lta.lactate_dehydrogenase_csf_first_abnormal_charttime
            , lta.lactate_dehydrogenase_csf_first_test_charttime
            , lta.lactate_dehydrogenase_csf_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.lactate_dehydrogenase_csf_first_abnormal_charttime)) / 3600 AS lactate_dehydrogenase_csf_abnormal_hrs_until_discharge
            , lta.lactate_dehydrogenase_ld_min_value
            , lta.lactate_dehydrogenase_ld_max_value
            , lta.lactate_dehydrogenase_ld_avg_value
            , lta.lactate_dehydrogenase_ld_ref_range_lower
            , lta.lactate_dehydrogenase_ld_ref_range_upper
            , lta.lactate_dehydrogenase_ld_was_abnormal
            , lta.lactate_dehydrogenase_ld_ratio_abnormal            
            , lta.lactate_dehydrogenase_ld_first_abnormal_charttime
            , lta.lactate_dehydrogenase_ld_first_test_charttime
            , lta.lactate_dehydrogenase_ld_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.lactate_dehydrogenase_ld_first_abnormal_charttime)) / 3600 AS lactate_dehydrogenase_ld_abnormal_hrs_until_discharge
            , lta.ph_min_value
            , lta.ph_max_value
            , lta.ph_avg_value
            , lta.ph_ref_range_lower
            , lta.ph_ref_range_upper
            , lta.ph_was_abnormal
            , lta.ph_ratio_abnormal            
            , lta.ph_first_abnormal_charttime
            , lta.ph_first_test_charttime
            , lta.ph_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.ph_first_abnormal_charttime)) / 3600 AS ph_abnormal_hrs_until_discharge
            , lta.po2_min_value
            , lta.po2_max_value
            , lta.po2_avg_value
            , lta.po2_ref_range_lower
            , lta.po2_ref_range_upper
            , lta.po2_was_abnormal
            , lta.po2_ratio_abnormal            
            , lta.po2_first_abnormal_charttime
            , lta.po2_first_test_charttime
            , lta.po2_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.po2_first_abnormal_charttime)) / 3600 AS po2_abnormal_hrs_until_discharge
            , lta.bicarbonate_min_value
            , lta.bicarbonate_max_value
            , lta.Bicarbonate_avg_value
            , lta.bicarbonate_ref_range_lower
            , lta.bicarbonate_ref_range_upper
            , lta.bicarbonate_was_abnormal
            , lta.bicarbonate_ratio_abnormal            
            , lta.bicarbonate_first_abnormal_charttime
            , lta.bicarbonate_first_test_charttime
            , lta.bicarbonate_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.bicarbonate_first_abnormal_charttime)) / 3600 AS bicarbonate_abnormal_hrs_until_discharge
            , lta.bicarbonate_urine_min_value
            , lta.bicarbonate_urine_max_value
            , lta.bicarbonate_urine_avg_value
            , lta.bicarbonate_urine_ref_range_lower
            , lta.bicarbonate_urine_ref_range_upper
            , lta.bicarbonate_urine_was_abnormal
            , lta.bicarbonate_urine_ratio_abnormal            
            , lta.bicarbonate_urine_first_abnormal_charttime
            , lta.bicarbonate_urine_first_test_charttime
            , lta.bicarbonate_urine_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.bicarbonate_urine_first_abnormal_charttime)) / 3600 AS bicarbonate_urine_abnormal_hrs_until_discharge
            , lta.calculated_bicarbonate_whole_blood_min_value
            , lta.calculated_bicarbonate_whole_blood_max_value
            , lta.calculated_bicarbonate_whole_blood_avg_value
            , lta.calculated_bicarbonate_whole_blood_ref_range_lower
            , lta.calculated_bicarbonate_whole_blood_ref_range_upper
            , lta.calculated_bicarbonate_whole_blood_was_abnormal
            , lta.calculated_bicarbonate_whole_blood_ratio_abnormal            
            , lta.calculated_bicarbonate_whole_blood_first_abnormal_charttime
            , lta.calculated_bicarbonate_whole_blood_first_test_charttime
            , lta.calculated_bicarbonate_whole_blood_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.calculated_bicarbonate_whole_blood_first_abnormal_charttime)) / 3600 AS calculated_bicarbonate_whole_blood_abnormal_hrs_until_discharge
            , lta.inr_pt_min_value
            , lta.inr_pt_max_value
            , lta.inr_pt_avg_value
            , lta.inr_pt_ref_range_lower
            , lta.inr_pt_ref_range_upper
            , lta.inr_pt_was_abnormal
            , lta.inr_pt_ratio_abnormal            
            , lta.inr_pt_first_abnormal_charttime
            , lta.inr_pt_first_test_charttime
            , lta.inr_pt_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.inr_pt_first_abnormal_charttime)) / 3600 AS inr_pt_abnormal_hrs_until_discharge
            , lta.pt_min_value
            , lta.pt_max_value
            , lta.pt_avg_value
            , lta.pt_ref_range_lower
            , lta.pt_ref_range_upper
            , lta.pt_was_abnormal
            , lta.pt_ratio_abnormal            
            , lta.pt_first_abnormal_charttime
            , lta.pt_first_test_charttime
            , lta.pt_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.pt_first_abnormal_charttime)) / 3600 AS pt_abnormal_hrs_until_discharge
            , lta.ptt_min_value
            , lta.ptt_max_value
            , lta.ptt_avg_value
            , lta.ptt_ref_range_lower
            , lta.ptt_ref_range_upper
            , lta.ptt_was_abnormal
            , lta.ptt_ratio_abnormal            
            , lta.ptt_first_abnormal_charttime
            , lta.ptt_first_test_charttime
            , lta.ptt_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.ptt_first_abnormal_charttime)) / 3600 AS ptt_abnormal_hrs_until_discharge
            , lta.crp_min_value
            , lta.crp_max_value
            , lta.crp_avg_value
            , lta.crp_ref_range_lower
            , lta.crp_ref_range_upper
            , lta.crp_was_abnormal
            , lta.crp_ratio_abnormal            
            , lta.crp_first_abnormal_charttime
            , lta.crp_first_test_charttime
            , lta.crp_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.crp_first_abnormal_charttime)) / 3600 AS crp_abnormal_hrs_until_discharge
            , lta.white_blood_cells_min_value
            , lta.white_blood_cells_max_value
            , lta.white_blood_cells_avg_value
            , lta.white_blood_cells_ref_range_lower
            , lta.white_blood_cells_ref_range_upper
            , lta.white_blood_cells_was_abnormal
            , lta.white_blood_cells_ratio_abnormal            
            , lta.white_blood_cells_first_abnormal_charttime
            , lta.white_blood_cells_first_test_charttime
            , lta.white_blood_cells_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.white_blood_cells_first_abnormal_charttime)) / 3600 AS white_blood_cells_abnormal_hrs_until_discharge
            , lta.ammonia_min_value
            , lta.ammonia_max_value
            , lta.ammonia_avg_value
            , lta.ammonia_ref_range_lower
            , lta.ammonia_ref_range_upper
            , lta.ammonia_was_abnormal
            , lta.ammonia_ratio_abnormal            
            , lta.ammonia_first_abnormal_charttime
            , lta.ammonia_first_test_charttime
            , lta.ammonia_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.ammonia_first_abnormal_charttime)) / 3600 AS ammonia_abnormal_hrs_until_discharge
            , lta.albumin_min_value
            , lta.albumin_max_value
            , lta.albumin_avg_value
            , lta.albumin_ref_range_lower
            , lta.albumin_ref_range_upper
            , lta.albumin_was_abnormal
            , lta.albumin_ratio_abnormal            
            , lta.albumin_first_abnormal_charttime
            , lta.albumin_first_test_charttime
            , lta.albumin_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.albumin_first_abnormal_charttime)) / 3600 AS albumin_abnormal_hrs_until_discharge
            , lta.albumin_creatine_urine_min_value
            , lta.albumin_creatine_urine_max_value
            , lta.albumin_creatine_urine_avg_value
            , lta.albumin_creatine_urine_ref_range_lower
            , lta.albumin_creatine_urine_ref_range_upper
            , lta.albumin_creatine_urine_was_abnormal
            , lta.albumin_creatine_urine_ratio_abnormal            
            , lta.albumin_creatine_urine_first_abnormal_charttime
            , lta.albumin_creatine_urine_first_test_charttime
            , lta.albumin_creatine_urine_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.albumin_creatine_urine_first_abnormal_charttime)) / 3600 AS albumin_creatine_urine_abnormal_hrs_until_discharge
            , lta.creatinine_min_value
            , lta.creatinine_max_value
            , lta.creatinine_avg_value
            , lta.creatinine_ref_range_lower
            , lta.creatinine_ref_range_upper
            , lta.creatinine_was_abnormal
            , lta.creatinine_ratio_abnormal            
            , lta.creatinine_first_abnormal_charttime
            , lta.creatinine_first_test_charttime
            , lta.creatinine_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.creatinine_first_abnormal_charttime)) / 3600 AS creatinine_abnormal_hrs_until_discharge
            , lta.creatinine_clearance_min_value
            , lta.creatinine_clearance_max_value
            , lta.creatinine_clearance_avg_value
            , lta.creatinine_clearance_ref_range_lower
            , lta.creatinine_clearance_ref_range_upper
            , lta.creatinine_clearance_was_abnormal
            , lta.creatinine_clearance_ratio_abnormal            
            , lta.creatinine_clearance_first_abnormal_charttime
            , lta.creatinine_clearance_first_test_charttime
            , lta.creatinine_clearance_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.creatinine_clearance_first_abnormal_charttime)) / 3600 AS creatinine_clearance_abnormal_hrs_until_discharge
            , lta.creatinine_serum_min_value
            , lta.creatinine_serum_max_value
            , lta.creatinine_serum_avg_value
            , lta.creatinine_serum_ref_range_lower
            , lta.creatinine_serum_ref_range_upper
            , lta.creatinine_serum_was_abnormal
            , lta.creatinine_serum_ratio_abnormal            
            , lta.creatinine_serum_first_abnormal_charttime
            , lta.creatinine_serum_first_test_charttime
            , lta.creatinine_serum_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.creatinine_serum_first_abnormal_charttime)) / 3600 AS creatinine_serum_abnormal_hrs_until_discharge
            , lta.urea_nitrogen_min_value
            , lta.urea_nitrogen_max_value
            , lta.urea_nitrogen_avg_value
            , lta.urea_nitrogen_ref_range_lower
            , lta.urea_nitrogen_ref_range_upper
            , lta.urea_nitrogen_was_abnormal
            , lta.urea_nitrogen_ratio_abnormal            
            , lta.urea_nitrogen_first_abnormal_charttime
            , lta.urea_nitrogen_first_test_charttime
            , lta.urea_nitrogen_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.urea_nitrogen_first_abnormal_charttime)) / 3600 AS urea_nitrogen_abnormal_hrs_until_discharge
            , lta.alt_min_value
            , lta.alt_max_value
            , lta.alt_avg_value
            , lta.alt_ref_range_lower
            , lta.alt_ref_range_upper
            , lta.alt_was_abnormal
            , lta.alt_ratio_abnormal            
            , lta.alt_first_abnormal_charttime
            , lta.alt_first_test_charttime
            , lta.alt_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.alt_first_abnormal_charttime)) / 3600 AS alt_abnormal_hrs_until_discharge
            , lta.ast_min_value
            , lta.ast_max_value
            , lta.ast_avg_value
            , lta.ast_ref_range_lower
            , lta.ast_ref_range_upper
            , lta.ast_was_abnormal
            , lta.ast_ratio_abnormal            
            , lta.ast_first_abnormal_charttime
            , lta.ast_first_test_charttime
            , lta.ast_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.ast_first_abnormal_charttime)) / 3600 AS ast_abnormal_hrs_until_discharge
            , lta.bilirubin_min_value
            , lta.bilirubin_max_value
            , lta.bilirubin_avg_value
            , lta.bilirubin_ref_range_lower
            , lta.bilirubin_ref_range_upper
            , lta.bilirubin_was_abnormal
            , lta.bilirubin_ratio_abnormal            
            , lta.bilirubin_first_abnormal_charttime
            , lta.bilirubin_first_test_charttime
            , lta.bilirubin_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.bilirubin_first_abnormal_charttime)) / 3600 AS bilirubin_abnormal_hrs_until_discharge
            , lta.bilirubin_direct_min_value
            , lta.bilirubin_direct_max_value
            , lta.bilirubin_direct_avg_value
            , lta.bilirubin_direct_ref_range_lower
            , lta.bilirubin_direct_ref_range_upper
            , lta.bilirubin_direct_was_abnormal
            , lta.bilirubin_direct_ratio_abnormal            
            , lta.bilirubin_direct_first_abnormal_charttime
            , lta.bilirubin_direct_first_test_charttime
            , lta.bilirubin_direct_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.bilirubin_direct_first_abnormal_charttime)) / 3600 AS bilirubin_direct_abnormal_hrs_until_discharge
            , lta.bilirubin_indirect_min_value
            , lta.bilirubin_indirect_max_value
            , lta.bilirubin_indirect_avg_value
            , lta.bilirubin_indirect_ref_range_lower
            , lta.bilirubin_indirect_ref_range_upper
            , lta.bilirubin_indirect_was_abnormal
            , lta.bilirubin_indirect_ratio_abnormal            
            , lta.bilirubin_indirect_first_abnormal_charttime
            , lta.bilirubin_indirect_first_test_charttime
            , lta.bilirubin_indirect_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.bilirubin_indirect_first_abnormal_charttime)) / 3600 AS bilirubin_indirect_abnormal_hrs_until_discharge
            , lta.bilirubin_total_min_value
            , lta.bilirubin_total_max_value
            , lta.bilirubin_total_avg_value
            , lta.bilirubin_total_ref_range_lower
            , lta.bilirubin_total_ref_range_upper
            , lta.bilirubin_total_was_abnormal
            , lta.bilirubin_total_ratio_abnormal            
            , lta.bilirubin_total_first_abnormal_charttime
            , lta.bilirubin_total_first_test_charttime
            , lta.bilirubin_total_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.bilirubin_total_first_abnormal_charttime)) / 3600 AS bilirubin_total_abnormal_hrs_until_discharge
            , lta.ratio_hemoglobin_a1c_min_value
            , lta.ratio_hemoglobin_a1c_max_value
            , lta.ratio_hemoglobin_a1c_avg_value
            , lta.ratio_hemoglobin_a1c_ref_range_lower
            , lta.ratio_hemoglobin_a1c_ref_range_upper
            , lta.ratio_hemoglobin_a1c_was_abnormal
            , lta.ratio_hemoglobin_a1c_ratio_abnormal            
            , lta.ratio_hemoglobin_a1c_first_abnormal_charttime
            , lta.ratio_hemoglobin_a1c_first_test_charttime
            , lta.ratio_hemoglobin_a1c_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.ratio_hemoglobin_a1c_first_abnormal_charttime)) / 3600 AS ratio_hemoglobin_a1c_abnormal_hrs_until_discharge
            , lta.hemoglobin_min_value
            , lta.hemoglobin_max_value
            , lta.hemoglobin_avg_value
            , lta.hemoglobin_ref_range_lower
            , lta.hemoglobin_ref_range_upper
            , lta.hemoglobin_was_abnormal
            , lta.hemoglobin_ratio_abnormal            
            , lta.hemoglobin_first_abnormal_charttime
            , lta.hemoglobin_first_test_charttime
            , lta.hemoglobin_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.hemoglobin_first_abnormal_charttime)) / 3600 AS hemoglobin_abnormal_hrs_until_discharge
            , lta.hematocrit_min_value
            , lta.hematocrit_max_value
            , lta.hematocrit_avg_value
            , lta.hematocrit_ref_range_lower
            , lta.hematocrit_ref_range_upper
            , lta.hematocrit_was_abnormal
            , lta.hematocrit_ratio_abnormal            
            , lta.hematocrit_first_abnormal_charttime
            , lta.hematocrit_first_test_charttime
            , lta.hematocrit_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.hematocrit_first_abnormal_charttime)) / 3600 AS hematocrit_abnormal_hrs_until_discharge
            , lta.hematocrit_csf_min_value
            , lta.hematocrit_csf_max_value
            , lta.hematocrit_csf_avg_value
            , lta.hematocrit_csf_ref_range_lower
            , lta.hematocrit_csf_ref_range_upper
            , lta.hematocrit_csf_was_abnormal
            , lta.hematocrit_csf_ratio_abnormal
            , lta.hematocrit_csf_first_abnormal_charttime
            , lta.hematocrit_csf_first_test_charttime
            , lta.hematocrit_csf_last_test_charttime
            , EXTRACT(EPOCH FROM (a.dischtime - lta.hematocrit_csf_first_abnormal_charttime)) / 3600 AS hematocrit_csf_abnormal_hrs_until_discharge
        FROM raw.admissions a
        JOIN labevents_to_admissions lta ON a.hadm_id = lta.hadm_id
        JOIN raw.patients p ON p.subject_id = a.subject_id
        JOIN raw.diagnoses_icd d ON p.subject_id = d.subject_id AND a.hadm_id = d.hadm_id
        JOIN raw.icustays i ON i.hadm_id = a.hadm_id
        WHERE d.icd_version = 10
        AND (
            d.icd_code LIKE 'I61%' OR
            d.icd_code LIKE 'I63%' OR
            d.icd_code LIKE 'G41%'
        )
    );
    """
)
