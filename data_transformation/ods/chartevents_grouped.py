"""Module to create chartevents_grouped table in ODS"""

import configparser
from data_transformation.data_transfer.utils import ConnectionDetails, DataTransfer

# pylint: disable=duplicate-code
config = configparser.ConfigParser()
config.read('config.ini')

file_folder = config['raw_data']['file_folder']
# pylint: enable=duplicate-code

dt = DataTransfer(ConnectionDetails(*(x[1] for x in config.items('database'))))

dt.run_query(
    """
    DROP TABLE IF EXISTS filtered_chartevents;
    CREATE TEMPORARY TABLE filtered_chartevents AS (
        SELECT date_trunc('day', c.charttime) AS charttime_day
            , c.subject_id
            , c.hadm_id
            , c.stay_id
            , c.caregiver_id
            , c.charttime
            , c.storetime
            , c.itemid
            , c.value
            , c.valueuom
            , c.warning
            , di.label
            , di.abbreviation
            , di.category
            , di.unitname
            , di.param_type
            , di.lownormalvalue
            , di.highnormalvalue
        FROM raw.chartevents c
        JOIN raw.d_items di ON c.itemid = di.itemid
        WHERE di.abbreviation IN (
                                  'HR',
                                  'RR',
                                  'ABPm',
                                  'ABPd',
                                  'ABPs',
                                  'NBPm',
                                  'NBPd',
                                  'NBPs',
                                  'Forehead SpO2 Sensor in Place',
                                  'SpO2 Desat Limit',
                                  'Temperature C',
                                  'Temperature F',
                                  'Cerebral T (C)',
                                  'Eye Opening',
                                  'Motor Response',
                                  'Verbal Response',
                                  'Pupil Response L',
                                  'Pupil Response R',
                                  'PH (Arterial)',
                                  'PH (Venous)',
                                  'HCO3 (serum)',
                                  'Sodium (serum)',
                                  'Potassium (serum)',
                                  'Chloride (serum)',
                                  'BUN',
                                  'Ventilator Mode',
                                  'Ventilator Mode (Hamilton)',
                                  'Ventilator Type',
                                  'BIS - EMG',
                                  'BIS Index Range',
                                  'Delirium assessment',
                                  'CAM-ICU Altered LOC',
                                  'CAM-ICU Disorganized thinking',
                                  'CAM-ICU Inattention',
                                  'Motor Deficit',
                                  'Goal Richmond-RAS Scale',
                                  'PNC-1 Appearance',
                                  'PNC-1 Bolus (mL)',
                                  'PNC-1 Infusion Rate (mL/hr)',
                                  'PNC-1 Location',
                                  'PNC-1 Medication',
                                  'PNC-1 Motor Deficit',
                                  'PNC-2 Appearance',
                                  'PNC-2 Infusion Rate (mL/hr)',
                                  'PNC-2 Location',
                                  'PNC-2 Medication',
                                  'PNC-2 Motor Deficit',
                                  'CPOT-Pain Assessment Method',
                                  'CPOT-Pain Management',
                                  'Pain Level',
                                  'Pain Level Acceptable',
                                  'Pain Level Acceptable (PreIntervention)',
                                  'Pain Level Response',
                                  'Pain Management',
                                  'NMB Medication',
                                  'Epidural Infusion Rate (mL/hr)',
                                  'Epidural Medication',
                                  'PCA 1 hour limit',
                                  'PCA attempt',
                                  'PCA basal rate (mL/hour)',
                                  'PCA bolus',
                                  'PCA cleared',
                                  'PCA concentrations',
                                  'PCA dose',
                                  'PCA inject',
                                  'PCA lockout (min)',
                                  'PCA medication',
                                  'PCA total dose',
                                  'TOF Response',
                                  'TOF Twitch',
                                  'Current Used/mA',
                                  'Daily Wake Up',
                                  'Daily Wake Up Deferred',
                                  'Untoward Effect',
                                  'PT Splint Location #1',
                                  'PT Splint Location #2',
                                  'PT Splint Status #1',
                                  'PT Splint Status #2',
                                  'PTT',
                                  'INR',
                                  'AST',
                                  'ALT',
                                  'Direct Bilirubin',
                                  'Total Bilirubin'
        )
    );
    
    CREATE TABLE ods.chartevents_grouped (
        charttime_week VARCHAR NOT NULL,
        subject_id INT,
        hadm_id INT,
        charttime TIMESTAMP NOT NULL,
        HR_value FLOAT,
        HR_warning INT,
        RR_value FLOAT,
        RR_warning INT,
        ABPm_value FLOAT,
        ABPm_warning INT,
        ABPd_value FLOAT,
        ABPd_warning INT,
        ABPs_value FLOAT,
        ABPs_warning INT,
        NBPm_value FLOAT,
        NBPm_warning INT,
        NBPd_value FLOAT,
        NBPd_warning INT,
        NBPs_value FLOAT,
        NBPs_warning INT,
        forehead_SpO2_value FLOAT,
        forehead_SpO2_warning INT,
        SpO2_Desat_Limit_value FLOAT,
        SpO2_Desat_Limit_warning INT,
        temperature_C_value FLOAT,
        temperature_C_warning INT,
        temperature_F_value FLOAT,
        temperature_F_warning INT,
        Cerebral_T_value FLOAT,
        Cerebral_T_warning INT,
        eye_opening_value VARCHAR,
        eye_opening_warning INT,
        motor_response_value VARCHAR,
        motor_response_warning INT,
        verbal_response_value VARCHAR,
        verbal_response_warning INT,
        pupil_response_L_value VARCHAR,
        pupil_response_L_warning INT,
        pupil_response_R_value VARCHAR,
        pupil_response_R_warning INT,
        PH_arterial_value FLOAT,
        PH_arterial_warning INT,
        PH_venous_value FLOAT,
        PH_venous_warning INT,
        HCO3_serum_value FLOAT,
        HCO3_serum_warning INT,
        sodium_serum_value FLOAT,
        sodium_serum_warning INT,
        potassium_serum_value FLOAT,
        potassium_serum_warning INT,
        chloride_serum_value FLOAT,
        chloride_serum_warning INT,
        bun_value FLOAT,
        bun_warning INT,
        ventilator_mode_value VARCHAR,
        ventilator_mode_warning INT,
        ventilator_mode_hamilton_value VARCHAR,
        ventilator_mode_hamilton_warning INT,
        ventilator_type_value VARCHAR,
        ventilator_type_warning INT,
        bis_emg_value VARCHAR,
        bis_emg_warning INT,
        bis_index_range_value FLOAT,
        bis_index_range_warning INT,
        delirium_assessment_value VARCHAR,
        delirium_assessment_warning INT,
        cam_icu_altered_loc_value VARCHAR,
        cam_icu_altered_loc_warning INT,
        cam_icu_disorganized_thinking_value VARCHAR,
        cam_icu_disorganized_thinking_warning INT,
        cam_icu_inattention_value VARCHAR,
        cam_icu_inattention_warning INT,
        motor_deficit_value VARCHAR,
        motor_deficit_warning INT,
        goal_richmond_ras_scale_value VARCHAR,
        goal_richmond_ras_scale_warning INT,
        pnc_1_appearance_value VARCHAR,
        pnc_1_appearance_warning INT,
        pnc_1_bolus_ml_value FLOAT,
        pnc_1_bolus_ml_warning INT,
        pnc_1_infusion_rate_ml_hr_value FLOAT,
        pnc_1_infusion_rate_ml_hr_warning INT,
        pnc_1_location_value VARCHAR,
        pnc_1_location_warning INT,
        pnc_1_medication_value VARCHAR,
        pnc_1_medication_warning INT,
        pnc_1_motor_deficit_value VARCHAR,
        pnc_1_motor_deficit_warning INT,
        pnc_2_appearance_value VARCHAR,
        pnc_2_appearance_warning INT,
        pnc_2_infusion_rate_ml_hr_value FLOAT,
        pnc_2_infusion_rate_ml_hr_warning INT,
        pnc_2_location_value VARCHAR,
        pnc_2_location_warning INT,
        pnc_2_medication_value VARCHAR,
        pnc_2_medication_warning INT,
        pnc_2_motor_deficit_value VARCHAR,
        pnc_2_motor_deficit_warning INT,
        cpot_pain_assessment_method_value VARCHAR,
        cpot_pain_assessment_method_warning INT,
        cpot_pain_management_value VARCHAR,
        cpot_pain_management_warning INT,
        pain_level_value VARCHAR,
        pain_level_warning INT,
        pain_level_acceptable_value VARCHAR,
        pain_level_acceptable_warning INT,
        pain_level_acceptable_preintervention_value VARCHAR,
        pain_level_acceptable_preintervention_warning INT,
        pain_level_response_value VARCHAR,
        pain_level_response_warning INT,
        pain_management_value VARCHAR,
        pain_management_warning INT,
        nmb_medication_value VARCHAR,
        nmb_medication_warning INT,
        epidural_infusion_rate_ml_hr_value FLOAT,
        epidural_infusion_rate_ml_hr_warning INT,
        epidural_medication_value VARCHAR,
        epidural_medication_warning INT,
        pca_1_hour_limit_value FLOAT,
        pca_1_hour_limit_warning INT,
        pca_attempt_value FLOAT,
        pca_attempt_warning INT,
        pca_basal_rate_ml_hour_value FLOAT,
        pca_basal_rate_ml_hour_warning INT,
        pca_bolus_value FLOAT,
        pca_bolus_warning INT,
        pca_cleared_value VARCHAR,
        pca_cleared_warning INT,
        pca_concentrations_value VARCHAR,
        pca_concentrations_warning INT,
        pca_dose_value FLOAT,
        pca_dose_warning INT,
        pca_inject_value FLOAT,
        pca_inject_warning INT,
        pca_lockout_min_value FLOAT,
        pca_lockout_min_warning INT,
        pca_medication_value VARCHAR,
        pca_medication_warning INT,
        pca_total_dose_value FLOAT,
        pca_total_dose_warning INT,
        tof_response_value FLOAT,
        tof_response_warning INT,
        tof_twitch_value VARCHAR,
        tof_twitch_warning INT,
        current_used_ma_value FLOAT,
        current_used_ma_warning INT,
        daily_wake_up_value VARCHAR,
        daily_wake_up_warning INT,
        daily_wake_up_deferred_value VARCHAR,
        daily_wake_up_deferred_warning INT,
        untoward_effect_value VARCHAR,
        untoward_effect_warning INT,
        pt_splint_location_1_value VARCHAR,
        pt_splint_location_1_warning INT,
        pt_splint_location_2_value VARCHAR,
        pt_splint_location_2_warning INT,
        pt_splint_status_1_value VARCHAR,
        pt_splint_status_1_warning INT,
        pt_splint_status_2_value VARCHAR,
        pt_splint_status_2_warning INT,
        ptt_value FLOAT,
        ptt_warning INT,
        inr_value FLOAT,
        inr_warning INT,
        ast_value FLOAT,
        ast_warning INT,
        alt_value FLOAT,
        alt_warning INT,
        direct_bilirubin_value FLOAT,
        direct_bilirubin_warning INT,
        total_bilirubin_value FLOAT,
        total_bilirubin_warning INT
    )
    PARTITION BY LIST(charttime_week);

    DO $$
        DECLARE
            start_date TIMESTAMP := '2110-01-16 00:00:00';
            end_date TIMESTAMP := '2211-01-19 00:00:00';
            cur_date TIMESTAMP := start_date;
            week_label VARCHAR;  -- Variable to hold the partition label (year_week)
        BEGIN
        WHILE cur_date < end_date LOOP
            -- Create the week label as 'YYYY_WW'
            week_label := to_char(cur_date, 'IYYY') || '_' || to_char(cur_date, 'IW');
    
            EXECUTE format('CREATE TABLE ods.chartevents_grouped_week_%s PARTITION OF ods.chartevents_grouped
                            FOR VALUES IN (''%s'')',
                           week_label,  -- Partition table name based on week label
                           week_label); -- Partition value in the list
    
            -- Move to the next week
            cur_date := cur_date + interval '1 week';
        END LOOP;
    END $$;
    
    INSERT INTO ods.chartevents_grouped
    SELECT TO_CHAR(charttime_day, 'IYYY_IW') AS charttime_week
        , subject_id
        , hadm_id
        , charttime
        , CAST(MAX(CASE WHEN abbreviation = 'HR' THEN value END) AS FLOAT) AS HR_value 
        , SUM(CASE WHEN abbreviation = 'HR' THEN warning END) AS HR_warning
        , CAST(MAX(CASE WHEN abbreviation = 'RR' THEN value END) AS FLOAT) AS RR_value 
        , SUM(CASE WHEN abbreviation = 'RR' THEN warning END) AS RR_warning
        , CAST(MAX(CASE WHEN abbreviation = 'ABPm' THEN value END) AS FLOAT) AS ABPm_value 
        , SUM(CASE WHEN abbreviation = 'ABPm' THEN warning END) AS ABPm_warning
        , CAST(MAX(CASE WHEN abbreviation = 'ABPd' THEN value END) AS FLOAT) AS ABPd_value 
        , SUM(CASE WHEN abbreviation = 'ABPd' THEN warning END) AS ABPd_warning
        , CAST(MAX(CASE WHEN abbreviation = 'ABPs' THEN value END) AS FLOAT) AS ABPs_value 
        , SUM(CASE WHEN abbreviation = 'ABPs' THEN warning END) AS ABPs_warning
        , CAST(MAX(CASE WHEN abbreviation = 'NBPm' THEN value END) AS FLOAT) AS NBPm_value 
        , SUM(CASE WHEN abbreviation = 'NBPm' THEN warning END) AS NBPm_warning
        , CAST(MAX(CASE WHEN abbreviation = 'NBPd' THEN value END) AS FLOAT) AS NBPd_value 
        , SUM(CASE WHEN abbreviation = 'NBPd' THEN warning END) AS NBPd_warning
        , CAST(MAX(CASE WHEN abbreviation = 'NBPs' THEN value END) AS FLOAT) AS NBPs_value 
        , SUM(CASE WHEN abbreviation = 'NBPs' THEN warning END) AS NBPs_warning
        , CAST(MAX(CASE WHEN abbreviation = 'Forehead SpO2 Sensor in Place' THEN value END) AS FLOAT) AS forehead_SpO2_value 
        , SUM(CASE WHEN abbreviation = 'Forehead SpO2 Sensor in Place' THEN warning END) AS forehead_SpO2_warning
        , CAST(MAX(CASE WHEN abbreviation = 'SpO2 Desat Limit' THEN value END) AS FLOAT) AS SpO2_Desat_Limit_value 
        , SUM(CASE WHEN abbreviation = 'SpO2 Desat Limit' THEN warning END) AS SpO2_Desat_Limit_warning
        , CAST(MAX(CASE WHEN abbreviation = 'Temperature C' THEN value END) AS FLOAT) AS temperature_C_value 
        , SUM(CASE WHEN abbreviation = 'Temperature C' THEN warning END) AS temperature_C_warning
        , CAST(MAX(CASE WHEN abbreviation = 'Temperature F' THEN value END) AS FLOAT) AS temperature_F_value 
        , SUM(CASE WHEN abbreviation = 'Temperature F' THEN warning END) AS temperature_F_warning
        , CAST(MAX(CASE WHEN abbreviation = 'Cerebral T (C)' THEN value END) AS FLOAT) AS Cerebral_T_value 
        , SUM(CASE WHEN abbreviation = 'Cerebral T (C)' THEN warning END) AS Cerebral_T_warning
        , MAX(CASE WHEN abbreviation = 'Eye Opening' THEN value END) AS eye_opening_value 
        , SUM(CASE WHEN abbreviation = 'Eye Opening' THEN warning END) AS eye_opening_warning
        , MAX(CASE WHEN abbreviation = 'Motor Response' THEN value END) AS motor_response_value 
        , SUM(CASE WHEN abbreviation = 'Motor Response' THEN warning END) AS motor_response_warning
        , MAX(CASE WHEN abbreviation = 'Verbal Response' THEN value END) AS verbal_response_value 
        , SUM(CASE WHEN abbreviation = 'Verbal Response' THEN warning END) AS verbal_response_warning
        , MAX(CASE WHEN abbreviation = 'Pupil Response L' THEN value END) AS pupil_response_L_value 
        , SUM(CASE WHEN abbreviation = 'Pupil Response L' THEN warning END) AS pupil_response_L_warning
        , MAX(CASE WHEN abbreviation = 'Pupil Response R' THEN value END) AS pupil_response_R_value 
        , SUM(CASE WHEN abbreviation = 'Pupil Response R' THEN warning END) AS pupil_response_R_warning
        , CAST(MAX(CASE WHEN abbreviation = 'PH (Arterial)' THEN value END) AS FLOAT) AS PH_arterial_value 
        , SUM(CASE WHEN abbreviation = 'PH (Arterial)' THEN warning END) AS PH_arterial_warning
        , CAST(MAX(CASE WHEN abbreviation = 'PH (Venous)' THEN value END) AS FLOAT) AS PH_venous_value 
        , SUM(CASE WHEN abbreviation = 'PH (Venous)' THEN warning END) AS PH_venous_warning
        , CAST(MAX(CASE WHEN abbreviation = 'HCO3 (serum)' THEN value END) AS FLOAT) AS HCO3_serum_value 
        , SUM(CASE WHEN abbreviation = 'HCO3 (serum)' THEN warning END) AS HCO3_serum_warning
        , CAST(MAX(CASE WHEN abbreviation = 'Sodium (serum)' THEN value END) AS FLOAT) AS sodium_serum_value 
        , SUM(CASE WHEN abbreviation = 'Sodium (serum)' THEN warning END) AS sodium_serum_warning
        , CAST(MAX(CASE WHEN abbreviation = 'Potassium (serum)' THEN value END) AS FLOAT) AS potassium_serum_value
        , SUM(CASE WHEN abbreviation = 'Potassium (serum)' THEN warning END) AS potassium_serum_warning
        , CAST(MAX(CASE WHEN abbreviation = 'Chloride (serum)' THEN value END) AS FLOAT) AS chloride_serum_value
        , SUM(CASE WHEN abbreviation = 'Chloride (serum)' THEN warning END) AS chloride_serum_warning
        , CAST(MAX(CASE WHEN abbreviation = 'BUN' THEN value END) AS FLOAT) AS bun_value
        , SUM(CASE WHEN abbreviation = 'BUN' THEN warning END) AS bun_warning
        , MAX(CASE WHEN abbreviation = 'Ventilator Mode' THEN value END) AS ventilator_mode_value
        , SUM(CASE WHEN abbreviation = 'Ventilator Mode' THEN warning END) AS ventilator_mode_warning
        , MAX(CASE WHEN abbreviation = 'Ventilator Mode (Hamilton)' THEN value END) AS ventilator_mode_hamilton_value
        , SUM(CASE WHEN abbreviation = 'Ventilator Mode (Hamilton)' THEN warning END) AS ventilator_mode_hamilton_warning
        , MAX(CASE WHEN abbreviation = 'Ventilator Type' THEN value END) AS ventilator_type_value
        , SUM(CASE WHEN abbreviation = 'Ventilator Type' THEN warning END) AS ventilator_type_warning
        , MAX(CASE WHEN abbreviation = 'BIS - EMG' THEN value END) AS bis_emg_value
        , SUM(CASE WHEN abbreviation = 'BIS - EMG' THEN warning END) AS bis_emg_warning
        , CAST(MAX(CASE WHEN abbreviation = 'BIS Index Range' THEN value END) AS FLOAT) AS bis_index_range_value
        , SUM(CASE WHEN abbreviation = 'BIS Index Range' THEN warning END) AS bis_index_range_warning
        , MAX(CASE WHEN abbreviation = 'Delirium assessment' THEN value END) AS delirium_assessment_value
        , SUM(CASE WHEN abbreviation = 'Delirium assessment' THEN warning END) AS delirium_assessment_warning
        , MAX(CASE WHEN abbreviation = 'CAM-ICU Altered LOC' THEN value END) AS cam_icu_altered_loc_value
        , SUM(CASE WHEN abbreviation = 'CAM-ICU Altered LOC' THEN warning END) AS cam_icu_altered_loc_warning
        , MAX(CASE WHEN abbreviation = 'CAM-ICU Disorganized thinking' THEN value END) AS cam_icu_disorganized_thinking_value
        , SUM(CASE WHEN abbreviation = 'CAM-ICU Disorganized thinking' THEN warning END) AS cam_icu_disorganized_thinking_warning
        , MAX(CASE WHEN abbreviation = 'CAM-ICU Inattention' THEN value END) AS cam_icu_inattention_value
        , SUM(CASE WHEN abbreviation = 'CAM-ICU Inattention' THEN warning END) AS cam_icu_inattention_warning
        , MAX(CASE WHEN abbreviation = 'Motor Deficit' THEN value END) AS motor_deficit_value
        , SUM(CASE WHEN abbreviation = 'Motor Deficit' THEN warning END) AS motor_deficit_warning
        , MAX(CASE WHEN abbreviation = 'Goal Richmond-RAS Scale' THEN value END) AS goal_richmond_ras_scale_value
        , SUM(CASE WHEN abbreviation = 'Goal Richmond-RAS Scale' THEN warning END) AS goal_richmond_ras_scale_warning
        , MAX(CASE WHEN abbreviation = 'PNC-1 Appearance' THEN value END) AS pnc_1_appearance_value
        , SUM(CASE WHEN abbreviation = 'PNC-1 Appearance' THEN warning END) AS pnc_1_appearance_warning
        , CAST(MAX(CASE WHEN abbreviation = 'PNC-1 Bolus (mL)' THEN value END) AS FLOAT) AS pnc_1_bolus_ml_value
        , SUM(CASE WHEN abbreviation = 'PNC-1 Bolus (mL)' THEN warning END) AS pnc_1_bolus_ml_warning
        , CAST(MAX(CASE WHEN abbreviation = 'PNC-1 Infusion Rate (mL/hr)' THEN value END) AS FLOAT) AS pnc_1_infusion_rate_ml_hr_value
        , SUM(CASE WHEN abbreviation = 'PNC-1 Infusion Rate (mL/hr)' THEN warning END) AS pnc_1_infusion_rate_ml_hr_warning
        , MAX(CASE WHEN abbreviation = 'PNC-1 Location' THEN value END) AS pnc_1_location_value
        , SUM(CASE WHEN abbreviation = 'PNC-1 Location' THEN warning END) AS pnc_1_location_warning
        , MAX(CASE WHEN abbreviation = 'PNC-1 Medication' THEN value END) AS pnc_1_medication_value
        , SUM(CASE WHEN abbreviation = 'PNC-1 Medication' THEN warning END) AS pnc_1_medication_warning
        , MAX(CASE WHEN abbreviation = 'PNC-1 Motor Deficit' THEN value END) AS pnc_1_motor_deficit_value
        , SUM(CASE WHEN abbreviation = 'PNC-1 Motor Deficit' THEN warning END) AS pnc_1_motor_deficit_warning
        , MAX(CASE WHEN abbreviation = 'PNC-2 Appearance' THEN value END) AS pnc_2_appearance_value
        , SUM(CASE WHEN abbreviation = 'PNC-2 Appearance' THEN warning END) AS pnc_2_appearance_warning
        , CAST(MAX(CASE WHEN abbreviation = 'PNC-2 Infusion Rate (mL/hr)' THEN value END) AS FLOAT) AS pnc_2_infusion_rate_ml_hr_value
        , SUM(CASE WHEN abbreviation = 'PNC-2 Infusion Rate (mL/hr)' THEN warning END) AS pnc_2_infusion_rate_ml_hr_warning
        , MAX(CASE WHEN abbreviation = 'PNC-2 Location' THEN value END) AS pnc_2_location_value
        , SUM(CASE WHEN abbreviation = 'PNC-2 Location' THEN warning END) AS pnc_2_location_warning
        , MAX(CASE WHEN abbreviation = 'PNC-2 Medication' THEN value END) AS pnc_2_medication_value
        , SUM(CASE WHEN abbreviation = 'PNC-2 Medication' THEN warning END) AS pnc_2_medication_warning
        , MAX(CASE WHEN abbreviation = 'PNC-2 Motor Deficit' THEN value END) AS pnc_2_motor_deficit_value
        , SUM(CASE WHEN abbreviation = 'PNC-2 Motor Deficit' THEN warning END) AS pnc_2_motor_deficit_warning
        , MAX(CASE WHEN abbreviation = 'CPOT-Pain Assessment Method' THEN value END) AS cpot_pain_assessment_method_value
        , SUM(CASE WHEN abbreviation = 'CPOT-Pain Assessment Method' THEN warning END) AS cpot_pain_assessment_method_warning
        , MAX(CASE WHEN abbreviation = 'CPOT-Pain Management' THEN value END) AS cpot_pain_management_value
        , SUM(CASE WHEN abbreviation = 'CPOT-Pain Management' THEN warning END) AS cpot_pain_management_warning
        , MAX(CASE WHEN abbreviation = 'Pain Level' THEN value END) AS pain_level_value
        , SUM(CASE WHEN abbreviation = 'Pain Level' THEN warning END) AS pain_level_warning
        , MAX(CASE WHEN abbreviation = 'Pain Level Acceptable' THEN value END) AS pain_level_acceptable_value
        , SUM(CASE WHEN abbreviation = 'Pain Level Acceptable' THEN warning END) AS pain_level_acceptable_warning
        , MAX(CASE WHEN abbreviation = 'Pain Level Acceptable (PreIntervention)' THEN value END) AS pain_level_acceptable_preintervention_value
        , SUM(CASE WHEN abbreviation = 'Pain Level Acceptable (PreIntervention)' THEN warning END) AS pain_level_acceptable_preintervention_warning
        , MAX(CASE WHEN abbreviation = 'Pain Level Response' THEN value END) AS pain_level_response_value
        , SUM(CASE WHEN abbreviation = 'Pain Level Response' THEN warning END) AS pain_level_response_warning
        , MAX(CASE WHEN abbreviation = 'Pain Management' THEN value END) AS pain_management_value
        , SUM(CASE WHEN abbreviation = 'Pain Management' THEN warning END) AS pain_management_warning
        , MAX(CASE WHEN abbreviation = 'NMB Medication' THEN value END) AS nmb_medication_value
        , SUM(CASE WHEN abbreviation = 'NMB Medication' THEN warning END) AS nmb_medication_warning
        , CAST(MAX(CASE WHEN abbreviation = 'Epidural Infusion Rate (mL/hr)' THEN value END) AS FLOAT) AS epidural_infusion_rate_ml_hr_value
        , SUM(CASE WHEN abbreviation = 'Epidural Infusion Rate (mL/hr)' THEN warning END) AS epidural_infusion_rate_ml_hr_warning
        , MAX(CASE WHEN abbreviation = 'Epidural Medication' THEN value END) AS epidural_medication_value
        , SUM(CASE WHEN abbreviation = 'Epidural Medication' THEN warning END) AS epidural_medication_warning
        , CAST(MAX(CASE WHEN abbreviation = 'PCA 1 hour limit' THEN value END) AS FLOAT) AS pca_1_hour_limit_value
        , SUM(CASE WHEN abbreviation = 'PCA 1 hour limit' THEN warning END) AS pca_1_hour_limit_warning
        , CAST(MAX(CASE WHEN abbreviation = 'PCA attempt' THEN value END) AS FLOAT) AS pca_attempt_value
        , SUM(CASE WHEN abbreviation = 'PCA attempt' THEN warning END) AS pca_attempt_warning
        , CAST(MAX(CASE WHEN abbreviation = 'PCA basal rate (mL/hour)' THEN value END) AS FLOAT) AS pca_basal_rate_ml_hour_value
        , SUM(CASE WHEN abbreviation = 'PCA basal rate (mL/hour)' THEN warning END) AS pca_basal_rate_ml_hour_warning
        , CAST(MAX(CASE WHEN abbreviation = 'PCA bolus' THEN value END) AS FLOAT) AS pca_bolus_value
        , SUM(CASE WHEN abbreviation = 'PCA bolus' THEN warning END) AS pca_bolus_warning
        , MAX(CASE WHEN abbreviation = 'PCA cleared' THEN value END) AS pca_cleared_value
        , SUM(CASE WHEN abbreviation = 'PCA cleared' THEN warning END) AS pca_cleared_warning
        , MAX(CASE WHEN abbreviation = 'PCA concentrations' THEN value END) AS pca_concentrations_value
        , SUM(CASE WHEN abbreviation = 'PCA concentrations' THEN warning END) AS pca_concentrations_warning
        , CAST(MAX(CASE WHEN abbreviation = 'PCA dose' THEN value END) AS FLOAT) AS pca_dose_value
        , SUM(CASE WHEN abbreviation = 'PCA dose' THEN warning END) AS pca_dose_warning
        , CAST(MAX(CASE WHEN abbreviation = 'PCA inject' THEN value END) AS FLOAT) AS pca_inject_value
        , SUM(CASE WHEN abbreviation = 'PCA inject' THEN warning END) AS pca_inject_warning
        , CAST(MAX(CASE WHEN abbreviation = 'PCA lockout (min)' THEN value END) AS FLOAT) AS pca_lockout_min_value
        , SUM(CASE WHEN abbreviation = 'PCA lockout (min)' THEN warning END) AS pca_lockout_min_warning
        , MAX(CASE WHEN abbreviation = 'PCA medication' THEN value END) AS pca_medication_value
        , SUM(CASE WHEN abbreviation = 'PCA medication' THEN warning END) AS pca_medication_warning
        , CAST(MAX(CASE WHEN abbreviation = 'PCA total dose' THEN value END) AS FLOAT) AS pca_total_dose_value
        , SUM(CASE WHEN abbreviation = 'PCA total dose' THEN warning END) AS pca_total_dose_warning
        , CAST(MAX(CASE WHEN abbreviation = 'TOF Response' THEN value END) AS FLOAT) AS tof_response_value
        , SUM(CASE WHEN abbreviation = 'TOF Response' THEN warning END) AS tof_response_warning
        , MAX(CASE WHEN abbreviation = 'TOF Twitch' THEN value END) AS tof_twitch_value
        , SUM(CASE WHEN abbreviation = 'TOF Twitch' THEN warning END) AS tof_twitch_warning
        , CAST(MAX(CASE WHEN abbreviation = 'Current Used/mA' THEN value END) AS FLOAT) AS current_used_ma_value
        , SUM(CASE WHEN abbreviation = 'Current Used/mA' THEN warning END) AS current_used_ma_warning
        , MAX(CASE WHEN abbreviation = 'Daily Wake Up' THEN value END) AS daily_wake_up_value
        , SUM(CASE WHEN abbreviation = 'Daily Wake Up' THEN warning END) AS daily_wake_up_warning
        , MAX(CASE WHEN abbreviation = 'Daily Wake Up Deferred' THEN value END) AS daily_wake_up_deferred_value
        , SUM(CASE WHEN abbreviation = 'Daily Wake Up Deferred' THEN warning END) AS daily_wake_up_deferred_warning
        , MAX(CASE WHEN abbreviation = 'Untoward Effect' THEN value END) AS untoward_effect_value
        , SUM(CASE WHEN abbreviation = 'Untoward Effect' THEN warning END) AS untoward_effect_warning
        , MAX(CASE WHEN abbreviation = 'PT Splint Location #1' THEN value END) AS pt_splint_location_1_value
        , SUM(CASE WHEN abbreviation = 'PT Splint Location #1' THEN warning END) AS pt_splint_location_1_warning
        , MAX(CASE WHEN abbreviation = 'PT Splint Location #2' THEN value END) AS pt_splint_location_2_value
        , SUM(CASE WHEN abbreviation = 'PT Splint Location #2' THEN warning END) AS pt_splint_location_2_warning
        , MAX(CASE WHEN abbreviation = 'PT Splint Status #1' THEN value END) AS pt_splint_status_1_value
        , SUM(CASE WHEN abbreviation = 'PT Splint Status #1' THEN warning END) AS pt_splint_status_1_warning
        , MAX(CASE WHEN abbreviation = 'PT Splint Status #2' THEN value END) AS pt_splint_status_2_value
        , SUM(CASE WHEN abbreviation = 'PT Splint Status #2' THEN warning END) AS pt_splint_status_2_warning
        , CAST(MAX(CASE WHEN abbreviation = 'PTT' THEN value END) AS FLOAT) AS ptt_value
        , SUM(CASE WHEN abbreviation = 'PTT' THEN warning END) AS ptt_warning
        , CAST(MAX(CASE WHEN abbreviation = 'INR' THEN value END) AS FLOAT) AS inr_value
        , SUM(CASE WHEN abbreviation = 'INR' THEN warning END) AS inr_warning
        , CAST(MAX(CASE WHEN abbreviation = 'AST' THEN value END) AS FLOAT) AS ast_value
        , SUM(CASE WHEN abbreviation = 'AST' THEN warning END) AS ast_warning
        , CAST(MAX(CASE WHEN abbreviation = 'ALT' THEN value END) AS FLOAT) AS alt_value
        , SUM(CASE WHEN abbreviation = 'ALT' THEN warning END) AS alt_warning
        , CAST(MAX(CASE WHEN abbreviation = 'Direct Bilirubin' THEN value END) AS FLOAT) AS direct_bilirubin_value
        , SUM(CASE WHEN abbreviation = 'Direct Bilirubin' THEN warning END) AS direct_bilirubin_warning
        , CAST(MAX(CASE WHEN abbreviation = 'Total Bilirubin' THEN value END) AS FLOAT) AS total_bilirubin_value
        , SUM(CASE WHEN abbreviation = 'Total Bilirubin' THEN warning END) AS total_bilirubin_warning
    FROM filtered_chartevents
    GROUP BY subject_id, hadm_id, charttime, charttime_day;
    """
)
