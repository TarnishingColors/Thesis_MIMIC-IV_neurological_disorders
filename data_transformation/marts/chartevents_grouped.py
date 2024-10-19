import configparser
from ..data_transfer.utils import Connection, DataTransfer

config = configparser.ConfigParser()
config.read('config.ini')

file_folder = config['raw_data']['file_folder']

dt = DataTransfer(Connection(*(x[1] for x in config.items('database'))))

dt.run_query(
    """
    DROP TABLE IF EXISTS filtered_chartevents;
    CREATE TEMPORARY TABLE filtered_chartevents AS (
        SELECT c.subject_id
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
    
    CREATE TABLE mart.chartevents_grouped
    AS (
        SELECT subject_id
            , hadm_id
            , stay_id
            , caregiver_id
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
            , MAX(CASE WHEN abbreviation = 'PNC-2 Infusion Rate (mL/hr)' THEN value END) AS pnc_2_infusion_rate_ml_hr_value
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
        GROUP BY subject_id, hadm_id, stay_id, caregiver_id, charttime
    )
    """
)
