"""Module to create chartevents_by_minute mart"""

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
    CREATE TABLE IF NOT EXISTS mart.chartevents_by_minute
    (LIKE ods.chartevents_grouped INCLUDING ALL);
    ALTER TABLE mart.chartevents_by_minute
    DROP COLUMN IF EXISTS charttime_week;
    """
)

# in case previous attempt was interrupted
already_there_hadm_ids = dt.fetch_data(
    """
    SELECT DISTINCT hadm_id
    FROM mart.chartevents_by_minute;
    """
)['hadm_id'].tolist()

hadm_ids = dt.fetch_data(
    """
    SELECT DISTINCT hadm_id
    FROM ods.chartevents_grouped;
    """
)['hadm_id'].tolist()

hadm_ids = [hadm_id for hadm_id in hadm_ids if hadm_id not in already_there_hadm_ids]

for i, hadm_id in enumerate(hadm_ids):
    min_charttime, max_charttime, subject_id = dt.fetch_data(
        f"""
        SELECT MIN(charttime) AS min_charttime
            , MAX(charttime) AS max_charttime
            , MIN(subject_id) AS subject_id
        FROM ods.chartevents_grouped
        WHERE hadm_id = {hadm_id}
        """
    ).iloc[0].tolist()
    print(f'{i + 1} out of {len(hadm_ids)}', hadm_id, subject_id, min_charttime, max_charttime)
    dt.run_query(
        f"""
        DROP TABLE IF EXISTS timestamps;
        CREATE TEMPORARY TABLE timestamps AS (
            SELECT dd
                , {hadm_id} AS hadm_id
            FROM GENERATE_SERIES(
                '{min_charttime}'::TIMESTAMP,
                '{max_charttime}'::TIMESTAMP,
                '1 minute'::INTERVAL
            ) dd
        );
        
        INSERT  INTO mart.chartevents_by_minute
        SELECT {subject_id} AS subject_id
            , {hadm_id} AS hadm_id
            , t.dd AS charttime
            , CASE WHEN t.dd = c.charttime THEN c.hr_value END AS hr_value
            , CASE WHEN t.dd = c.charttime THEN c.hr_warning END AS hr_warning
            , CASE WHEN t.dd = c.charttime THEN c.rr_value END AS rr_value
            , CASE WHEN t.dd = c.charttime THEN c.rr_warning END AS rr_warning
            , CASE WHEN t.dd = c.charttime THEN c.abpm_value END AS abpm_value
            , CASE WHEN t.dd = c.charttime THEN c.abpm_warning END AS abpm_warning
            , CASE WHEN t.dd = c.charttime THEN c.abpd_value END AS abpd_value
            , CASE WHEN t.dd = c.charttime THEN c.abpd_warning END AS abpd_warning
            , CASE WHEN t.dd = c.charttime THEN c.abps_value END AS abps_value
            , CASE WHEN t.dd = c.charttime THEN c.abps_warning END AS abps_warning
            , CASE WHEN t.dd = c.charttime THEN c.nbpm_value END AS nbpm_value
            , CASE WHEN t.dd = c.charttime THEN c.nbpm_warning END AS nbpm_warning
            , CASE WHEN t.dd = c.charttime THEN c.nbpd_value END AS nbpd_value
            , CASE WHEN t.dd = c.charttime THEN c.nbpd_warning END AS nbpd_warning
            , CASE WHEN t.dd = c.charttime THEN c.nbps_value END AS nbps_value
            , CASE WHEN t.dd = c.charttime THEN c.nbps_warning END AS nbps_warning
            , CASE WHEN t.dd = c.charttime THEN c.forehead_spo2_value END AS forehead_spo2_value
            , CASE WHEN t.dd = c.charttime THEN c.forehead_spo2_warning END AS forehead_spo2_warning
            , CASE WHEN t.dd = c.charttime THEN c.spo2_desat_limit_value END AS spo2_desat_limit_value
            , CASE WHEN t.dd = c.charttime THEN c.spo2_desat_limit_warning END AS spo2_desat_limit_warning
            , CASE WHEN t.dd = c.charttime THEN c.temperature_c_value END AS temperature_c_value
            , CASE WHEN t.dd = c.charttime THEN c.temperature_c_warning END AS temperature_c_warning
            , CASE WHEN t.dd = c.charttime THEN c.temperature_f_value END AS temperature_f_value
            , CASE WHEN t.dd = c.charttime THEN c.temperature_f_warning END AS temperature_f_warning
            , CASE WHEN t.dd = c.charttime THEN c.cerebral_t_value END AS cerebral_t_value
            , CASE WHEN t.dd = c.charttime THEN c.cerebral_t_warning END AS cerebral_t_warning
            , CASE WHEN t.dd = c.charttime THEN c.eye_opening_value END AS eye_opening_value
            , CASE WHEN t.dd = c.charttime THEN c.eye_opening_warning END AS eye_opening_warning
            , CASE WHEN t.dd = c.charttime THEN c.motor_response_value END AS motor_response_value
            , CASE WHEN t.dd = c.charttime THEN c.motor_response_warning END AS motor_response_warning
            , CASE WHEN t.dd = c.charttime THEN c.verbal_response_value END AS verbal_response_value
            , CASE WHEN t.dd = c.charttime THEN c.verbal_response_warning END AS verbal_response_warning
            , CASE WHEN t.dd = c.charttime THEN c.pupil_response_l_value END AS pupil_response_l_value
            , CASE WHEN t.dd = c.charttime THEN c.pupil_response_l_warning END AS pupil_response_l_warning
            , CASE WHEN t.dd = c.charttime THEN c.pupil_response_r_value END AS pupil_response_r_value
            , CASE WHEN t.dd = c.charttime THEN c.pupil_response_r_warning END AS pupil_response_r_warning
            , CASE WHEN t.dd = c.charttime THEN c.ph_arterial_value END AS ph_arterial_value
            , CASE WHEN t.dd = c.charttime THEN c.ph_arterial_warning END AS ph_arterial_warning
            , CASE WHEN t.dd = c.charttime THEN c.ph_venous_value END AS ph_venous_value
            , CASE WHEN t.dd = c.charttime THEN c.ph_venous_warning END AS ph_venous_warning
            , CASE WHEN t.dd = c.charttime THEN c.hco3_serum_value END AS hco3_serum_value
            , CASE WHEN t.dd = c.charttime THEN c.hco3_serum_warning END AS hco3_serum_warning
            , CASE WHEN t.dd = c.charttime THEN c.sodium_serum_value END AS sodium_serum_value
            , CASE WHEN t.dd = c.charttime THEN c.sodium_serum_warning END AS sodium_serum_warning
            , CASE WHEN t.dd = c.charttime THEN c.potassium_serum_value END AS potassium_serum_value
            , CASE WHEN t.dd = c.charttime THEN c.potassium_serum_warning END AS potassium_serum_warning
            , CASE WHEN t.dd = c.charttime THEN c.chloride_serum_value END AS chloride_serum_value
            , CASE WHEN t.dd = c.charttime THEN c.chloride_serum_warning END AS chloride_serum_warning
            , CASE WHEN t.dd = c.charttime THEN c.bun_value END AS bun_value
            , CASE WHEN t.dd = c.charttime THEN c.bun_warning END AS bun_warning
            , CASE WHEN t.dd = c.charttime THEN c.ventilator_mode_value END AS ventilator_mode_value
            , CASE WHEN t.dd = c.charttime THEN c.ventilator_mode_warning END AS ventilator_mode_warning
            , CASE WHEN t.dd = c.charttime THEN c.ventilator_mode_hamilton_value END AS ventilator_mode_hamilton_value
            , CASE WHEN t.dd = c.charttime THEN c.ventilator_mode_hamilton_warning END AS ventilator_mode_hamilton_warning
            , CASE WHEN t.dd = c.charttime THEN c.ventilator_type_value END AS ventilator_type_value
            , CASE WHEN t.dd = c.charttime THEN c.ventilator_type_warning END AS ventilator_type_warning
            , CASE WHEN t.dd = c.charttime THEN c.bis_emg_value END AS bis_emg_value
            , CASE WHEN t.dd = c.charttime THEN c.bis_emg_warning END AS bis_emg_warning
            , CASE WHEN t.dd = c.charttime THEN c.bis_index_range_value END AS bis_index_range_value
            , CASE WHEN t.dd = c.charttime THEN c.bis_index_range_warning END AS bis_index_range_warning
            , CASE WHEN t.dd = c.charttime THEN c.delirium_assessment_value END AS delirium_assessment_value
            , CASE WHEN t.dd = c.charttime THEN c.delirium_assessment_warning END AS delirium_assessment_warning
            , CASE WHEN t.dd = c.charttime THEN c.cam_icu_altered_loc_value END AS cam_icu_altered_loc_value
            , CASE WHEN t.dd = c.charttime THEN c.cam_icu_altered_loc_warning END AS cam_icu_altered_loc_warning
            , CASE WHEN t.dd = c.charttime THEN c.cam_icu_disorganized_thinking_value END AS cam_icu_disorganized_thinking_value
            , CASE WHEN t.dd = c.charttime THEN c.cam_icu_disorganized_thinking_warning END AS cam_icu_disorganized_thinking_warning
            , CASE WHEN t.dd = c.charttime THEN c.cam_icu_inattention_value END AS cam_icu_inattention_value
            , CASE WHEN t.dd = c.charttime THEN c.cam_icu_inattention_warning END AS cam_icu_inattention_warning
            , CASE WHEN t.dd = c.charttime THEN c.motor_deficit_value END AS motor_deficit_value
            , CASE WHEN t.dd = c.charttime THEN c.motor_deficit_warning END AS motor_deficit_warning
            , CASE WHEN t.dd = c.charttime THEN c.goal_richmond_ras_scale_value END AS goal_richmond_ras_scale_value
            , CASE WHEN t.dd = c.charttime THEN c.goal_richmond_ras_scale_warning END AS goal_richmond_ras_scale_warning
            , CASE WHEN t.dd = c.charttime THEN c.pnc_1_appearance_value END AS pnc_1_appearance_value
            , CASE WHEN t.dd = c.charttime THEN c.pnc_1_appearance_warning END AS pnc_1_appearance_warning
            , CASE WHEN t.dd = c.charttime THEN c.pnc_1_bolus_ml_value END AS pnc_1_bolus_ml_value
            , CASE WHEN t.dd = c.charttime THEN c.pnc_1_bolus_ml_warning END AS pnc_1_bolus_ml_warning
            , CASE WHEN t.dd = c.charttime THEN c.pnc_1_infusion_rate_ml_hr_value END AS pnc_1_infusion_rate_ml_hr_value
            , CASE WHEN t.dd = c.charttime THEN c.pnc_1_infusion_rate_ml_hr_warning END AS pnc_1_infusion_rate_ml_hr_warning
            , CASE WHEN t.dd = c.charttime THEN c.pnc_1_location_value END AS pnc_1_location_value
            , CASE WHEN t.dd = c.charttime THEN c.pnc_1_location_warning END AS pnc_1_location_warning
            , CASE WHEN t.dd = c.charttime THEN c.pnc_1_medication_value END AS pnc_1_medication_value
            , CASE WHEN t.dd = c.charttime THEN c.pnc_1_medication_warning END AS pnc_1_medication_warning
            , CASE WHEN t.dd = c.charttime THEN c.pnc_1_motor_deficit_value END AS pnc_1_motor_deficit_value
            , CASE WHEN t.dd = c.charttime THEN c.pnc_1_motor_deficit_warning END AS pnc_1_motor_deficit_warning
            , CASE WHEN t.dd = c.charttime THEN c.pnc_2_appearance_value END AS  pnc_2_appearance_value
            , CASE WHEN t.dd = c.charttime THEN c.pnc_2_appearance_warning END AS pnc_2_appearance_warning
            , CASE WHEN t.dd = c.charttime THEN c.pnc_2_infusion_rate_ml_hr_value END AS pnc_2_infusion_rate_ml_hr_value
            , CASE WHEN t.dd = c.charttime THEN c.pnc_2_infusion_rate_ml_hr_warning END AS pnc_2_infusion_rate_ml_hr_warning
            , CASE WHEN t.dd = c.charttime THEN c.pnc_2_location_value END AS pnc_2_location_value
            , CASE WHEN t.dd = c.charttime THEN c.pnc_2_location_warning END AS pnc_2_location_warning
            , CASE WHEN t.dd = c.charttime THEN c.pnc_2_medication_value END AS pnc_2_medication_value
            , CASE WHEN t.dd = c.charttime THEN c.pnc_2_medication_warning END AS pnc_2_medication_warning
            , CASE WHEN t.dd = c.charttime THEN c.pnc_2_motor_deficit_value END AS pnc_2_motor_deficit_value
            , CASE WHEN t.dd = c.charttime THEN c.pnc_2_motor_deficit_warning END AS pnc_2_motor_deficit_warning
            , CASE WHEN t.dd = c.charttime THEN c.cpot_pain_assessment_method_value END AS cpot_pain_assessment_method_value
            , CASE WHEN t.dd = c.charttime THEN c.cpot_pain_assessment_method_warning END AS cpot_pain_assessment_method_warning
            , CASE WHEN t.dd = c.charttime THEN c.cpot_pain_management_value END AS cpot_pain_management_value
            , CASE WHEN t.dd = c.charttime THEN c.cpot_pain_management_warning END AS cpot_pain_management_warning
            , CASE WHEN t.dd = c.charttime THEN c.pain_level_value END AS pain_level_value
            , CASE WHEN t.dd = c.charttime THEN c.pain_level_warning END AS pain_level_warning
            , CASE WHEN t.dd = c.charttime THEN c.pain_level_acceptable_value END AS pain_level_acceptable_value
            , CASE WHEN t.dd = c.charttime THEN c.pain_level_acceptable_warning END AS pain_level_acceptable_warning
            , CASE WHEN t.dd = c.charttime THEN c.pain_level_acceptable_preintervention_value END AS pain_level_acceptable_preintervention_value
            , CASE WHEN t.dd = c.charttime THEN c.pain_level_acceptable_preintervention_warning END AS pain_level_acceptable_preintervention_warning
            , CASE WHEN t.dd = c.charttime THEN c.pain_level_response_value END AS pain_level_response_value
            , CASE WHEN t.dd = c.charttime THEN c.pain_level_response_warning END AS pain_level_response_warning
            , CASE WHEN t.dd = c.charttime THEN c.pain_management_value END AS pain_management_value
            , CASE WHEN t.dd = c.charttime THEN c.pain_management_warning END AS pain_management_warning
            , CASE WHEN t.dd = c.charttime THEN c.nmb_medication_value END AS nmb_medication_value
            , CASE WHEN t.dd = c.charttime THEN c.nmb_medication_warning END AS nmb_medication_warning
            , CASE WHEN t.dd = c.charttime THEN c.epidural_infusion_rate_ml_hr_value END AS epidural_infusion_rate_ml_hr_value
            , CASE WHEN t.dd = c.charttime THEN c.epidural_infusion_rate_ml_hr_warning END AS epidural_infusion_rate_ml_hr_warning
            , CASE WHEN t.dd = c.charttime THEN c.epidural_medication_value END AS epidural_medication_value
            , CASE WHEN t.dd = c.charttime THEN c.epidural_medication_warning END AS epidural_medication_warning
            , CASE WHEN t.dd = c.charttime THEN c.pca_1_hour_limit_value END AS pca_1_hour_limit_value
            , CASE WHEN t.dd = c.charttime THEN c.pca_1_hour_limit_warning END AS pca_1_hour_limit_warning
            , CASE WHEN t.dd = c.charttime THEN c.pca_attempt_value END AS pca_attempt_value
            , CASE WHEN t.dd = c.charttime THEN c.pca_attempt_warning END AS pca_attempt_warning
            , CASE WHEN t.dd = c.charttime THEN c.pca_basal_rate_ml_hour_value END AS pca_basal_rate_ml_hour_value
            , CASE WHEN t.dd = c.charttime THEN c.pca_basal_rate_ml_hour_warning END AS pca_basal_rate_ml_hour_warning
            , CASE WHEN t.dd = c.charttime THEN c.pca_bolus_value END AS pca_bolus_value
            , CASE WHEN t.dd = c.charttime THEN c.pca_bolus_warning END AS pca_bolus_warning
            , CASE WHEN t.dd = c.charttime THEN c.pca_cleared_value END AS pca_cleared_value
            , CASE WHEN t.dd = c.charttime THEN c.pca_cleared_warning END AS pca_cleared_warning
            , CASE WHEN t.dd = c.charttime THEN c.pca_concentrations_value END AS pca_concentrations_value
            , CASE WHEN t.dd = c.charttime THEN c.pca_concentrations_warning END AS pca_concentrations_warning
            , CASE WHEN t.dd = c.charttime THEN c.pca_dose_value END AS pca_dose_value
            , CASE WHEN t.dd = c.charttime THEN c.pca_dose_warning END AS pca_dose_warning
            , CASE WHEN t.dd = c.charttime THEN c.pca_inject_value END AS pca_inject_value
            , CASE WHEN t.dd = c.charttime THEN c.pca_inject_warning END AS pca_inject_warning
            , CASE WHEN t.dd = c.charttime THEN c.pca_lockout_min_value END AS pca_lockout_min_value
            , CASE WHEN t.dd = c.charttime THEN c.pca_lockout_min_warning END AS pca_lockout_min_warning
            , CASE WHEN t.dd = c.charttime THEN c.pca_medication_value END AS pca_medication_value
            , CASE WHEN t.dd = c.charttime THEN c.pca_medication_warning END AS pca_medication_warning
            , CASE WHEN t.dd = c.charttime THEN c.pca_total_dose_value END AS pca_total_dose_value
            , CASE WHEN t.dd = c.charttime THEN c.pca_total_dose_warning END AS pca_total_dose_warning
            , CASE WHEN t.dd = c.charttime THEN c.tof_response_value END AS tof_response_value
            , CASE WHEN t.dd = c.charttime THEN c.tof_response_warning END AS tof_response_warning
            , CASE WHEN t.dd = c.charttime THEN c.tof_twitch_value END AS tof_twitch_value
            , CASE WHEN t.dd = c.charttime THEN c.tof_twitch_warning END AS tof_twitch_warning
            , CASE WHEN t.dd = c.charttime THEN c.current_used_ma_value END AS current_used_ma_value
            , CASE WHEN t.dd = c.charttime THEN c.current_used_ma_warning END AS current_used_ma_warning
            , CASE WHEN t.dd = c.charttime THEN c.daily_wake_up_value END AS daily_wake_up_value
            , CASE WHEN t.dd = c.charttime THEN c.daily_wake_up_warning END AS daily_wake_up_warning
            , CASE WHEN t.dd = c.charttime THEN c.daily_wake_up_deferred_value END AS daily_wake_up_deferred_value
            , CASE WHEN t.dd = c.charttime THEN c.daily_wake_up_deferred_warning END AS daily_wake_up_deferred_warning
            , CASE WHEN t.dd = c.charttime THEN c.untoward_effect_value END AS untoward_effect_value
            , CASE WHEN t.dd = c.charttime THEN c.untoward_effect_warning END AS untoward_effect_warning
            , CASE WHEN t.dd = c.charttime THEN c.pt_splint_location_1_value END AS pt_splint_location_1_value
            , CASE WHEN t.dd = c.charttime THEN c.pt_splint_location_1_warning END AS pt_splint_location_1_warning
            , CASE WHEN t.dd = c.charttime THEN c.pt_splint_location_2_value END AS pt_splint_location_2_value
            , CASE WHEN t.dd = c.charttime THEN c.pt_splint_location_2_warning END AS pt_splint_location_2_warning
            , CASE WHEN t.dd = c.charttime THEN c.pt_splint_status_1_value END AS pt_splint_status_1_value
            , CASE WHEN t.dd = c.charttime THEN c.pt_splint_status_1_warning END AS pt_splint_status_1_warning
            , CASE WHEN t.dd = c.charttime THEN c.pt_splint_status_2_value END AS pt_splint_status_2_value
            , CASE WHEN t.dd = c.charttime THEN c.pt_splint_status_2_warning END AS pt_splint_status_2_warning
            , CASE WHEN t.dd = c.charttime THEN c.ptt_value END AS ptt_value
            , CASE WHEN t.dd = c.charttime THEN c.ptt_warning END AS ptt_warning
            , CASE WHEN t.dd = c.charttime THEN c.inr_value END AS inr_value
            , CASE WHEN t.dd = c.charttime THEN c.inr_warning END AS inr_warning
            , CASE WHEN t.dd = c.charttime THEN c.ast_value END AS ast_value
            , CASE WHEN t.dd = c.charttime THEN c.ast_warning END AS ast_warning
            , CASE WHEN t.dd = c.charttime THEN c.alt_value END AS alt_value
            , CASE WHEN t.dd = c.charttime THEN c.alt_warning END AS alt_warning
            , CASE WHEN t.dd = c.charttime THEN c.direct_bilirubin_value END AS direct_bilirubin_value
            , CASE WHEN t.dd = c.charttime THEN c.direct_bilirubin_warning END AS direct_bilirubin_warning
            , CASE WHEN t.dd = c.charttime THEN c.total_bilirubin_value END AS total_bilirubin_value
            , CASE WHEN t.dd = c.charttime THEN c.total_bilirubin_warning END AS total_bilirubin_warning
        FROM timestamps t 
        LEFT JOIN ods.chartevents_grouped c 
        ON t.dd = c.charttime AND c.hadm_id = {hadm_id};
        """
    )
