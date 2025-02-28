"""Implementation of ML models"""

from typing import List, Tuple
import os
import sys
sys.path.append(f'{os.getcwd()}/data_transformation/data_transfer')
# pylint: disable=wrong-import-position
from utils import DataTransfer, Connection
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Masking
from tensorflow.keras.preprocessing.sequence import pad_sequences
from tensorflow.keras.utils import to_categorical
# pylint: enable=wrong-import-position


chartevent_tests = [
    'hr_value',
    'hr_warning',
    'rr_value',
    'rr_warning',
    'abpm_value',
    'abpm_warning',
    'abpd_value',
    'abpd_warning',
    'abps_value',
    'abps_warning',
    'nbpm_value',
    'nbpm_warning',
    'nbpd_value',
    'nbpd_warning',
    'nbps_value',
    'nbps_warning',
    'forehead_spo2_value',
    'forehead_spo2_warning',
    'spo2_desat_limit_value',
    'spo2_desat_limit_warning',
    'temperature_c_value',
    'temperature_c_warning',
    'temperature_f_value',
    'temperature_f_warning',
    'cerebral_t_value',
    'cerebral_t_warning',
    'eye_opening_value',
    'eye_opening_warning',
    'motor_response_value',
    'motor_response_warning',
    'verbal_response_value',
    'verbal_response_warning',
    'pupil_response_l_value',
    'pupil_response_l_warning',
    'pupil_response_r_value',
    'pupil_response_r_warning',
    'ph_arterial_value',
    'ph_arterial_warning',
    'ph_venous_value',
    'ph_venous_warning',
    'hco3_serum_value',
    'hco3_serum_warning',
    'sodium_serum_value',
    'sodium_serum_warning',
    'potassium_serum_value',
    'potassium_serum_warning',
    'chloride_serum_value',
    'chloride_serum_warning',
    'bun_value',
    'bun_warning',
    'ventilator_mode_value',
    'ventilator_mode_warning',
    'ventilator_mode_hamilton_value',
    'ventilator_mode_hamilton_warning',
    'ventilator_type_value',
    'ventilator_type_warning',
    'bis_emg_value',
    'bis_emg_warning',
    'bis_index_range_value',
    'bis_index_range_warning',
    'delirium_assessment_value',
    'delirium_assessment_warning',
    'cam_icu_altered_loc_value',
    'cam_icu_altered_loc_warning',
    'cam_icu_disorganized_thinking_value',
    'cam_icu_disorganized_thinking_warning',
    'cam_icu_inattention_value',
    'cam_icu_inattention_warning',
    'motor_deficit_value',
    'motor_deficit_warning',
    'goal_richmond_ras_scale_value',
    'goal_richmond_ras_scale_warning',
    'pnc_1_appearance_value',
    'pnc_1_appearance_warning',
    'pnc_1_bolus_ml_value',
    'pnc_1_bolus_ml_warning',
    'pnc_1_infusion_rate_ml_hr_value',
    'pnc_1_infusion_rate_ml_hr_warning',
    'pnc_1_location_value',
    'pnc_1_location_warning',
    'pnc_1_medication_value',
    'pnc_1_medication_warning',
    'pnc_1_motor_deficit_value',
    'pnc_1_motor_deficit_warning',
    'pnc_2_appearance_value',
    'pnc_2_appearance_warning',
    'pnc_2_infusion_rate_ml_hr_value',
    'pnc_2_infusion_rate_ml_hr_warning',
    'pnc_2_location_value',
    'pnc_2_location_warning',
    'pnc_2_medication_value',
    'pnc_2_medication_warning',
    'pnc_2_motor_deficit_value',
    'pnc_2_motor_deficit_warning',
    'cpot_pain_assessment_method_value',
    'cpot_pain_assessment_method_warning',
    'cpot_pain_management_value',
    'cpot_pain_management_warning',
    'pain_level_value',
    'pain_level_warning',
    'pain_level_acceptable_value',
    'pain_level_acceptable_warning',
    'pain_level_acceptable_preintervention_value',
    'pain_level_acceptable_preintervention_warning',
    'pain_level_response_value',
    'pain_level_response_warning',
    'pain_management_value',
    'pain_management_warning',
    'nmb_medication_value',
    'nmb_medication_warning',
    'epidural_infusion_rate_ml_hr_value',
    'epidural_infusion_rate_ml_hr_warning',
    'epidural_medication_value',
    'epidural_medication_warning',
    'pca_1_hour_limit_value',
    'pca_1_hour_limit_warning',
    'pca_attempt_value',
    'pca_attempt_warning',
    'pca_basal_rate_ml_hour_value',
    'pca_basal_rate_ml_hour_warning',
    'pca_bolus_value',
    'pca_bolus_warning',
    'pca_cleared_value',
    'pca_cleared_warning',
    'pca_concentrations_value',
    'pca_concentrations_warning',
    'pca_dose_value',
    'pca_dose_warning',
    'pca_inject_value',
    'pca_inject_warning',
    'pca_lockout_min_value',
    'pca_lockout_min_warning',
    'pca_medication_value',
    'pca_medication_warning',
    'pca_total_dose_value',
    'pca_total_dose_warning',
    'tof_response_value',
    'tof_response_warning',
    'tof_twitch_value',
    'tof_twitch_warning',
    'current_used_ma_value',
    'current_used_ma_warning',
    'daily_wake_up_value',
    'daily_wake_up_warning',
    'daily_wake_up_deferred_value',
    'daily_wake_up_deferred_warning',
    'untoward_effect_value',
    'untoward_effect_warning',
    'pt_splint_location_1_value',
    'pt_splint_location_1_warning',
    'pt_splint_location_2_value',
    'pt_splint_location_2_warning',
    'pt_splint_status_1_value',
    'pt_splint_status_1_warning',
    'pt_splint_status_2_value',
    'pt_splint_status_2_warning',
    'ptt_value',
    'ptt_warning',
    'inr_value',
    'inr_warning',
    'ast_value',
    'ast_warning',
    'alt_value',
    'alt_warning',
    'direct_bilirubin_value',
    'direct_bilirubin_warning',
    'total_bilirubin_value',
    'total_bilirubin_warning'
]


def classify_los(los):
    if los < 3:
        los = 0
    elif los < 8:
        los = 1
    else:
        los = 2

    return los


class LSTMModel:
    """
    LSTM model constructor
    """
    def __init__(self, chartevent_tests: List[str], connection: Connection):
        """
        :param chartevent_tests: list of chartevent tests
        :param connection: connection object
        """

        self.model = Sequential()
        self.model.add(Masking(mask_value=0.0, input_shape=(None, len(chartevent_tests)))) # Mask for variable-length sequences
        self.model.add(LSTM(128, return_sequences=False))
        self.model.add(Dense(3, activation='softmax'))
        self.model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])
        self.scaler = MinMaxScaler()
        self.chartevent_tests = chartevent_tests
        self.dt = DataTransfer(connection)

    def read_transform_data(
            self,
            df: pd.DataFrame,
            with_sliding_window: bool = False,
            size: int = 256,
            step: int = 128,
            classification: bool = True
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        :param df: dataframe
        :param with_sliding_window: flag for sliding window
        :param size: size of sliding window
        :param step: step of sliding window
        :return: tuple consisting of sequences and targets
        """

        df = df.sort_values(by=['hadm_id', 'charttime'])

        df = df.drop(columns=['subject_id'])

        for test in self.chartevent_tests:
            if 'value' in test:
                if df[test].dtype in (int, float):
                    df[[test]] = self.scaler.fit_transform(df[[test]])
                    df[[test]].interpolate(method='linear', inplace=True)
                else:
                    df[test].ffill(inplace=True)
                    encoder = LabelEncoder()
                    df[test] = encoder.fit_transform(df[test].astype(str))
            elif 'warning' in test:
                df[test] = df[test].fillna(0)
                df[test] = df[test].astype(int)

        df.fillna(value=-1, inplace=True)

        sequences = []
        los_targets = []
        for hadm_id, group in df.groupby('hadm_id'):
            final_time = self.dt.fetch_data(
                f"""
                SELECT COALESCE(i.outtime, a.deathtime) 
                FROM raw.icustays i
                JOIN raw.admissions a
                ON i.hadm_id = a.hadm_id
                WHERE i.hadm_id = {hadm_id}
                """
            ).iloc[0].tolist()[0]
            sequence = group[self.chartevent_tests].values

            if with_sliding_window:
                for start in range(0, len(sequence) - size + 1, step):
                    end = start + size
                    window = sequence[start:end]

                    remaining_los = (final_time - group['charttime'].iloc[end - 1]).total_seconds() / 3600 / 24
                    sequences.append(window)
                    if classification:
                        remaining_los = classify_los(remaining_los)
                    los_targets.append(remaining_los)

            else:
                los = (final_time - group['charttime'].min()).total_seconds() / 3600 / 24
                if classification:
                    los = classify_los(los)
                sequences.append(sequence)
                los_targets.append(los)

        sequences = pad_sequences(sequences, dtype='float32', padding='post')

        los_targets = to_categorical(los_targets, num_classes=3)

        return np.array(sequences), np.array(los_targets)

    def split_data(self, X, y, test_size=0.2) -> List:
        """
        :param X: data
        :param y: targets
        :param test_size: test size
        :return: data split in train and test
        """

        return train_test_split(X, y, test_size=test_size, random_state=42)

    def train_model(self, X_train, y_train, epochs=50, batch_size=16) -> None:
        """
        :param X_train: training data
        :param y_train: targets of training data
        :param epochs: number of epochs
        :param batch_size: size of batch
        """

        self.model.fit(X_train, y_train, epochs=epochs, batch_size=batch_size, validation_split=0.2)

    def predict(self, X_test) -> List[float]:
        """
        :param X_test: test data
        :return: list of predictions
        """

        return np.argmax(self.model.predict(X_test), axis=1)
