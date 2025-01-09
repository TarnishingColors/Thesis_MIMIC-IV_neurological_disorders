import os
import numpy as np
import pandas as pd
from typing import Union, Dict, List, Tuple
from sklearn.model_selection import train_test_split, cross_val_score
import xgboost as xgb
from sklearn.linear_model import LinearRegression
from catboost import CatBoostRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import (
    mean_squared_error,
    mean_absolute_error,
    r2_score,
    accuracy_score,
    precision_score,
    recall_score,
    roc_auc_score,
    f1_score
)
import joblib
import optuna

ModelRegressor = Union[
    LinearRegression, RandomForestRegressor, CatBoostRegressor, xgb.XGBRegressor
]


def rmse(y_true, y_pred):
    return np.sqrt(mean_squared_error(y_true, y_pred))


def learn_models(
        models: Dict[str, ModelRegressor],
        dfs: Union[List[pd.DataFrame], Dict[str, List[pd.DataFrame]]],
        folder: str,
        regression: bool = True
) -> pd.DataFrame:
    results = []
    os.makedirs(f'models/{folder}', exist_ok=True)

    for model_name, model in models.items():
        print(f"Training {model_name}...")
        X_train, X_test, y_train, y_test = dfs[model_name] if type(dfs) == dict else dfs

        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        joblib.dump(model, f'models/{folder}/{model_name}.joblib')

        if regression:
            mse = mean_squared_error(y_test, y_pred)
            mae = mean_absolute_error(y_test, y_pred)
            r2 = r2_score(y_test, y_pred)
            rmse_value = rmse(y_test, y_pred)

            results.append({
                'model': model_name,
                'MAE': mae,
                'MSE': mse,
                'RMSE': rmse_value,
                'R2': r2
            })

            print(f"{model_name}: RMSE = {rmse_value}, MAE = {mae}, R² = {r2}")

        else:
            accuracy = accuracy_score(y_test, y_pred)
            precision = precision_score(y_test, y_pred, average='weighted', zero_division=0)
            recall = recall_score(y_test, y_pred, average='weighted', zero_division=0)
            f1 = f1_score(y_test, y_pred, average=None)  # Per-class F1-score

            overall_precision = precision_score(y_test, y_pred, average="weighted")  # Weighted by class support
            overall_recall = recall_score(y_test, y_pred, average="weighted")
            overall_f1 = f1_score(y_test, y_pred, average="weighted")

            results.append({
                'model': model_name,
                'Accuracy': accuracy,
                'Precision': precision,
                'Recall': recall,
                'F1-Score': f1
            })

            print(
                f"{model_name} (Classification): Accuracy = {accuracy}, Precision = {precision}, Recall = {recall}, F1-Score = {f1}"
            )

    results_df = pd.DataFrame(results)
    return results_df


class ModelOptimizer:
    def __init__(self, df: pd.DataFrame, label: str = 'los', test_size: float = 0.3, random_state: int = 42):
        self.df = df.dropna().copy()
        self.label = label
        self.test_size = test_size
        self.random_state = random_state
        self.model = None

        self.X_train, self.X_test, self.y_train, self.y_test = self.prepare_data()

    def prepare_data(self):
        X = self.df.drop(self.label, axis=1).values
        y = self.df[self.label].values
        return train_test_split(X, y, test_size=self.test_size, random_state=self.random_state)

    def objective(self, trial, model_name: str):
        if model_name == 'Random Forest':
            n_estimators = trial.suggest_int('n_estimators', 50, 200)
            max_depth = trial.suggest_int('max_depth', 5, 20)
            self.model = RandomForestRegressor(n_estimators=n_estimators, max_depth=max_depth, n_jobs=-1)

        elif model_name == 'XGBoost':
            n_estimators = trial.suggest_int('n_estimators', 50, 200)
            max_depth = trial.suggest_int('max_depth', 3, 10)
            learning_rate = trial.suggest_loguniform('learning_rate', 0.01, 0.3)
            self.model = xgb.XGBRegressor(n_estimators=n_estimators, max_depth=max_depth, learning_rate=learning_rate, n_jobs=-1)

        elif model_name == 'CatBoost':
            n_estimators = trial.suggest_int('n_estimators', 50, 200)
            depth = trial.suggest_int('depth', 4, 10)
            learning_rate = trial.suggest_loguniform('learning_rate', 0.01, 0.3)
            self.model = CatBoostRegressor(n_estimators=n_estimators, depth=depth, learning_rate=learning_rate, verbose=0)

        self.model.fit(self.X_train, self.y_train)
        return -np.mean(cross_val_score(self.model, self.X_train, self.y_train, cv=3, scoring='neg_root_mean_squared_error'))

    def optimize_model(self, model_name: str, n_trials: int = 50, random_state: int = 42):
        print(f"Optimizing hyperparameters for {model_name}...")
        study = optuna.create_study(direction='minimize', random_state=random_state)
        study.optimize(lambda trial: self.objective(trial, model_name), n_trials=n_trials)

        print(f"Best parameters for {model_name}: {study.best_params}")
        return study.best_params

    def optimize_models(self, models: list, n_trials: int = 50, random_state: int = 42):
        best_params_dict = {}
        for model_name in models:
            best_params = self.optimize_model(model_name, n_trials=n_trials, random_state=random_state)
            best_params_dict[model_name] = best_params
        return best_params_dict


class FeatureExtractor:
    def __init__(self, features):
        self.features = features

    def extract_feature_importance(self, folder: str, model_name: str) -> pd.Series:
        model = joblib.load(f'models/{folder}/{model_name}.joblib')
        feature_importance = pd.Series(model.feature_importances_, index=self.features) \
            if model_name in ['Random Forest', 'XGBoost', 'CatBoost'] \
            else pd.Series(model.coef_, index=self.features)
        return feature_importance.abs().sort_values(ascending=False)

    def select_k_most_relevant_features_from_df(
            self, k: int, folder: str, model_name: str, X_train: pd.DataFrame, X_test: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        top_k_features = self.extract_feature_importance(folder, model_name).head(k).keys().tolist()
        return (
            X_train[X_train.columns.intersection(top_k_features)], X_test[X_test.columns.intersection(top_k_features)]
        )
