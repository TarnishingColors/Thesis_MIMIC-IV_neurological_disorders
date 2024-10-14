import os
import numpy as np
import pandas as pd
from typing import Union, Dict, List
from sklearn.model_selection import train_test_split, cross_val_score
import xgboost as xgb
from sklearn.linear_model import LinearRegression
from catboost import CatBoostRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
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
) -> pd.DataFrame:
    results = []
    os.makedirs(f'models/{folder}', exist_ok=True)

    for model_name, model in models.items():
        print(f"Training {model_name}...")
        X_train, X_test, y_train, y_test = dfs[model_name] if type(dfs) == dict else dfs

        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        joblib.dump(model, f'models/{folder}/{model_name}.joblib')
        mse = mean_squared_error(y_test, y_pred)
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        rmse_value = rmse(y_test, y_pred)

        results.append({
            'model': model_name,
            'MSE': mse,
            'MAE': mae,
            'RMSE': rmse_value,
            'R2': r2
        })

        print(f"{model_name}: RMSE = {rmse_value}, MAE = {mae}, R² = {r2}")

    results_df = pd.DataFrame(results)
    return results_df


# def objective(trial, model_name: str, X_train, y_train):
#     # Define hyperparameter search space for each model
#     if model_name == 'Ridge Regression':
#         alpha = trial.suggest_loguniform('alpha', 0.001, 10.0)
#         model = Ridge(alpha=alpha)
#
#     elif model_name == 'Lasso Regression':
#         alpha = trial.suggest_loguniform('alpha', 0.001, 1.0)
#         model = Lasso(alpha=alpha)
#
#     elif model_name == 'Support Vector Regression':
#         C = trial.suggest_loguniform('C', 0.1, 10)
#         kernel = trial.suggest_categorical('kernel', ['linear', 'rbf'])
#         model = SVR(C=C, kernel=kernel)
#
#     elif model_name == 'Decision Tree Regression':
#         max_depth = trial.suggest_int('max_depth', 3, 10)
#         model = DecisionTreeRegressor(max_depth=max_depth)
#
#     elif model_name == 'Random Forest Regression':
#         n_estimators = trial.suggest_int('n_estimators', 50, 200)
#         max_depth = trial.suggest_int('max_depth', 5, 20)
#         model = RandomForestRegressor(n_estimators=n_estimators, max_depth=max_depth, n_jobs=-1)
#
#     elif model_name == 'Gradient Boosting Regression':
#         n_estimators = trial.suggest_int('n_estimators', 50, 200)
#         learning_rate = trial.suggest_loguniform('learning_rate', 0.01, 0.2)
#         model = GradientBoostingRegressor(n_estimators=n_estimators, learning_rate=learning_rate)
#
#     # Cross-validation or training
#     model.fit(X_train, y_train)
#     return -np.mean(cross_val_score(model, X_train, y_train, cv=3, scoring='neg_root_mean_squared_error'))


# def hyperparameter_optimization(
#         models: Dict[str, ModelRegressor],
#         df: pd.DataFrame,
#         label: str = 'los',
#         n_trials: int = 50,
#         random_state: int = 42
# ) -> Dict[str, Dict[str, float]]:
#     best_params_dict = {}
#
#     # Preprocessing data
#     cur_df = df.dropna().copy()
#     X = cur_df.drop(label, axis=1).values
#     y = cur_df[label].values
#
#     for model_name in models.keys():
#         print(f"Optimizing hyperparameters for {model_name}...")
#
#         # Optuna study for hyperparameter optimization
#         study = optuna.create_study(direction='minimize', random_state=random_state)
#         study.optimize(lambda trial: objective(trial, model_name, X, y), n_trials=n_trials)
#
#         best_params_dict[model_name] = study.best_params
#         print(f"Best parameters for {model_name}: {study.best_params}")
#
#     return best_params_dict


def extract_feature_importance(folder: str, model_name: str, features: List[str]):
    model = joblib.load(f'models/{folder}/{model_name}.joblib')
    feature_importance = pd.Series(model.feature_importances_, index=features) \
        if model_name in ['Random Forest', 'XGBoost'] \
        else pd.Series(model.coef_, index=features)
    return feature_importance.abs().sort_values(ascending=False)
