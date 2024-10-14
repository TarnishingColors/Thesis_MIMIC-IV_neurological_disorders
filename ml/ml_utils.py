import os
import numpy as np
import pandas as pd
from typing import Union, Dict, List
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split, cross_val_score, KFold
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.svm import SVR
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import make_scorer, mean_squared_error, mean_absolute_error, r2_score
import joblib

ModelRegressor = Union[
    LinearRegression, Ridge, Lasso, SVR, DecisionTreeRegressor, RandomForestRegressor, GradientBoostingRegressor]


def rmse(y_true, y_pred):
    differences = y_pred - y_true
    differences_squared = differences ** 2
    mean_of_differences_squared = differences_squared.mean()

    return np.sqrt(mean_of_differences_squared)


def learn_models(
        models: Dict[str, ModelRegressor],
        df: Union[pd.DataFrame, Dict[str, pd.DataFrame]],
        folder: str,
        label: str = 'los',
        kf: KFold = KFold(n_splits=5, shuffle=True, random_state=42)
):
    results = []

    os.makedirs(f'models/{folder}', exist_ok=True)

    rmse_scorer = make_scorer(rmse, greater_is_better=False)
    scorer = make_scorer(mean_squared_error, greater_is_better=False)
    mae_scorer = make_scorer(mean_absolute_error, greater_is_better=False)
    r2_scorer = make_scorer(r2_score, greater_is_better=False)

    df_list = 1 if type(df) == dict else 0

    if not df_list:
        cur_df = df.copy()
        cur_df = cur_df.dropna()
        X = cur_df.drop(label, axis=1)
        y = cur_df[label]

    for i, (name, model) in enumerate(models.items()):
        if df_list:
            cur_df = df[name].copy()
            cur_df = cur_df.dropna()
            X = cur_df.drop(label, axis=1)
            y = cur_df[label]

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

        if 'SVR' in name:
            scaler = StandardScaler()
            X_train = scaler.fit_transform(X_train)
            X_test = scaler.transform(X_test)

        cv_rmse_scores = cross_val_score(model, X_train, y_train, cv=kf, scoring=rmse_scorer)
        cv_scores = cross_val_score(model, X_train, y_train, cv=kf, scoring=scorer)
        cv_mae_scores = cross_val_score(model, X_train, y_train, cv=kf, scoring=mae_scorer)
        cv_r2_scores = cross_val_score(model, X_train, y_train, cv=kf, scoring=r2_scorer)

        model.fit(X_train, y_train)

        joblib.dump(model, f'models/{folder}/{name}.joblib')

        results.append({
            'model': name,
            'RMSE': -np.mean(cv_rmse_scores),
            'MAE': -np.mean(cv_mae_scores),
            'R2': -np.mean(cv_r2_scores)
        })
        print(f"{name}: Mean MSE (cross-validated) = {-np.mean(cv_scores)}")
        print(f"{name}: Mean MAE (cross-validated) = {-np.mean(cv_mae_scores)}")
        print(f"{name}: Mean R^2 (cross-validated) = {-np.mean(cv_r2_scores)}")

    return pd.DataFrame(columns=['model', 'RMSE', 'MAE', 'R2'], data=results)


def extract_feature_importance(folder: str, model_name: str, features: List[str]):
    model = joblib.load(f'models/{folder}/{model_name}.joblib')
    feature_importance = pd.Series(model.feature_importances_, index=features) \
        if model_name in ['Decision Tree Regression', 'Random Forest Regression', 'Gradient Boosting Regression'] \
        else pd.Series(model.coef_, index=features)
    return feature_importance.abs().sort_values(ascending=False)
