import os
import pickle
import logging

import geopandas as gpd
import miceforest as mf
import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.inspection import permutation_importance
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, FunctionTransformer
from skopt import BayesSearchCV

from common import execute_query, upload_to_table

logging.basicConfig(level=logging.INFO)

_random_state = 112024
_cat_cols = ["city", "type", "orientation", "age", "parking", "floor",
             "elevator", "furniture", "state", "heating", "water_heating",
             "pets", "loudness"]
_num_cols = ["price", "rooms", "bathrooms", "emissions", "energy", "surface", "latitude", "longitude"]
_transform_columns = ["surface"]
_remove_cols = ["price", "energy", "emissions", "latitude", "longitude", "rooms", "bathrooms"]

_all_cols = _cat_cols + _num_cols

os.chdir("..")


class OutputTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, model, columns=None, cat_columns=None, output_transform=None, target_transform=None):
        self.model = model
        self.columns = columns
        self.cat_columns = cat_columns
        self.output_transform = output_transform
        self.target_transform = target_transform

    def transform_df(self, X, y=None):
        X = pd.DataFrame(X, columns=self.columns)
        for c in self.cat_columns:
            if c in X.columns:
                X[c] = pd.Categorical(X[c])
        return X

    def fit(self, X, y=None):
        if self.target_transform is not None:
            y = self.target_transform(y)
        X = self.transform_df(X)
        self.model.fit(X, y)
        return self

    def predict(self, X):
        X = self.transform_df(X)
        predictions = self.model.predict(X)
        if self.output_transform is not None:
            predictions = self.output_transform(predictions)
        return predictions

    def score(self, X, y):
        if self.target_transform is not None:
            y = self.target_transform(y)
        X = self.transform_df(X)
        return self.model.score(X, y)

    def transform(self, X, y=None):
        return X


class ColumnSelector(BaseEstimator, TransformerMixin):
    def __init__(self, remove_columns):
        self.remove_columns = remove_columns

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        return X[X.columns.difference(self.remove_columns)]


def load_data():
    logging.info("Loading data...")
    q = "SELECT * FROM houses_clean"
    df = execute_query(q)

    return df


def add_extra_data(df):
    logging.info("Adding extra data...")
    loudness_df = gpd.read_file("Files/2017_isofones_total_lden_mapa_estrategic_soroll_bcn.gpkg",
                                layer="2017_Isofones_Total_Lden_Mapa_Estrategic_Soroll_BCN")
    loudness_df = loudness_df.to_crs(epsg=4326)

    bcn_geometry = gpd.read_file("Files/comarques-barcelona.geojson")
    bcn_geometry = bcn_geometry.to_crs(epsg=4326)

    df = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326")
    df = df.sjoin(loudness_df, how="left")
    df = df[df.columns.difference(["fid", "index_right"])]
    df = df.sjoin(bcn_geometry, how="inner")
    df = df[df.columns.difference(["geometry", "cap_comar", "comarca", "nom_comar"])]
    df = df.rename(columns={"Rang": "loudness"})

    return df


def preprocess(df):
    logging.info("Preprocessing...")
    df = add_extra_data(df)

    for c in _cat_cols:
        df.loc[df[c].isnull(), c] = "Null"
        df[c] = df[c].astype(str)
        if c == "age":
            df[c] = pd.Categorical(df[c], ["Null", "Menos de 1 año", "1 a 5 años", "5 a 10 años", "10 a 20 años",
                                           "20 a 30 años", "30 a 50 años", "50 a 70 años", "70 a 100 años",
                                           "+ 100 años"])
        elif c == "floor":
            df[c] = pd.Categorical(df[c], ["Null", "Sótano", "Otro", "Bajos", "Entresuelo", "1ª planta", "2ª planta",
                                           "3ª planta", "4ª planta", "5ª planta", "6ª planta", "7ª planta", "8ª planta",
                                           "9ª planta", "10ª planta", "11ª planta", "12ª planta", "13ª planta",
                                           "14ª planta", "A partir de la 15ª planta"])
        elif c == "orientation":
            df[c] = pd.Categorical(df[c], ["Null", "Este", "Noreste", "Norte", "Noroeste", "Oeste", "Suroeste", "Sur",
                                           "Sureste"])
        else:
            df[c] = pd.Categorical(df[c])

    df = df[_all_cols]

    return df


def impute_data(df):
    logging.info("Imputing missing data...")
    df_mice = df.copy().reset_index(drop=True)

    kds = mf.ImputationKernel(df_mice, random_state=_random_state)
    kds.mice(3)
    df_imputed = kds.complete_data()

    return df_imputed


def transform_categories(X, cat_cols):
    X = X.copy()
    for c in cat_cols:
        if c in X.columns:
            _df = X[[c]].copy()
            _df["count"] = 1
            _df = _df.groupby(c, observed=True).count().reset_index()[[c, "count"]]
            _df["count"] = _df["count"] / _df["count"].sum()
            infrequent_cats = _df.loc[_df["count"] < 0.01, c].unique()
            X[c] = X[c].astype(object).replace(infrequent_cats, "Other")

    return X


def get_pipeline_preprocessor(cat_cols=None):
    if cat_cols is None:
        cat_cols = _cat_cols

    cats_transformer = FunctionTransformer(transform_categories, kw_args={"cat_cols": cat_cols})
    std_scaler = StandardScaler()
    log_transform_transformer = FunctionTransformer(np.log)

    transforms_no_onehot = [
        ("cat_transforms", Pipeline([
            ("cat_transforms", cats_transformer)
        ]), cat_cols),
        ("num_transforms", Pipeline([
            ("log_transform", log_transform_transformer),
            ("std_scaler", std_scaler)
        ]), _transform_columns)
    ]
    preprocessor = ColumnTransformer(transformers=transforms_no_onehot, remainder="drop")

    return preprocessor


def train_model(df, cat_cols=None, tune_model=False, include_ci=None):
    logging.info("Training model...")

    if cat_cols is None:
        cat_cols = _cat_cols
    X = df[cat_cols + _transform_columns]
    y = df["price"].values

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=_random_state)

    col_selector = ColumnSelector(remove_columns=_remove_cols)
    preprocessor_no_onehot = get_pipeline_preprocessor(cat_cols=cat_cols)

    if tune_model:
        params = tune_model_bayes(df, cat_cols=cat_cols)
        best_model = HistGradientBoostingRegressor(random_state=_random_state, categorical_features="from_dtype",
                                                   **params)
    else:
        best_model = HistGradientBoostingRegressor(random_state=_random_state, categorical_features="from_dtype")
    output_model = OutputTransformer(model=best_model, columns=cat_cols + _transform_columns,
                                     cat_columns=cat_cols,
                                     output_transform=np.exp, target_transform=np.log)

    pipe_full = Pipeline(steps=[
        ("selector", col_selector),
        ("preprocessor", preprocessor_no_onehot),
        ("model", output_model)
    ])
    pipe_full.fit_transform(X_train, y_train)

    # models for confidence intervals
    if include_ci:
        gb_lower = HistGradientBoostingRegressor(random_state=_random_state, categorical_features="from_dtype",
                                                 loss="quantile", quantile=0.1)
        gb_lower = OutputTransformer(model=gb_lower, columns=cat_cols + _transform_columns,
                                     cat_columns=cat_cols, output_transform=np.exp, target_transform=np.log)
        gb_upper = HistGradientBoostingRegressor(random_state=_random_state, categorical_features="from_dtype",
                                                 loss="quantile", quantile=0.9)
        gb_upper = OutputTransformer(model=gb_upper, columns=cat_cols + _transform_columns,
                                     cat_columns=cat_cols, output_transform=np.exp, target_transform=np.log)

        pipe_gb_lower = Pipeline(steps=[
            ("selector", col_selector),
            ("preprocessor", preprocessor_no_onehot),
            ("model", gb_lower)
        ])
        pipe_gb_upper = Pipeline(steps=[
            ("selector", col_selector),
            ("preprocessor", preprocessor_no_onehot),
            ("model", gb_upper)
        ])
        pipe_gb_lower.fit_transform(X_train, y_train)
        pipe_gb_upper.fit_transform(X_train, y_train)

    else:
        pipe_gb_lower, pipe_gb_upper = (None, None)

    return pipe_full, pipe_gb_lower, pipe_gb_upper


def tune_model_bayes(df, cat_cols=None):
    logging.info("Tuning model...")
    if cat_cols is None:
        cat_cols = _cat_cols

    params = {
        'learning_rate': np.logspace(-4, 0, 8),
        'max_leaf_nodes': np.concatenate([np.arange(10, 100, 5, dtype=np.int16), [None]]),
        "min_samples_leaf": np.linspace(20, 100, 9, dtype=np.int16),
        "max_iter": np.arange(50, 1050, 50),
        "l2_regularization": np.logspace(-4, 0, 20),
        "max_features": np.arange(0.5, 1, 0.05),
        "max_depth": np.concatenate([np.linspace(1, 50, 50, dtype=np.int16), [None]]),
    }

    X = df[cat_cols + _transform_columns]
    y = df["price"].values

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=_random_state)

    col_selector = ColumnSelector(remove_columns=_remove_cols)
    preprocessor_no_onehot = get_pipeline_preprocessor(cat_cols=cat_cols)

    pipe_full_bayes = Pipeline(steps=[
        ("selector", col_selector),
        ('preprocessor', preprocessor_no_onehot)
    ])
    X_train_scaled_hyper = pipe_full_bayes.fit_transform(X_train)
    X_train_scaled_hyper = pd.DataFrame(X_train_scaled_hyper, columns=cat_cols + _transform_columns)

    for c in cat_cols:
        if c in X_train_scaled_hyper.columns:
            X_train_scaled_hyper[c] = pd.Categorical(X_train_scaled_hyper[c])

    y_train_hyper = np.log(y_train)

    output_model_opt = HistGradientBoostingRegressor(random_state=_random_state, early_stopping=True,
                                                     categorical_features="from_dtype")
    bayes_search_model = BayesSearchCV(estimator=output_model_opt, search_spaces=params,
                                       random_state=_random_state)
    bayes_search_model.fit(X_train_scaled_hyper, y_train_hyper)

    return bayes_search_model.best_params_


def select_important_columns(trained_model, df):
    logging.info("Selecting important columns...")

    X = df[_cat_cols + _num_cols]
    y = df["price"].values
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=_random_state)

    result = permutation_importance(trained_model, X_train, y_train, n_repeats=10, random_state=_random_state)

    importance_df = pd.DataFrame({
        "feature": X_train.columns,
        "importance_mean": result.importances_mean,
        "importance_std": result.importances_std
    })

    importance_df = importance_df.sort_values(by="importance_mean", ascending=False)
    relevant_cols = importance_df.loc[importance_df["importance_mean"] > 0.01, "feature"].values.tolist()
    df_subset = df[relevant_cols + ["price"]]

    return df_subset, relevant_cols


def create_and_train_model():
    df = load_data()
    df = preprocess(df)
    df = impute_data(df)

    trained_model, _, _ = train_model(df)

    df_subset, relevant_cols = select_important_columns(trained_model, df)
    relevant_cat_cols = [c for c in relevant_cols if c in _cat_cols]

    trained_model, model_lower_bound, model_upper_bound = train_model(df_subset, cat_cols=relevant_cat_cols,
                                                                      tune_model=True, include_ci=True)

    with open("price_model_full.pkl", "wb") as f:
        pickle.dump(trained_model, f)
    with open("price_model_lower_bound.pkl", "wb") as f:
        pickle.dump(model_lower_bound, f)
    with open("price_model_upper_bound.pkl", "wb") as f:
        pickle.dump(model_upper_bound, f)


def predict_and_load():
    logging.info("Predicting values and storing in DB...")

    with open("price_model_full.pkl", "rb") as f:
        model = pickle.load(f)
    with open("price_model_lower_bound.pkl", "rb") as f:
        model_lower_bound = pickle.load(f)
    with open("price_model_upper_bound.pkl", "rb") as f:
        model_upper_bound = pickle.load(f)

    df = load_data()
    indexes = df["id"]
    df = preprocess(df)
    df["prediction"] = model.predict(df)
    df["prediction_lower_bound"] = model_lower_bound.predict(df)
    df["prediction_upper_bound"] = model_upper_bound.predict(df)
    df["id"] = indexes

    df_load = df[["id", "prediction", "prediction_lower_bound", "prediction_upper_bound"]]
    upload_to_table(df_load, "houses_predictions", replace_existing_values=True)


if __name__ == "__main__":
    create_and_train_model()
    predict_and_load()
