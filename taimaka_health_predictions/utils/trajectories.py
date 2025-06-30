"""
Author: Hunter Merrill

Description:
This module defines the `AnthropometricTrajectories` class, which is used to fit and
forecast anthropometric trajectories using b-spline basis functions and mixed linear
models. The base model is adapted from
https://journals.plos.org/globalpublichealth/article?id=10.1371/journal.pgph.0003741.
I've also used Bayesian inference to forecast patient-specific trajectories and
categorical outcomes.
"""

from typing import Dict, List

import numpy as np
import pandas as pd
from scipy.interpolate import BSpline
from scipy.stats import norm
from statsmodels.regression.mixed_linear_model import MixedLM


class AnthropometricTrajectories:
    """
    Class to train and forecast anthropometric trajectories.
    """

    def __init__(
        self, min_days: int = 0, max_days: int = 90, num_knots: int = 10, degree: int = 3
    ) -> None:
        """
        Parameters
        ----------
        min_days: int
            Lower end of the b-spline basis. Extrapolation will occur below this value.
        max_days: int
            Upper end of the b-spline basis. Extrapolation will occur above this value.
        num_knots: int
            Number of knots for the b-spline.
        degree: int
            Degree of the b-spline.
        """
        self.min_days = min_days
        self.max_days = max_days
        self.num_knots = num_knots
        self.degree = degree
        self.knots = np.linspace(self.min_days, self.max_days, self.num_knots)

    def create_design_matrix(
        self,
        days: np.ndarray | pd.Series,
        cat_vector: np.ndarray | pd.Series | None,
        cat_labels: List[str] | None = None,
    ) -> np.ndarray:
        """
        Create the design matrix for the b-spline basis.

        Parameters
        ----------
        days: np.ndarray | pd.Series | None
            Array of days for which to create the design matrix.
        cat_vector: np.ndarray | pd.Series | None
            Optional array of categories for which independent smoothing splines will be created.
        cat_labels: List[str] | None
            Optional list of category labels. If provided, the first category will be used as the baseline.

        Returns
        -------
        np.ndarray
            Design matrix for the b-spline basis.
        """
        if isinstance(days, pd.Series):
            days = days.values
        design_matrix = BSpline.design_matrix(
            x=days, t=self.knots, k=self.degree, extrapolate=True
        ).toarray()

        # make a copy, which we may use to add category-specific terms
        full_design_matrix = design_matrix.copy()

        if cat_vector is not None:
            if isinstance(cat_vector, pd.Series):
                cat_vector = cat_vector.values
            if cat_labels is None:
                cat_labels = np.unique(cat_vector)
            for cat in cat_labels[1:]:  # Skip the first (baseline) category
                cat_mask = cat_vector == cat
                full_design_matrix = np.concatenate(
                    [full_design_matrix, design_matrix * cat_mask[:, np.newaxis]], axis=1
                )
        return full_design_matrix

    def fit(
        self,
        df: pd.DataFrame,
        metric_col: str,
        days_col: str = "days_in_program",
        group_col: str = "pid",
        cat_col: str | None = None,
    ) -> None:
        """
        Fit the b-spline model to the data.

        Parameters
        ----------
        df: pd.DataFrame
            DataFrame containing the data.
        metric_col: str
            Column name for the metric to be modeled (e.g., "muac_weekly").
        days_col: str
            Column name for the days in program (e.g., "days_in_program").
        group_col: str
            Column name for grouping (e.g., patient ID).
        cat_col: str | None
            Optional column name for categorical variable (e.g., "status").
        """
        self.metric_col = metric_col
        self.days_col = days_col
        self.group_col = group_col
        self.cat_col = cat_col
        self.cat_labels = None

        y = df[metric_col].values

        # get unique categories if provided (and if there's more than one)
        if cat_col is not None:
            self.cat_labels = np.unique(df[cat_col].values)
            if len(self.cat_labels) == 1:
                cat_col = None
                self.cat_labels = None

        X = self.create_design_matrix(
            df[days_col].values,
            df[cat_col].values if cat_col else None,
            cat_labels=self.cat_labels,
        )
        model = MixedLM(
            endog=y,
            exog=X,
            groups=df[group_col].values,
        )
        self.fitted_model = model.fit()

        # get the parameters and their covariance, as well as the group and residual variances.
        # (Group variance is the variance of the subject-specific intercepts.)
        self.params = {
            "beta": np.array(self.fitted_model.params[:-1]),
            "cov_beta": self.fitted_model.cov_params()[:-1, :-1],
            "group_variance": self.fitted_model.params[-1],
            "residual_variance": self.fitted_model.scale,
        }

    def forecast(self, df: pd.DataFrame, priors: Dict[str, float] | None = None) -> pd.DataFrame:
        """
        Forecast the metric for a single patient using the fitted model.

        Parameters
        ----------
        df: pd.DataFrame
            DataFrame containing the data to forecast.
        priors: Dict[str, float] | None
            Optional dictionary of priors for the forecasted category. If provided, it should
            contain keys for each category and their corresponding prior values.

        Returns
        -------
        pd.DataFrame
            DataFrame with the forecasted values and standard deviation.
        """
        df = df.copy()
        if not hasattr(self, "fitted_model"):
            raise ValueError("Model must be fitted before forecasting.")

        if df[self.group_col].nunique() > 1:
            raise ValueError("Forecasting is only supported for a single patient.")

        days = df[self.days_col].values

        # if the category column is not provided, we will forecast over it.
        if self.cat_col is not None and self.cat_col not in df.columns:
            if priors is None:
                priors = {cat: 1.0 / len(self.cat_labels) for cat in self.cat_labels}

            # get likelihood of each class
            likelihoods = {}
            for cat in self.cat_labels:
                x = self.create_design_matrix(
                    days, cat_vector=np.repeat(cat, days.shape[0]), cat_labels=self.cat_labels
                )
                mean = (x @ self.params["beta"]).squeeze()
                lk = np.exp(
                    norm(loc=mean, scale=np.sqrt(self.params["residual_variance"]))
                    .logpdf(df[self.metric_col].values)
                    .sum()
                )
                likelihoods[cat] = lk

            # get the posterior probabilities of each class
            denominator = sum([priors[cat] * likelihoods[cat] for cat in priors.keys()])
            self.posteriors = {}
            for cat in priors.keys():
                self.posteriors[cat] = priors[cat] * likelihoods[cat] / denominator

            # assume the most likely category for the forecast.
            assumed_category = max(self.posteriors, key=self.posteriors.get)
            df.loc[:, self.cat_col] = assumed_category
        else:
            # If the category column is provided, we will use it directly.
            assumed_category = df[self.cat_col].values[0] if self.cat_col else None

        # Now we can create the design matrix for the forecasted or observed category.
        X_forecast = self.create_design_matrix(
            days, df[self.cat_col].values if self.cat_col else None, cat_labels=self.cat_labels
        )

        # Get the forecasted mean and standard deviation.
        forecast_mean = (X_forecast @ self.params["beta"]).squeeze()

        # The patient-specific intercept is unknown. Let's estimate it with Bayesian inference.
        z = df[self.metric_col].values - forecast_mean
        u = (
            self.params["group_variance"]
            / ((self.params["residual_variance"] / len(z)) + self.params["group_variance"])
            * z.mean()
        )
        v = 1 / ((len(z) / self.params["residual_variance"]) + (1 / self.params["group_variance"]))

        # Now we can make a patient-specific forecast over a grid of days.
        forecast_day_grid = np.linspace(self.min_days, self.max_days, 100)
        X_grid = self.create_design_matrix(
            forecast_day_grid,
            np.repeat(assumed_category, forecast_day_grid.shape[0]) if self.cat_col else None,
            cat_labels=self.cat_labels,
        )
        forecast = u + (X_grid @ self.params["beta"]).squeeze()
        forecast_std = np.sqrt(
            v
            + self.params["residual_variance"]
            * (1 + np.diag(X_grid @ self.params["cov_beta"] @ X_grid.T))
        )
        result = {
            "days": forecast_day_grid,
            "forecast": forecast,
            "std": forecast_std,
            "category": assumed_category,
        }
        if hasattr(self, "posteriors"):
            result["posterior_probability"] = self.posteriors[assumed_category]
        return pd.DataFrame(result)
