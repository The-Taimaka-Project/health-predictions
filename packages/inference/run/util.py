"""This script contains utility functions for data processing and inference."""


# TODO:
# 1. add docstrings and type-hinting to all functions
# 2. move all imports to the top of the file

class EtlReaderWriter:
  """Class for reading/writing ETL data to/from DigitalOcean Spaces/Postgres or Google Drive."""
  def __init__(self):
    from google.colab import drive
    drive.mount("/content/drive")
    import pandas as pd
    self.pd = pd


  def read_data(self):
    dir = "/content/drive/My Drive/[PBA] Full datasets/"
    current = pd.read_csv(dir + "FULL_pba_current_processed_2024-11-15.csv")
    admit = pd.read_csv(dir + "FULL_pba_admit_processed_2024-11-15.csv")
    weekly = pd.read_csv(dir + "FULL_pba_weekly_processed_2024-11-15.csv")
    raw = pd.read_csv(dir + "FULL_pba_admit_raw_2024-11-15.csv")
    weekly_raw = pd.read_csv(dir + "FULL_pba_weekly_raw_2024-11-15.csv")
    itp = pd.read_csv(dir + "FULL_pba_itp_roster_2024-11-15.csv")
    relapse = pd.read_csv(dir + "FULL_pba_relapse_raw2024-11-15.csv")
    mh = pd.read_csv(dir + "FULL_pba_mh_raw2024-11-15.csv")
    return current,admit,weekly,raw,weekly_raw,itp,relapse,mh

def make_populated_column(detn, variable):
    detn[f"{variable}_populated"] = detn[variable].notnull().astype(int)
    return detn, f"{variable}_populated"


# Find columns in 'df' with nunique between 3 and 10 and aren't boolean
def make_categorical(df):
    cols_to_check = df.select_dtypes(exclude=["bool", "number"])
    result_cols = [col for col in cols_to_check.columns if 2 <= df[col].nunique() <= 10]
    for col in result_cols:
        df[col] = df[col].astype("category")
    return df


def remove_anthros(detn):
    remove_anthros_keep_wk1_muac(detn, False)


def remove_anthros_keep_wk1_muac(detn, keep_wk1_muac=False):
    detn.drop(columns=[col for col in detn.columns if "wfh" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "calc_los" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "wfa" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "hl" in col], inplace=True)
    detn.drop(
        columns=[col for col in detn.columns if "weight" in col and col != "detn_weight_loss_ever"],
        inplace=True,
    )
    detn.drop(columns=[col for col in detn.columns if "hfa" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "date" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "time" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "day" in col], inplace=True)
    detn.drop(
        columns=[
            col for col in detn.columns if "week" in col and col != "muac_loss_2_weeks_consecutive"
        ],
        inplace=True,
    )
    detn.drop(columns=[col for col in detn.columns if "give" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "visitnum" in col], inplace=True)
    # detn.drop(columns=[col for col in detn.columns if 'h1' in col],inplace=True)
    detn.drop(columns=[col for col in detn.columns if "vd" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "age" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "site" in col], inplace=True)
    # detn.drop(columns=[col for col in detn.columns if 'h1' in col],inplace=True)
    detn.drop(columns=[col for col in detn.columns if "maln" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "contprogram" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "cornulc" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "excluded" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "mamneedsitp" in col], inplace=True)

    detn.drop(columns=[col for col in detn.columns if "possexclucrit" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "twinpid" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "b_wast" in col], inplace=True)
    if keep_wk1_muac:
        detn.drop(
            columns=[
                col
                for col in detn.columns
                if "muac" in col and col != "muac_loss_2_weeks_consecutive" and col != "wk1_muac"
            ],
            inplace=True,
        )
    else:
        detn.drop(
            columns=[
                col
                for col in detn.columns
                if "muac" in col and col != "muac_loss_2_weeks_consecutive"
            ],
            inplace=True,
        )
    detn.drop(columns=[col for col in detn.columns if "random" in col], inplace=True)

    # prompt: find all single-valued columns in detn
    # Find single-valued columns in detn
    single_valued_cols = [col for col in detn.columns if detn[col].nunique() <= 1]
    detn.drop(columns=single_valued_cols, inplace=True)
    detn.drop(columns=[col for col in detn.columns if "doneses" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "pidscannable" in col], inplace=True)

    detn.drop(columns=[col for col in detn.columns if "attachments" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "rutf" in col], inplace=True)

    detn.drop(columns=[col for col in detn.columns if "end_time" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "endtime" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "submissiondate" in col], inplace=True)

    detn.drop(columns=[col for col in detn.columns if "name" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "pp_cm" in col], inplace=True)

    detn.drop(columns=[col for col in detn.columns if "starttime" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "submission_date" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "start_time" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "last_admit" in col], inplace=True)

    detn.drop(columns=[col for col in detn.columns if "c_assigned_cm" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "photo" in col], inplace=True)

    detn.drop(columns=[col for col in detn.columns if "picture" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "drug_record" in col], inplace=True)

    detn.drop(columns=[col for col in detn.columns if "first_admit" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "site_admit" in col], inplace=True)

    detn.drop(columns=[col for col in detn.columns if "sequence_num" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "form_version" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "dose" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "submitterid" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "dischqualanthro" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "pull_lastms" in col], inplace=True)

    detn.drop(columns=[col for col in detn.columns if "row_count" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "submitter_id" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "wasreferred" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "pull_lastms" in col], inplace=True)

    detn.drop(columns=[col for col in detn.columns if "eff_ref" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "pull_lastms" in col], inplace=True)

    detn.drop(columns=[col for col in detn.columns if "device" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "itpotp" in col], inplace=True)


def infer_phq_score(admit_current_mh, admit_current, detn):
    import pandas as pd
    import statsmodels.api as sm

    df = admit_current_mh[["age_takewater", "rainy_season", "phq_score"]].copy()

    df.dropna(inplace=True)

    # Split the data into independent variables (X) and the dependent variable (y)
    X = df[["age_takewater", "rainy_season"]]
    y = df["phq_score"]

    # Add a constant to the independent variables for the intercept term
    X = sm.add_constant(X)

    # Fit the linear regression model
    model = sm.OLS(y, X).fit()

    admit_current["phq_predicted"] = model.predict(
        sm.add_constant(admit_current[["age_takewater", "rainy_season"]])
    )

    admit_current = pd.merge(
        admit_current, admit_current_mh[["pid", "phq_score"]], on="pid", how="left"
    )

    admit_current["phq_score"].fillna(admit_current["phq_predicted"], inplace=True)
    admit_current["phq_score"].fillna(admit_current_mh["phq_score"].mean(), inplace=True)
    detn = pd.merge(detn, admit_current[["pid", "phq_score"]], on="pid", how="left")
    return detn


def find_collinear_columns(df, threshold=0.99, col_ct_threshold=100):
    """
    This function is designed to identify columns within a Pandas DataFrame (df) that are highly correlated with each other, meaning they carry very similar information. This is often referred to as collinearity.

      Args:
        df: The Pandas DataFrame to analyze.
        threshold: The minimum correlation value between two columns to be considered collinear (defaults to 0.99).
        col_ct_threshold: The minimum number of non-null values a column must have to be included in the analysis (defaults to 100).

      Returns:
          just prints
    """
    # Set a threshold for collinearity (e.g., 0.9)

    numeric_cols = df.select_dtypes(include=["number"]).columns

    # Find columns in weekly_joined with more than 20000 count
    column_counts = df[numeric_cols].count()

    # prompt: use corr() to find columns in weekly_joined that are collinear

    # Find highly correlated columns in weekly_raw
    correlation_matrix = df[column_counts[column_counts > col_ct_threshold].index.tolist()].corr()

    # Find columns where the absolute correlation is above the threshold
    collinear_columns = (
        correlation_matrix[(correlation_matrix > threshold) & (correlation_matrix != 1.0)]
        .stack()
        .index.tolist()
    )

    # Print the collinear columns
    print(f"Collinear columns in passed dataframe:")
    for col1, col2 in collinear_columns:
        print(f"- {col1} and {col2} (Correlation: {correlation_matrix.loc[col1, col2]:.2f})")


def explain_gbm_model(gbm5, detn5, idx, iloc, gbm_features, label):
    import matplotlib.pyplot as plt
    import shap

    explainer_gbm2 = shap.TreeExplainer(gbm5)
    shap_values_gbm2 = explainer_gbm2.shap_values(detn5[gbm_features])

    values_to_display = detn5.loc[idx][["pid", label]]
    plt.text(0.3, 0.1, f"{values_to_display}", transform=plt.gca().transAxes)

    exp2 = shap.Explanation(
        shap_values_gbm2[iloc],
        explainer_gbm2.expected_value,
        data=detn5[gbm_features].loc[idx].values,
        feature_names=gbm_features,
    )

    log_odds = exp2.values.sum() + exp2.base_values
    probability = log_odds_to_probability(log_odds)
    plt.text(0.3, 0.5, "LightGBM model", transform=plt.gca().transAxes)
    plt.text(
        0.3,
        0.3,
        f"The probability of {label}\ncorresponding to log odds {log_odds:.3f} is: {probability:.2%}",
        transform=plt.gca().transAxes,
    )

    shap.plots.waterfall(exp2)  # Pass the Explanation object to waterfall plot


def split_detn_new_onset_medical_complication(detn, label):
    import pandas as pd

    # split detn into 4 very different subsets:
    # Use .copy() to ensure detn_admit_only is a copy, not a view
    detn_admit_only = detn[
        (detn["wk1_calcdate_weekly"].isnull())
        & (detn["wk2_calcdate_weekly"].isnull())
        & (detn["wk3_calcdate_weekly"].isnull())
    ].copy()
    cat1_cols = [col for col in detn.columns if col.startswith("y_")]
    # Use .loc to avoid chained indexing and to fill na values
    detn_admit_only.loc[:, cat1_cols] = detn_admit_only.loc[:, cat1_cols].fillna(0)

    # prompt: rows where  detn[detn['wk2_calcdate_weekly'].isnull()] and pid not in detn_admit_only['pid']
    # Use .copy() to ensure detn_wk1_only is a copy, not a view
    detn_wk1_only = detn[
        (detn["wk1_calcdate_weekly"].notnull())
        & (detn["wk2_calcdate_weekly"].isnull())
        & (detn["wk3_calcdate_weekly"].isnull())
    ].copy()

    # prompt: rows where  detn[detn['wk3_calcdate_weekly'].isnull()] and pid not in detn_admit_only['pid'] and pid not in detn_wk1_only
    # Use .copy() to ensure detn_wk2 is a copy, not a view
    detn_wk2 = detn[
        (detn["wk1_calcdate_weekly"].notnull())
        & (detn["wk2_calcdate_weekly"].notnull())
        & (detn["wk3_calcdate_weekly"].isnull())
    ].copy()

    # prompt: rows where  detn[detn['wk3_calcdate_weekly'].isnull()] and pid not in detn_admit_only['pid'] and pid not in detn_wk1_only
    # Use .copy() to ensure detn_wk3 is a copy, not a view
    detn_wk3 = detn[
        (detn["wk3_calcdate_weekly"].notnull())
        & (detn["wk2_calcdate_weekly"].notnull())
        & (detn["wk1_calcdate_weekly"].notnull())
    ].copy()

    # prompt: drop all columns in detn_admit_only,detn_wk2,detn_wk3 that are for subsequent weeks, even if they're null,
    # just to ensure that no future leaks into present

    wk1_cols = [col for col in detn.columns if col.startswith("wk1")]
    wk2_cols = [col for col in detn.columns if col.startswith("wk2")]
    wk3_cols = [col for col in detn.columns if col.startswith("wk3")]

    # Drop columns in detn_admit_only that start with wk1 or wk2 or wk3
    detn_admit_only.drop(columns=wk1_cols, inplace=True)
    detn_admit_only.drop(columns=wk2_cols, inplace=True)
    detn_admit_only.drop(columns=wk3_cols, inplace=True)
    # b_needsitp = 'indicates whether child was referred to an ITP', may be caused by complication
    detn_admit_only.drop(columns="b_needsitp", inplace=True)
    # early referrals may be caused by complication
    detn_admit_only.drop(
        columns=[col for col in detn.columns if col.startswith("ref_")], inplace=True
    )

    # Filter detn_admit_only where the label is 0
    detn_admit_only_label_0 = detn_admit_only[detn_admit_only[label] == 0].copy()

    # Get the columns where all values are null for the filtered data
    null_columns = detn_admit_only_label_0.columns[detn_admit_only_label_0.isnull().all()].to_list()
    # remove them, otherwise the model will just key on them to find label == 1
    cols_to_drop = list(
        set(null_columns) - set(cat1_cols)
    )  # keep y columns as they won't be used to predict
    detn_admit_only.drop(columns=cols_to_drop, inplace=True)

    detn_wk1_only.drop(columns=wk2_cols, inplace=True)
    detn_wk1_only.drop(columns=wk3_cols, inplace=True)
    # rsquared meaningless for single row patients
    detn_wk1_only.drop(
        columns=[col for col in detn_wk1_only.columns if col.endswith("rsquared")], inplace=True
    )
    detn_wk1_only.drop(
        columns=[col for col in detn_wk1_only.columns if col.endswith("_trend")], inplace=True
    )

    detn_wk2.drop(columns=wk3_cols, inplace=True)
    # rsquared is always 1 for the 2 row patients as they have complication on the third anthro row (2nd visit)
    detn_wk2.drop(
        columns=[col for col in detn_wk2.columns if col.endswith("rsquared")], inplace=True
    )

    print(detn_admit_only.shape, detn_wk1_only.shape, detn_wk2.shape, detn_wk3.shape)
    # detn_admit_only = make_dummy_columns(detn_admit_only)
    # detn_wk1_only = make_dummy_columns(detn_wk1_only)
    # detn_wk2 = make_dummy_columns(detn_wk2)
    # detn_wk3 = make_dummy_columns(detn_wk3)
    return detn_admit_only, detn_wk1_only, detn_wk2, detn_wk3


def make_test(detn, ag_features, label):
    from sklearn.model_selection import train_test_split

    X = make_dummy_columns(detn[ag_features])

    # X = detn.drop(columns=label)
    y = detn[label]

    # Perform train-test split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=43)  #
    # prompt: columns in X_train that are datetime64[ns]

    datetime_columns = X_train.select_dtypes(include="datetime64[ns]").columns
    for col in datetime_columns:
        X_train, _ = days_since_min(X_train, col)
        X_train.drop(columns=col, inplace=True)
        X_test, _ = days_since_min(X_test, col)
        X_test.drop(columns=col, inplace=True)

    X_train, X_test = impute_missing_values(X_train, X_test)
    print(X_train.shape, X_test.shape, y_train.shape, y_test.shape)
    return X_train, X_test, y_train, y_test


def label_test(X_test_ag, y_test_ag, predictor, label, y_pred_proba, detn):
    # y_pred_proba = predictor.predict_proba(X_test)
    import pandas as pd

    # Join X_test and y_test
    df = X_test_ag.join(y_test_ag)

    # prompt: join df to y_pred renaming column to predicted
    y_pred = predictor.predict(X_test_ag)
    # Rename the y_pred Series to 'predicted'
    y_pred = pd.Series(y_pred, name=f"predicted_{label}")

    # Concatenate the DataFrames
    df = pd.concat([df, y_pred], axis=1)

    pred_proba_series = y_pred_proba[1].rename(f"probability_{label}")
    df = df.join(pred_proba_series)

    test_labelled = df.join(detn[["pid"]])
    return test_labelled


def label_test_lin(X_test, y_test, logreg_model, label, detn):
    import pandas as pd

    y_pred_proba = logreg_model.predict_proba(X_test)
    y_pred = logreg_model.predict(X_test)
    # Rename the y_pred Series to 'predicted'
    y_pred = pd.Series(y_pred, name=f"predicted_{label}")

    # Join X_test and y_test
    df = X_test.join(y_test)

    # prompt: join df to y_pred renaming column to predicted
    # Reset index of both DataFrames before concatenation
    df = df.reset_index(drop=False)
    y_pred = y_pred.reset_index(drop=True)

    # Concatenate the DataFrames
    df = pd.concat([df, y_pred], axis=1)

    # prompt: get the second column in y_pred_proba
    df = df.join(pd.Series(y_pred_proba[:, 1], name=f"probability_{label}"))

    df = df.set_index("index")
    test_labelled_lin = df.join(detn[["pid"]])
    return test_labelled_lin


def linear_regress_general(detn_mh, regressor_cols, label, OLS=True):
    import statsmodels.api as sm

    # Prepare the data
    df = detn_mh[regressor_cols + [label]]
    print(df.shape)
    df = df.dropna()
    print(df.shape)
    X = df[regressor_cols]
    y = df[label]

    # Add a constant to the independent variable for the intercept
    X = sm.add_constant(X)

    # Fit the OLS model
    if OLS:
        model = sm.OLS(y, X).fit()
    else:
        model = sm.Logit(y, X).fit()

    # Print the model summary
    print(model.summary())
    return model


def linear_regress_ols(detn_mh, regressor_col, label):
    return linear_regress_general(detn_mh, regressor_col, label, OLS=True)


def reduce_dimensionality(detn, columns_for_reduction, reduced_column_name):
    import pandas as pd
    from sklearn.decomposition import PCA
    from sklearn.preprocessing import StandardScaler

    # Select the columns for dimensionality reduction
    df_nonnull = detn[columns_for_reduction].dropna()
    # Create a StandardScaler object
    scaler = StandardScaler()
    scaler.fit(df_nonnull)

    # Transform the data
    scaled_data = scaler.transform(df_nonnull)

    # Create a PCA object with the desired number of components
    pca = PCA(n_components=1)  # Reduce to 1 component

    # Fit the PCA model on the selected columns
    pca.fit(scaled_data)
    print(pca.explained_variance_ratio_)
    # Transform the data to reduce its dimensionality
    reduced_data = pca.transform(scaled_data)
    reduced_data.columns = [reduced_column_name]
    # Concatenate the reduced data with the original DataFrame
    detn = pd.concat([detn, reduced_data], axis=1)
    # detn.drop(columns=columns_for_reduction,inplace=True)
    return detn


def merge_probabilities(
    pid_probabilities, label, X_train_transformed, X_test_transformed, gbm, detn
):
    import pandas as pd

    gbm_probability_label = f"gbm_probability_{label}"
    y_pred_proba = gbm.predict_proba(X_train_transformed)
    df_train = X_train_transformed.join(detn["pid"])
    df_train = df_train.reset_index(drop=False)
    df_train = df_train.join(pd.Series(y_pred_proba[:, 1], name=gbm_probability_label))
    df_train = df_train.set_index("index")

    y_pred_proba = gbm.predict_proba(X_test_transformed)
    df_test = X_test_transformed.join(detn["pid"])
    df_test = df_test.reset_index(drop=False)
    df_test = df_test.join(pd.Series(y_pred_proba[:, 1], name=gbm_probability_label))
    df_test = df_test.set_index("index")
    # df[df['pid']== '23-2685'].T
    df = pd.concat([df_train, df_test], axis=0)
    pid_probabilities = pd.merge(
        pid_probabilities, df[["pid", gbm_probability_label]], on="pid", how="left"
    )
    return pid_probabilities


def label_test_tree(X_test, y_test, tree_model, label, detn):
    import numpy as np
    import pandas as pd

    y_pred_proba = tree_model.predict_proba(X_test)
    y_pred = tree_model.predict(X_test)
    # Rename the y_pred Series to 'predicted'
    y_pred = pd.Series(y_pred, name=f"predicted_{label}")
    tree_features = X_test.columns[np.array(tree_model.feature_importances_, dtype=bool)].to_list()
    # Join X_test and y_test
    df = X_test[tree_features].join(y_test)

    # prompt: join df to y_pred renaming column to predicted
    # Reset index of both DataFrames before concatenation
    df = df.reset_index(drop=False)
    y_pred = y_pred.reset_index(drop=True)

    # Concatenate the DataFrames
    df = pd.concat([df, y_pred], axis=1)

    # prompt: get the second column in y_pred_proba
    df = df.join(pd.Series(y_pred_proba[:, 1], name=f"probability_{label}"))

    df = df.set_index("index")
    test_labelled_tree = df.join(detn[["pid"]])
    return test_labelled_tree


def days_since_min(detn, variable):
    # Calculate the number of days since the minimum of 'wk1_nv_date_weekly'
    min_date = detn[variable].min()
    variable_days = f"days_since_min_{variable}"
    detn[variable_days] = (detn[variable] - min_date).dt.days
    return detn, variable_days


def regress(df, pid, variable):
    import numpy as np
    import statsmodels.formula.api as smf

    """regress variable against cumulative_days

  Args:
      df (pd.DataFrame): anthropomorphic data
      pid (str): patient id
      variable (str): variable to regress against cumulative_days

  Returns:
      trend: coefficient of variable against cumulative_days
      r_squared: r-squared value of the regression model
  """
    # Filter for the specific pid
    anthros_pid = df[df["pid"] == pid]
    if anthros_pid[variable].count() < 2:
        return None, None

    # Fit the linear regression model
    # print(anthros_pid.columns)
    # print(f'{variable} ~ cumulative_days')
    np.seterr(divide="ignore", invalid="ignore")
    model = smf.ols(f"{variable} ~ cumulative_days", data=anthros_pid).fit()

    # Get the R-squared value
    if model.rsquared < 0:
        r_squared = 0
    else:
        r_squared = model.rsquared

    # Get the coefficients
    coefficients = model.params

    return coefficients["cumulative_days"], r_squared


class AutogluonWrapper:
    def __init__(self, predictor, feature_names, target_class=None):
        self.ag_model = predictor
        self.feature_names = feature_names
        self.target_class = target_class
        if target_class is None and predictor.problem_type != "regression":
            print("Since target_class not specified, SHAP will explain predictions for each class")

    def predict_proba(self, X):
        import pandas as pd

        if isinstance(X, pd.Series):
            X = X.values.reshape(1, -1)
        if not isinstance(X, pd.DataFrame):
            X = pd.DataFrame(X, columns=self.feature_names)
        preds = self.ag_model.predict_proba(X)
        if self.ag_model.problem_type == "regression" or self.target_class is None:
            return preds
        else:
            return preds[self.target_class]


def export_model(source_path, destination_path):
    import shutil

    # Use shutil.copytree to copy the directory
    try:
        shutil.copytree(source_path, destination_path)
        print(f"Successfully copied '{source_path}' to '{destination_path}'")
    except FileExistsError:
        print(
            f"Destination directory '{destination_path}' already exists. Please remove or choose a different destination."
        )
    except FileNotFoundError:
        print(f"Source directory '{source_path}' not found.")
    except OSError as e:
        print(f"An error occurred: {e}")


def explain(idx, explainer, X_test):
    import shap

    shap_values_single = explainer.shap_values(X_test.loc[idx])  # Calculate SHAP values

    exp = shap.Explanation(
        shap_values_single,
        explainer.expected_value,
        data=X_test.loc[idx].values,
        feature_names=X_test.columns,
    )
    shap.plots.waterfall(exp)  # Pass the Explanation object to waterfall plot


def plot_survival3(pid, aft, regression_dataset, label, covariate1, covariate2, covariate3):
    import matplotlib.pyplot as plt

    pid_row = regression_dataset[regression_dataset["pid"] == pid]
    if pid_row.empty:
        print(f"No data found for PID: {pid}.  Is a covariate null?")
        return
    fig, ax = plt.subplots(nrows=1, ncols=2, figsize=(10, 4))
    survival_series = aft.predict_survival_function(
        pid_row, conditional_after=pid_row["duration_days"]
    )
    survival_series.columns = [f"{pid} {label} survival"]
    ax1 = survival_series.plot(ax=ax[0])
    ax[0].set_title(f"Survival function for {pid}")
    ax[0].set_xlabel("Days")
    ax[0].set_ylabel("Survival Probability")
    ax[0].set_label(pid)
    median_days = aft.predict_median(pid_row, conditional_after=pid_row["duration_days"]).iloc[0]
    ax[0].text(
        0.03,
        0.6,
        f"median time to\n{label}: {median_days.round(0):.0f} days",
        transform=ax[0].transAxes,
    )
    if covariate1 is not None:
        ax[0].text(
            0.03, 0.2, f"{covariate1}: {pid_row[covariate1].iloc[0]}", transform=ax[0].transAxes
        )
    if covariate2 is not None:
        ax[0].text(
            0.03, 0.3, f"{covariate2}: {pid_row[covariate2].iloc[0]}", transform=ax[0].transAxes
        )
    if covariate3 is not None:
        ax[0].text(
            0.03, 0.4, f"{covariate3}: {pid_row[covariate3].iloc[0]}", transform=ax[0].transAxes
        )
    hazard_series = aft.predict_hazard(pid_row, conditional_after=pid_row["duration_days"])
    hazard_series.columns = [f"{pid} {label} rate per day"]

    hazard_series.plot(ax=ax[1])
    ax[1].set_title(f"Hazard function for {pid}")
    ax[1].set_xlabel("Days")
    ax[1].set_ylabel(f"Chance of {label} occurring (per day)")
    ax[1].yaxis.set_label_position("right")
    ax[1].set_label(pid)
    fig.suptitle(f"Survival Curves for {pid} ({label})")

    plt.show()


# prompt: plot both survival_series and hazard_series using fig, ax = plt.subplots(nrows=1, ncols=2, figsize=(10, 4)) and put text on them
def plot_survival(pid, aft, regression_dataset, label, covariate1, covariate2):
    plot_survival3(pid, aft, regression_dataset, label, covariate1, covariate2, None)


# prompt: perform logistic regression
def logistic_regression(log_reg_top_features, detn, label, scale=True):
    import matplotlib.pyplot as plt
    import pandas as pd

    # Set global output format to Pandas
    from sklearn import set_config
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import classification_report, confusion_matrix
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import StandardScaler

    set_config(transform_output="pandas")

    detn_top_features = detn[log_reg_top_features].copy()
    detn_top_features.fillna(detn_top_features.mean(), inplace=True)
    # detn_top_features.dropna(inplace=True)
    print(f"--- Correlation of Features with {label}")
    print(detn_top_features[log_reg_top_features].corrwith(detn_top_features[label]))

    X = detn_top_features.drop(columns=label)
    y = detn_top_features[label]
    if scale:
        X = StandardScaler().fit_transform(X)

    # Perform train-test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=43
    )  # Adjust test_size and random_state as needed

    # Initialize and train the Logistic Regression model
    logreg_model = LogisticRegression(
        max_iter=1000, C=1, penalty="l1", solver="liblinear"
    )  # Change solver to liblinear or saga
    logreg_model.fit(X_train, y_train)

    # Make predictions on the test set
    y_pred = logreg_model.predict(X_test)

    # Evaluate the model (example: using a confusion matrix)
    cm = confusion_matrix(y_test, y_pred)
    print("\nConfusion Matrix:")
    print(cm)

    print("\nClassification Report:")
    print(classification_report(y_test, y_pred))

    # prompt: get feature importance of logreg_model
    coefs = logreg_model.coef_[0]
    feature_importances = pd.Series(abs(coefs), index=X_train.columns)
    feature_importances = feature_importances[feature_importances > 0.01]
    feature_importances.sort_values(ascending=True, inplace=True)
    print("\nCoefficients:")
    print(feature_importances)
    if scale:
        feature_importances.plot(kind="barh")
        plt.show()
    return logreg_model, X_train, X_test, y_train, y_test


def decision_tree(max_depth, X_train, y_train, X_test, y_test):
    import matplotlib.pyplot as plt
    from sklearn import tree
    from sklearn.metrics import (
        accuracy_score,
        balanced_accuracy_score,
        confusion_matrix,
        f1_score,
        precision_score,
        recall_score,
        roc_auc_score,
    )
    from sklearn.tree import DecisionTreeClassifier

    model = DecisionTreeClassifier(max_depth=max_depth)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    y_pred_proba = model.predict_proba(X_test)
    print("score", model.score(X_test, y_test))

    print("balanced acc", balanced_accuracy_score(y_test, y_pred))
    print("accuracy", accuracy_score(y_test, y_pred))
    print("precision", precision_score(y_test, y_pred))
    print("recall", recall_score(y_test, y_pred))
    print("f1", f1_score(y_test, y_pred))
    print("auc", roc_auc_score(y_test, y_pred))
    # Generate the confusion matrix
    cm = confusion_matrix(y_test, y_pred)

    print("Confusion Matrix:")
    print(cm)

    tree.plot_tree(model, proportion=True)
    plt.show()

    return model


def decision_tree_f1(max_depth, X_train, y_train, X_test, y_test):
    from sklearn.tree import DecisionTreeClassifier

    model = DecisionTreeClassifier(max_depth=max_depth)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    return model, y_pred


# prompt: get AIC value for model
# Assuming 'model' is your trained LGBMRegressor model and X_train_transformed, y_train are available
# Calculate AIC
def calculate_aic(model, X, y):
    import numpy as np

    y_pred = model.predict(X)
    n = len(y)
    k = len(model.feature_importances_)  # Number of features (parameters) in your model
    residuals = y - y_pred
    sse = np.sum(residuals**2)
    aic = 2 * k - 2 * np.log(sse)
    return aic


def lightgbm_regress(X_train_transformed, X_test_transformed, y_train, y_test):
    # prompt: train lightgbm model on X_train_transformed
    import lightgbm as lgb
    import pandas as pd
    from sklearn.metrics import mean_squared_error, r2_score

    gbm = lgb.LGBMRegressor(verbosity=-1)
    # Train the LightGBM model
    gbm.fit(X_train_transformed, y_train)
    # Make predictions on the test set
    y_pred = gbm.predict(X_test_transformed)
    feature_importances = gbm.feature_importances_
    # Get feature names
    feature_names = X_train_transformed.columns
    # Create a DataFrame for better visualization
    feature_importance_df = pd.DataFrame(
        {"Feature": feature_names, "Importance": feature_importances}
    )
    feature_importance_df = feature_importance_df.sort_values(by="Importance", ascending=False)
    # Filter features with importance greater than 0
    important_features = feature_importance_df[feature_importance_df["Importance"] > 0]
    top_features = important_features["Feature"].to_list()
    aic = calculate_aic(gbm, X_test_transformed, y_test)
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    # print(f"Mean Squared Error: {mse}, "R-squared: {r2}")

    return gbm, mse, aic, top_features


def select_features_regress(
    gbm,
    X_train_transformed_top,
    X_test_transformed_top,
    y_train,
    y_test,
    max_features,
    min_features,
    step,
):
    import numpy as np
    import pandas as pd

    results4 = []
    features = {}
    best_aic = 1000000
    for n in np.arange(max_features, min_features, step):
        top_n_features = get_top_features(gbm, X_train_transformed_top, features_to_select=n)
        X_train_transformed_top = X_train_transformed_top[top_n_features].copy()
        X_test_transformed_top = X_test_transformed_top[top_n_features].copy()
        # print(n,len(top_n_features),X_train_transformed_top.shape)
        gbm, mse, aic, top_features = lightgbm_regress(
            X_train_transformed_top, X_test_transformed_top, y_train, y_test
        )
        # print(len(gbm.feature_name_))
        data = {"mse": mse, "AIC": aic, "num_features": len(gbm.feature_name_)}
        features[n] = gbm.feature_names_in_
        results4.append(data)
        if aic < best_aic:
            best_aic = aic
            best_features = gbm.feature_names_in_
            best_gbm = gbm
    return best_gbm, best_features, pd.DataFrame(results4), best_aic, features


def get_best_tree_model(X_train, y_train, X_test, y_test, range=range(1, 3)):
    import numpy as np
    import pandas as pd
    from sklearn.metrics import f1_score
    from sklearn.tree import DecisionTreeClassifier

    best_model = None
    best_f1 = 0
    best_aic = float("inf")
    best_pred = None
    for max_depth in range:
        tree_model, y_pred = decision_tree_f1(max_depth, X_train, y_train, X_test, y_test)
        y_pred_prob = tree_model.predict_proba(X_test)[:, 1]  # Get probabilities for class 1 only

        log_likelihood = np.sum(
            y_test * np.log(y_pred_prob) + (1 - y_test) * np.log(1 - y_pred_prob)
        )

        # Calculate AIC
        # k =  Number of parameters (features)
        positive_features_count = sum(
            1 for importance in tree_model.feature_importances_ if importance > 0
        )
        k = positive_features_count  # Number of parameters (features)
        aic = 2 * k - 2 * log_likelihood
        f1_scored = f1_score(y_test, y_pred)
        print(f"max_depth: {max_depth}, f1: {f1_scored}, aic: {aic}")
        if aic < best_aic:
            best_aic = aic
            best_f1 = f1_scored
            best_model = tree_model
            best_pred = y_pred
    if best_model is None:
        print("no best model")
        best_model = tree_model
        best_pred = y_pred

    feature_importances = best_model.feature_importances_

    # Get feature names
    feature_names = X_train.columns

    # Create a DataFrame for better visualization
    importance_df = pd.DataFrame({"Feature": feature_names, "Importance": feature_importances})

    # Filter out features with zero importance
    important_features = importance_df[importance_df["Importance"] > 0]

    # Print or use the important features as needed
    important_features["Feature"].to_list()

    return best_model, best_f1, best_pred, best_aic, important_features["Feature"].to_list()


def print_tree(tree_model):
    import numpy as np

    n_nodes = tree_model.tree_.node_count
    children_left = tree_model.tree_.children_left
    children_right = tree_model.tree_.children_right
    feature = tree_model.tree_.feature
    threshold = tree_model.tree_.threshold
    values = tree_model.tree_.value
    columns = tree_model.feature_names_in_

    node_depth = np.zeros(shape=n_nodes, dtype=np.int64)
    is_leaves = np.zeros(shape=n_nodes, dtype=bool)
    stack = [(0, 0)]  # start with the root node id (0) and its depth (0)
    while len(stack) > 0:
        # `pop` ensures each node is only visited once
        node_id, depth = stack.pop()
        node_depth[node_id] = depth

        # If the left and right child of a node is not the same we have a split
        # node
        is_split_node = children_left[node_id] != children_right[node_id]
        # If a split node, append left and right children and depth to `stack`
        # so we can loop through them
        if is_split_node:
            stack.append((children_left[node_id], depth + 1))
            stack.append((children_right[node_id], depth + 1))
        else:
            is_leaves[node_id] = True

    print(
        "The binary tree structure has {n} nodes and has "
        "the following tree structure:\n".format(n=n_nodes)
    )
    for i in range(n_nodes):
        if is_leaves[i]:
            print(
                "{space}node={node} is a leaf node with value={value}.".format(
                    space=node_depth[i] * "\t", node=i, value=np.around(values[i], 3)
                )
            )
        else:
            print(
                "{space}node={node} is a split node with value={value}: "
                "go to node {left} if X[:, {feature}] <= {threshold} "
                "else to node {right}.".format(
                    space=node_depth[i] * "\t",
                    node=i,
                    left=children_left[i],
                    feature=columns[feature[i]],
                    threshold=threshold[i],
                    right=children_right[i],
                    value=np.around(values[i], 3),
                )
            )


def explain_tree_sample(clf, idx, X_test):
    node_indicator = clf.decision_path(X_test)
    leaf_id = clf.apply(X_test)
    n_nodes = clf.tree_.node_count
    feature = clf.tree_.feature
    threshold = clf.tree_.threshold
    values = clf.tree_.value
    columns = clf.feature_names_in_

    sample_id = X_test.index.get_loc(idx)
    # obtain ids of the nodes `sample_id` goes through, i.e., row `sample_id`
    node_index = node_indicator.indices[
        node_indicator.indptr[sample_id] : node_indicator.indptr[sample_id + 1]
    ]

    print(f"Decision tree rules used to predict sample loc {idx} row number {sample_id}:\n")
    for node_id in node_index:
        # continue to the next node if it is a leaf node
        if leaf_id[sample_id] == node_id:
            continue

        # check if value of the split feature for sample 0 is below threshold
        if X_test.iloc[sample_id, feature[node_id]] <= threshold[node_id]:
            threshold_sign = "<="
        else:
            threshold_sign = ">"

        print(
            "decision node {node} : (X_test[{sample}, {feature}] = {value}) "
            "{inequality} {threshold})".format(
                node=node_id,
                sample=sample_id,
                feature=columns[feature[node_id]],
                value=X_test.iloc[sample_id, feature[node_id]],
                inequality=threshold_sign,
                threshold=threshold[node_id],
            )
        )


def get_aic(gbm, X_test_transformed, y_test):
    import numpy as np

    # prompt: get AIC value for gbm
    # Assuming 'gbm' is your trained LightGBM model and 'X_test_transformed' and 'y_test' are defined.
    # Calculate negative log-likelihood (you may need to adjust based on your specific loss function)
    y_pred_prob = gbm.predict_proba(X_test_transformed)[:, 1]  # Get probabilities for class 1 only

    # y_pred_prob = gbm.predict(X_test_transformed, num_iteration=gbm.best_iteration)
    log_likelihood = np.sum(y_test * np.log(y_pred_prob) + (1 - y_test) * np.log(1 - y_pred_prob))

    # Calculate AIC
    # k = len(gbm.feature_name()) # Number of parameters (features)
    k = len(gbm.feature_name_)  # Number of parameters (features)
    aic = 2 * k - 2 * log_likelihood
    # print(f"AIC: {aic}")
    return aic


def logistic_train(X_train_transformed, X_test_transformed, y_train, y_test):
    # prompt: train lightgbm model on X_train_transformed
    import pandas as pd

    # Set global output format to Pandas
    from sklearn import set_config
    from sklearn.metrics import accuracy_score, f1_score

    set_config(transform_output="pandas")
    import numpy as np
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler

    model = LogisticRegression(max_iter=1000, C=1, penalty="l1", solver="liblinear")
    # scale so abs of coefs becomes importance
    X_train_transformed = StandardScaler().fit_transform(X_train_transformed)
    X_test_transformed = StandardScaler().fit_transform(X_test_transformed)

    model.fit(X_train_transformed, y_train)
    # Make predictions on the test set
    y_pred = model.predict(X_test_transformed)
    # Convert probabilities to class labels (e.g. 0 or 1)
    y_pred_class = [1 if prob > 0.5 else 0 for prob in y_pred]  # Adjust threshold as needed
    # Evaluate the model (example metrics)

    f1_scored = f1_score(y_test, y_pred_class)
    # print(f"f1: {f1_scored}")
    coefs = model.coef_[0]
    feature_importances = pd.Series(abs(coefs), index=X_train_transformed.columns)
    # feature_importances.sort_values(ascending=True,inplace=True)
    # important_feature_indices = feature_importances[feature_importances > 0]

    # Predict log probabilities for the model
    log_prob = model.predict_log_proba(X_test_transformed)
    # Calculate log-likelihood for the model
    log_likelihood = log_prob[np.arange(len(y_test)), y_test].sum()
    k = len(model.feature_names_in_) + 1  # Number of parameters in model
    aic = 2 * k - 2 * log_likelihood
    # print(f"AIC: {aic}")

    return model, f1_scored, aic, feature_importances


def select_logistic_features(
    model,
    X_train_transformed_imputed,
    important_feature_indices,
    X_test_transformed_imputed,
    y_train,
    y_test,
):
    """
    Selects the optimal set of features for a logistic regression model using AIC.

    Iterates through decreasing subsets of the most important features (based on initial logistic regression), trains a logistic regression model on each subset, and selects the subset with the lowest AIC.

    Args:
      model: The initial trained logistic regression model.
      X_train_transformed_imputed: The imputed and transformed training features.
      important_feature_indices: A pandas Series containing feature importance scores.
      X_test_transformed_imputed: The imputed and transformed test features.
      y_train: The training target variable.
      y_test: The test target variable.


    Returns:
      tuple: A tuple containing the best model, the best feature set, a DataFrame of results, the best AIC, and a dictionary of features used for each iteration.
    """
    import numpy as np
    import pandas as pd
    from sklearn.linear_model import LogisticRegression

    results_lin = []
    features_lin = {}
    best_aic_lin = 1000000
    # n = 100 or important_feature_indices.size, whichever is less
    for n in np.arange(min(40, important_feature_indices.size), 1, -1):
        top_n_features = important_feature_indices.nlargest(n).index.to_list()
        X_train_transformed_top = X_train_transformed_imputed[top_n_features].copy()
        X_test_transformed_top = X_test_transformed_imputed[top_n_features].copy()
        # print(n,len(top_n_features),X_train_transformed_top.shape)
        model, f1_scored, aic, important_feature_indices = logistic_train(
            X_train_transformed_top, X_test_transformed_top, y_train, y_test
        )
        # print(len(model.feature_names_in_))
        data = {
            "f1_score": [f1_scored],
            "AIC": [aic],
            "num_features": [len(model.feature_names_in_)],
        }
        features_lin[n] = model.feature_names_in_
        results_lin.append(data)
        if aic < best_aic_lin:
            best_aic_lin = aic
            best_features_lin = list(model.feature_names_in_)
            model = LogisticRegression(max_iter=1000, C=1, penalty="l1", solver="liblinear")
            model.fit(X_train_transformed_top[best_features_lin], y_train)
            best_model = model
            best_features_index = important_feature_indices
    return best_model, best_features_lin, pd.DataFrame(results_lin), best_aic_lin, features_lin


def impute_missing_values(X_train, X_test):
    import numpy as np

    # print(X_train.isnull().sum()[X_train.isnull().sum() > 0].sort_values(ascending=False) / X_train.shape[0])
    # print(X_train.isnull().sum().sum())
    X_train_imputed = X_train.copy()
    X_test_imputed = X_test.copy()
    # Replace infinite values with NaN
    X_train_imputed.replace([np.inf, -np.inf], np.nan, inplace=True)
    X_test_imputed.replace([np.inf, -np.inf], np.nan, inplace=True)

    # Impute NaN values with the mean
    X_train_imputed.fillna(X_train_imputed.mean(), inplace=True)
    X_test_imputed.fillna(X_test_imputed.mean(), inplace=True)
    # print(X_test_imputed.isnull().sum()[X_test_imputed.isnull().sum() > 0].sort_values(ascending=False) / X_test_imputed.shape[0])
    X_test_not_null_cols = X_test_imputed.columns[X_test_imputed.notnull().any()].tolist()
    X_train_not_null_cols = X_train_imputed.columns[X_train_imputed.notnull().any()].tolist()
    not_null_cols = list(set(X_train_not_null_cols) & set(X_test_not_null_cols))
    # print(X_train_imputed[not_null_cols].isnull().sum().sum())
    # print(X_test_imputed[not_null_cols].isnull().sum().sum())
    return X_train_imputed[not_null_cols], X_test_imputed[not_null_cols]


# prompt: find notnull columns in detn_admit
def get_best_lin_model(detn_admit_only, label):
    import numpy as np
    import shap
    from sklearn.model_selection import train_test_split

    not_null_cols = detn_admit_only.columns[detn_admit_only.notnull().any()].tolist()
    multi_value_cols = [
        col for col in detn_admit_only[not_null_cols].columns if detn_admit_only[col].nunique() > 1
    ]
    numeric_dtypes = (
        detn_admit_only[multi_value_cols].select_dtypes(include=["number"]).columns.to_list()
    )
    numeric_dtypes.remove(label)
    X = detn_admit_only[numeric_dtypes]
    y = detn_admit_only[label]

    # Perform train-test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=43
    )  # Adjust test_size and random_state as needed
    # print(X_train.shape, X_test.shape, y_train.shape, y_test.shape)

    X_train, X_test = impute_missing_values(X_train, X_test)

    model, f1_scored, aic, important_feature_indices = logistic_train(
        X_train, X_test, y_train, y_test
    )

    (
        best_model,
        best_features_lin,
        results_lin_df,
        best_aic_lin,
        features_lin,
    ) = select_logistic_features(model, X_train, important_feature_indices, X_test, y_train, y_test)

    explainer_linear = shap.Explainer(best_model, X_train[best_features_lin])

    return (
        best_model,
        best_features_lin,
        results_lin_df,
        best_aic_lin,
        features_lin,
        X_test,
        y_test,
        explainer_linear,
        X_train,
        y_train,
    )


def get_top_features(gbm, X_train_transformed, features_to_select):
    import pandas as pd

    # prompt: get feature importance from gbm
    # Get feature importances from the trained LightGBM model
    # feature_importances = gbm.feature_importance()
    feature_importances = gbm.feature_importances_

    # Get feature names
    feature_names = X_train_transformed.columns

    # Create a DataFrame for better visualization
    feature_importance_df = pd.DataFrame(
        {"Feature": feature_names, "Importance": feature_importances}
    )

    # Sort the DataFrame by importance in descending order
    feature_importance_df = feature_importance_df.sort_values(by="Importance", ascending=False)
    # prompt: get the top 100 features with importance > 0

    # Filter features with importance greater than 0
    important_features = feature_importance_df[feature_importance_df["Importance"] > 0]

    # Get the top 100 features
    top_100_features = important_features.head(features_to_select)

    top_features = top_100_features["Feature"].to_list()
    return top_features


def lightgbm_train(X_train_transformed, X_test_transformed, y_train, y_test):
    # prompt: train lightgbm model on X_train_transformed
    import lightgbm as lgb
    import pandas as pd
    from sklearn.metrics import accuracy_score, f1_score

    # gbm = lgb.LGBMClassifier()
    gbm = lgb.LGBMClassifier(objective="binary", metric="binary_logloss", verbosity=-1)
    # Train the LightGBM model
    gbm.fit(X_train_transformed, y_train)
    # Make predictions on the test set
    y_pred = gbm.predict(X_test_transformed)
    # Convert probabilities to class labels (e.g. 0 or 1)
    y_pred_class = [1 if prob > 0.5 else 0 for prob in y_pred]  # Adjust threshold as needed
    # Evaluate the model (example metrics)
    accuracy = accuracy_score(y_test, y_pred_class)
    # print(f"Accuracy: {accuracy}")
    f1_scored = f1_score(y_test, y_pred_class)
    # print(f"f1: {f1_scored}")
    # Get feature importances from the trained LightGBM model
    feature_importances = gbm.feature_importances_
    # Get feature names
    feature_names = X_train_transformed.columns
    # Create a DataFrame for better visualization
    feature_importance_df = pd.DataFrame(
        {"Feature": feature_names, "Importance": feature_importances}
    )
    feature_importance_df = feature_importance_df.sort_values(by="Importance", ascending=False)
    # Filter features with importance greater than 0
    important_features = feature_importance_df[feature_importance_df["Importance"] > 0]
    top_features = important_features["Feature"].to_list()
    aic = get_aic(gbm, X_test_transformed, y_test)
    return gbm, f1_scored, aic, top_features


def drop_columns(detn):
    drop_columns_muac(detn, drop_muac=True)


def drop_columns_muac(detn, drop_muac=True):
    detn.drop(columns=[col for col in detn.columns if "lastms" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "otp" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "precalcsite" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "numweeksback" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "glbsite" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "autosite" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "additionalnotes" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "wast" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "attachments" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "photo" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "picture" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "canmovevisit" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "staffmember" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "bednet" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "receivedsmc" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "device" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "lookup_calc" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "submitter" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "dose" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "settlement" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "calcdate" in col], inplace=True)
    detn.drop(
        columns=[col for col in detn.columns if "wfh" in col and col not in ["weekly_last_wfh"]],
        inplace=True,
    )
    detn.drop(columns=[col for col in detn.columns if "manual_daystonv" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "resp_rate_2" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "doneses" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "end_time" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "endtime" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "submissiondate" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "name" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "pp_cm" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "starttime" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "submission_date" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "start_time" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "last_admit" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "c_assigned_cm" in col], inplace=True)
    # detn.drop(columns=[col for col in detn.columns if 'wfa' in col],inplace=True)
    # detn.drop(columns=[col for col in detn.columns if 'hfa' in col],inplace=True)
    detn.drop(columns=[col for col in detn.columns if "first_admit" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "site_admit" in col], inplace=True)
    detn.drop(
        columns=[
            col
            for col in detn.columns
            if "_week" in col and col not in ["muac_loss_2_weeks_consecutive"]
        ],
        inplace=True,
    )
    detn.drop(columns=[col for col in detn.columns if "site_admit" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "site_admit" in col], inplace=True)
    detn.drop(
        columns=[
            col for col in detn.columns if col.endswith("_week") and not col.endswith("_weekly")
        ],
        inplace=True,
    )
    # detn.drop(columns=[col for col in detn.columns if 'hl' in col],inplace=True)
    if drop_muac:
        detn.drop(
            columns=[
                col
                for col in detn.columns
                if "muac" in col
                and col not in ["weekly_last_muac", "muac_loss_2_weeks_consecutive"]
            ],
            inplace=True,
        )
    detn.drop(columns=[col for col in detn.columns if "todate" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if col.endswith("_age")], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "birthdate" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "vax_dates" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if col.startswith("vd_")], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "sequence_num" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if col.endswith("visitnum")], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "row_count" in col], inplace=True)
    detn.drop(
        columns=[
            col
            for col in detn.columns
            if "los" in col
            and col
            not in ["wk1_calc_los", "detn_weight_loss_ever", "muac_loss_2_weeks_consecutive"]
        ],
        inplace=True,
    )
    detn.drop(columns=[col for col in detn.columns if "time_minutes" in col], inplace=True)
    detn.drop(
        columns=[col for col in detn.columns if "wfa" in col and col not in ["wfa_trend"]],
        inplace=True,
    )
    detn.drop(
        columns=[col for col in detn.columns if "hl" in col and col not in ["hl_trend"]],
        inplace=True,
    )
    detn.drop(
        columns=[col for col in detn.columns if "hfa" in col and col not in ["hfa_trend"]],
        inplace=True,
    )
    detn.drop(columns=[col for col in detn.columns if "form" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "date" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "drug_record" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if col.endswith("vax")], inplace=True)
    detn.drop(
        columns=[
            col
            for col in detn.columns
            if "weight" in col and col not in ["wk1_weight_diff_rate", "detn_weight_loss_ever"]
        ],
        inplace=True,
    )


def gbm_shap(
    features,
    N_FEATURES,
    X_train_transformed,
    X_test_transformed,
    X_test_transformed_top,
    y_train,
    y_test,
    cutoff=0.5,
):
    import shap

    gbm_best_features = list(features[N_FEATURES])
    gbm2, _, _, _ = lightgbm_train(
        X_train_transformed[gbm_best_features],
        X_test_transformed[gbm_best_features],
        y_train,
        y_test,
    )
    # Create a SHAP explainer object
    explainer = shap.TreeExplainer(gbm2)
    shap_values = explainer.shap_values(X_test_transformed[gbm_best_features])

    # Generate summary plot
    shap.summary_plot(shap_values, X_test_transformed_top[gbm_best_features])
    # Wrap shap_values in an Explanation object
    shap_values_explanation = shap.Explanation(
        shap_values,
        data=X_test_transformed_top[
            gbm_best_features
        ].values,  # Assuming .values gives you the underlying NumPy array
        feature_names=gbm_best_features,
    )  # Assuming .columns gives you the feature names

    clustering = shap.utils.hclust(X_test_transformed_top[gbm_best_features], y_test)
    shap.plots.bar(
        shap_values_explanation, max_display=20, clustering=clustering, clustering_cutoff=cutoff
    )


def drop_result_columns(detn, label):
    """
    drop columns that happen as a result of the deterioration, reverse causality
    """
    detn.drop(columns=[col for col in detn.columns if "interpolated" in col], inplace=True)
    # could be caused by poor (or good) weight gain
    detn.drop(columns=[col for col in detn.columns if "sachets" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "receivingitp_filter" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "discharge" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "dose" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "rationweeks" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "nv_date" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "eligible" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "drugs" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "dischq" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "lag" in col], inplace=True)
    # Actively receiving treatment may because of complication rather than predicting it
    detn.drop(columns=[col for col in detn.columns if "status_text" in col], inplace=True)
    # set to 1 if Actively receiving treatment
    detn.drop(columns=[col for col in detn.columns if "correct_status" in col], inplace=True)
    # for zero weeks (those w/o wk1) completely determines new onset complication, by def'n
    if "weekly_row_count" in detn.columns:
        detn.drop(columns=[col for col in detn.columns if "weekly_row_count" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "form_version" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "submitter_id" in col], inplace=True)

    # for the 4 in that have only 1 week, if either is set to 1, then new complications always 0, probably caused by new onset complication, rather than predicting it
    detn.drop(columns=[col for col in detn.columns if "imci_emergency_otp" in col], inplace=True)
    detn.drop(columns=[col for col in detn.columns if "referred_emergency" in col], inplace=True)

    # detn.drop(columns=[f'{label}_date'], inplace=True)

    return detn


def explain_logreg(idx, test_labelled_logreg, label):
    for df, explainer, X_test, f1_scored in test_labelled_logreg:
        if idx in df.index:
            test_labelled_lin = df
            lin_model_f1 = f1_scored
            # print(test_labelled_lin.loc[idx])
            _ = explain_prediction(idx, explainer, df, X_test, label)
            break
    return test_labelled_lin, lin_model_f1


def select_features(
    gbm,
    X_train_transformed_top,
    X_test_transformed_top,
    y_train,
    y_test,
    max_features,
    min_features,
    step,
):
    import numpy as np
    import pandas as pd

    results4 = []
    features = {}
    best_aic = 1000000
    for n in np.arange(max_features, min_features, step):
        top_n_features = get_top_features(gbm, X_train_transformed_top, features_to_select=n)
        X_train_transformed_top = X_train_transformed_top[top_n_features].copy()
        X_test_transformed_top = X_test_transformed_top[top_n_features].copy()
        # print(n,len(top_n_features),X_train_transformed_top.shape)
        gbm, f1_scored, aic, top_features = lightgbm_train(
            X_train_transformed_top, X_test_transformed_top, y_train, y_test
        )
        # print(len(gbm.feature_name_))
        data = {"f1_score": f1_scored, "AIC": aic, "num_features": len(gbm.feature_name_)}
        features[n] = gbm.feature_names_in_
        results4.append(data)
        if aic < best_aic:
            best_aic = aic
            best_features = gbm.feature_names_in_
            best_gbm = gbm
    return best_gbm, best_features, pd.DataFrame(results4), best_aic, features


def strip_column_names(top_features):
    # prompt: for each column in top_features remove the period and the text after the period

    # Assuming 'top_features' list is already defined as in your provided code.
    top_features = [col.split(".", 1)[0] for col in top_features]
    top_features = [
        col.replace("_day", "") if col.endswith("_day") else col for col in top_features
    ]
    top_features = [
        col.replace("_month", "") if col.endswith("_month") else col for col in top_features
    ]
    top_features = set(top_features)
    top_features = list(top_features)
    return top_features


def print_patient_probabilities(
    label,
    idx,
    test_labelled_tree,
    test_labelled,
    test_labelled_lin,
    tree_model_f1,
    ag_model_f1,
    lin_model_f1,
    test_labelled_gbm,
    gbm_model_f1,
):
    probability = test_labelled_tree.loc[idx][f"probability_{label}"]
    ag_probability = test_labelled.loc[idx][f"probability_{label}"]
    gbm_probability = test_labelled_gbm.loc[idx][f"probability_{label}"]
    print("\n")
    print(f"Autogluon (f1: {ag_model_f1:f}) probability: {ag_probability:f}")
    print(f"GBM classifier (f1: {gbm_model_f1:f}) probability: {gbm_probability:f}")
    print(f"Decision tree classifier (f1: {tree_model_f1:f}) probability: {probability:f}")
    if lin_model_f1 != None:
        lin_probability = test_labelled_lin.loc[idx][f"probability_{label}"]
        print(f"Logistic regression (f1: {lin_model_f1:f}) probability: {lin_probability:f}")


def explain_prediction(idx, explainer, test_labelled, X_test, label):
    import matplotlib.pyplot as plt
    import shap

    # Instead of:
    # shap.plots.waterfall(explainer.shap_values(X_test.iloc[0,:]))
    # Use:
    # shap_values_single = explainer.shap_values(X_test.iloc[0,:]) # Calculate SHAP values

    shap_values_single = explainer.shap_values(X_test.loc[idx])  # Calculate SHAP values

    values_to_display = test_labelled.loc[idx][
        ["pid", f"probability_{label}", f"predicted_{label}", label]
    ]
    plt.text(0.3, 0.1, f"{values_to_display}", transform=plt.gca().transAxes)
    exp = shap.Explanation(
        shap_values_single,
        explainer.expected_value,
        # data=X_test.iloc[75,:].values,
        data=X_test.loc[idx].values,
        feature_names=X_test.columns,
    )
    shap.plots.waterfall(exp)  # Pass the Explanation object to waterfall plot
    return values_to_display


def explain_prediction_gbm(idx, gbm, test_labelled_gbm, X_test_transformed, label):
    import matplotlib.pyplot as plt
    import shap

    explainer_gbm2 = shap.TreeExplainer(gbm)
    shap_values_gbm2 = explainer_gbm2.shap_values(X_test_transformed)

    iloc = X_test_transformed.index.get_loc(idx)

    values_to_display = test_labelled_gbm.loc[idx][
        ["pid", f"probability_{label}", f"predicted_{label}", label]
    ]
    plt.text(0.3, 0.1, f"{values_to_display}", transform=plt.gca().transAxes)

    exp2 = shap.Explanation(
        shap_values_gbm2[iloc],
        explainer_gbm2.expected_value,
        # data=X_test.iloc[75,:].values,
        data=X_test_transformed.loc[idx].values,
        feature_names=X_test_transformed.columns,
    )

    log_odds = exp2.values.sum() + exp2.base_values

    probability = log_odds_to_probability(log_odds)
    plt.text(0.3, 0.5, "LightGBM model", transform=plt.gca().transAxes)
    plt.text(
        0.3,
        0.3,
        f"The probability of {label}\ncorresponding to log odds {log_odds:.3f} is: {probability:.2%}",
        transform=plt.gca().transAxes,
    )

    shap.plots.waterfall(exp2)  # Pass the Explanation object to waterfall plot

    return values_to_display


def log_odds_to_probability(log_odds):
    """Converts log odds to probability.

    Args:
      log_odds: The log odds value.

    Returns:
      The probability.
    """
    import numpy as np

    odds = np.exp(log_odds)
    probability = odds / (1 + odds)
    return probability


def convert_to_bool(df):
    for col in df.columns:
        # Check if the column has exactly two unique non-null values (excluding NaN)
        # and if both of these values can be interpreted as boolean (0/1, True/False, etc.)

        # Get unique non-null values
        series1 = df[col].dropna()
        if len(series1) < 2:
            continue
        unique_values = series1.unique()

        if len(unique_values) == 2 and all(val in [0, 1, True, False] for val in unique_values):
            # Convert to boolean only if both values are boolean-like
            df[col] = df[col].astype(bool)
    return df


def convert_bool_to_int(df):
    """Converts all boolean columns in a pandas DataFrame to integer type."""
    for column in df.columns:
        if df[column].dtype == bool:
            df[column] = df[column].astype(int)
    return df


def check_cols(admit_current_row, cols, which_cat="cat1"):
    true_cols = []
    for col in cols:
        if admit_current_row[col].iloc[0] == True:
            true_cols.append(col)

    if true_cols:
        print(f"{which_cat} Columns with True values upon admission:")
        for col in true_cols:
            print(col)
    else:
        print(f"all {which_cat} columns false upon admission")


def make_dummy_columns(df):
    import pandas as pd

    categorical_cols = df.select_dtypes(include=["category"]).columns
    if len(categorical_cols) == 0:
        return df

    # Create dummy variables for categorical columns
    dummy_df = pd.get_dummies(df[categorical_cols], drop_first=True)
    for col in dummy_df.columns:
        if dummy_df[col].dtype == bool:
            dummy_df[col] = dummy_df[col].astype(int)

    # Get non-categorical columns
    non_categorical_cols = df.select_dtypes(exclude=["category"]).columns

    # Concatenate dummy variables with non-categorical columns
    df = pd.concat([df[non_categorical_cols], dummy_df], axis=1)
    df.columns = [col.replace(" ", "_") for col in df.columns]

    return df


def plot_corr(detn, variable, label):
    plot_corr_jitter(detn, variable, label, 0.1)


def plot_anthros(
    values_to_display,
    admit_weekly,
    admit_current,
    detn,
    cat1_cols,
    cat2_cols,
    boolean_cat1_weekly_cols,
    boolean_cat2_weekly_cols,
    label,
    cat1_weekly_cols,
    cat2_weekly_cols,
):
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd

    pid = values_to_display["pid"]
    admit_weekly_rows = admit_weekly[admit_weekly["pid"] == pid]
    admit_current_row = admit_current[admit_current["pid"] == pid]
    detn_row = detn[detn["pid"] == pid]
    # prompt: get columns that contain cat1 and end in _weekly from admit_weekly
    # check_cols(admit_current_row,cat1_cols)
    # check_cols(admit_current_row,cat2_cols,which_cat='cat2')

    # Filter columns that contain 'cat1' and end with '_weekly'

    # prompt: columns in admit_weekly_rows[boolean_cat2_weekly_cols].sum() where sum() > 0

    # Get columns where the sum is greater than 0
    positive_sum_cols1 = (
        admit_weekly_rows[boolean_cat1_weekly_cols].sum()[lambda x: x > 0].index.to_list()
    )
    positive_sum_cols1.insert(0, "calcdate_days_since_first")
    positive_sum_cols1.insert(0, "sequence_num")
    positive_sum_cols1.insert(0, "calcdate_weekly")

    admit_weekly_rows[positive_sum_cols1]
    #
    # prompt: get positive_sum_cols1 for calcdate_weekly == detn_row[f'{label}_date']
    onset_date = detn_row[f"{label}_date"].iloc[0]
    print("onset_date", onset_date)

    filtered_admit_weekly_rows = admit_weekly_rows[
        admit_weekly_rows["calcdate_weekly"] == onset_date
    ].copy()

    filtered_admit_weekly_rows["calcdate_weekly"] = filtered_admit_weekly_rows[
        "calcdate_weekly"
    ].dt.date

    # prompt: columns in admit_weekly_rows[boolean_cat2_weekly_cols].sum() where sum() > 0
    # Get columns where the sum is greater than 0
    positive_sum_cols2 = (
        admit_weekly_rows[boolean_cat2_weekly_cols].sum()[lambda x: x > 0].index.to_list()
    )
    positive_sum_cols2.insert(0, "calcdate_weekly")
    admit_weekly_rows[positive_sum_cols2]

    # prompt: find rows in admit_weekly_rows where  boolean_cat2_weekly_cols.any()

    # Find rows where any boolean_cat2_weekly_cols is True
    rows_with_true_cat2 = admit_weekly_rows[
        admit_weekly_rows[boolean_cat2_weekly_cols].any(axis=1)
    ].copy()

    # positive_sum_cols2.remove('calcdate')
    positive_sum_cols2.insert(0, "calcdate_days_since_first")
    # positive_sum_cols2.insert(0,'calcdate_weekly')

    # positive_sum_cols1.insert(0,'calcdate_days_since_first')
    positive_sum_cols2.insert(0, "sequence_num")
    rows_with_true_cat2["calcdate_weekly"] = rows_with_true_cat2["calcdate_weekly"].dt.date

    rows_with_true_cat2[positive_sum_cols2]
    cat1_weekly_cols.insert(0, "calcdate_weekly")
    cat2_weekly_cols.insert(0, "calcdate_weekly")

    anthro_cols = ["calcdate", "muac", "weight", "hl"]
    admit_weekly_rows = admit_weekly_rows.rename(
        columns={
            "calcdate_weekly": "calcdate",
            "muac_weekly": "muac",
            "weight_weekly": "weight",
            "hl_weekly": "hl",
        }
    )

    admit_current_row = admit_current_row.rename(columns={"hl_admit": "hl"})

    anthros = pd.concat([admit_weekly_rows[anthro_cols], admit_current_row[anthro_cols]], axis=0)
    anthros, variable_days = days_since_min(anthros, "calcdate")
    anthros.sort_values(by="calcdate", inplace=True)

    anthroz_cols = ["calcdate", "wfh_weekly", "hfa_weekly", "wfa_weekly"]

    temp_row = admit_current_row.rename(
        columns={"wfh": "wfh_weekly", "hfa": "hfa_weekly", "wfa": "wfa_weekly"}
    )

    anthroz = pd.concat([admit_weekly_rows[anthroz_cols], temp_row[anthroz_cols]], axis=0)

    anthroz = anthroz.rename(
        columns={"wfh_weekly": "wfh", "hfa_weekly": "hfa", "wfa_weekly": "wfa"}
    )
    anthroz, variable_days = days_since_min(anthroz, "calcdate")
    anthroz.sort_values(by="calcdate", inplace=True)

    # Create the plot
    fig, ax1 = plt.subplots(figsize=(12, 6))

    # Plot MUAC, weight, and hl on the primary y-axis
    ax1.plot(anthros["days_since_min_calcdate"], anthros["muac"], label="MUAC", marker="o")
    line = ax1.lines[-1]  # Get the most recently added line (the plot line)
    muac_color = line.get_color()
    ax1.plot(anthros["days_since_min_calcdate"], anthros["weight"], label="Weight", marker="x")
    line = ax1.lines[-1]  # Get the most recently added line (the plot line)
    weight_color = line.get_color()

    # Create a secondary y-axis
    ax2 = ax1.twinx()
    ax2.plot(
        anthros["days_since_min_calcdate"], anthros["hl"], label="HL", marker="s", color="green"
    )
    line = ax2.lines[-1]  # Get the most recently added line (the plot line)
    hl_color = line.get_color()

    # Add labels and title
    ax1.set_xlabel("Days Since Admission (Minimum Calcdate)")
    ax1.set_ylabel("MUAC and Weight")
    ax2.set_ylabel("HL")
    plt.title(f"MUAC, Weight, and HL over Time for {pid}")

    if rows_with_true_cat2.shape[0] > 0:
        first_cat2_onset = rows_with_true_cat2["calcdate_weekly"].iloc[0]
        first_cat2_onset = pd.to_datetime(first_cat2_onset)
        first_cat2 = anthros[anthros["calcdate"] == first_cat2_onset][
            "days_since_min_calcdate"
        ].iloc[0]

        ax1.axvline(x=first_cat2 - 0.5, color="black", linestyle="--")

        # Add text at the midpoint of the y-axis
        y_midpoint = (ax1.get_ylim()[0] + ax1.get_ylim()[1]) / 2
        cat2_text = rows_with_true_cat2[positive_sum_cols2].T
        # ax1.text(first_cat2 - 0.6, y_midpoint, f'{cat2_text}', rotation=0, va='center', ha='right')

    onset_date = detn_row[f"{label}_date"].iloc[0]

    # Check if the filtered Series is empty before accessing iloc[0]
    filtered_series = anthros[anthros["calcdate"] == onset_date]["days_since_min_calcdate"]
    if not filtered_series.empty:
        first_cat1 = filtered_series.iloc[0]
        ax1.axvline(x=first_cat1, color="red", linestyle="--")
        # Add text at the midpoint of the y-axis
        cat1_info = filtered_admit_weekly_rows[positive_sum_cols1].T
        y_midpoint = (ax1.get_ylim()[0] + ax1.get_ylim()[1]) / 2
        ax1.text(first_cat1 + 0.1, y_midpoint, f"{cat1_info}", rotation=0, va="center", ha="left")

    # Add trend lines
    if anthros.shape[0] > 2:
        z = np.polyfit(anthros["days_since_min_calcdate"], anthros["muac"], 1)
        p = np.poly1d(z)
        ax1.plot(
            anthros["days_since_min_calcdate"],
            p(anthros["days_since_min_calcdate"]),
            "--",
            color=muac_color,
            label="MUAC Trend",
        )

        z = np.polyfit(anthros["days_since_min_calcdate"], anthros["weight"], 1)
        p = np.poly1d(z)
        ax1.plot(
            anthros["days_since_min_calcdate"],
            p(anthros["days_since_min_calcdate"]),
            "--",
            color=weight_color,
            label="Weight Trend",
        )

        z = np.polyfit(anthros["days_since_min_calcdate"], anthros["hl"], 1)
        p = np.poly1d(z)
        ax2.plot(
            anthros["days_since_min_calcdate"],
            p(anthros["days_since_min_calcdate"]),
            "--",
            color=hl_color,
            label="HL Trend",
        )

    # Add legends
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels + labels2, loc="upper left")

    # Show the plot
    plt.show(block=True)

    # prompt: plot 'wfhz', 'hfaz', 'wfaz'  with trend lines in anthroz by days_since_min_calcdate

    # Create the plot
    fig, ax = plt.subplots(figsize=(12, 6))

    for col in ["wfh", "wfa"]:
        ax.plot(anthroz["days_since_min_calcdate"], anthroz[col], label=col, marker="o")
        if anthroz.shape[0] > 2:
            z = np.polyfit(anthroz["days_since_min_calcdate"], anthroz[col], 1)
            p = np.poly1d(z)
            line = ax.lines[-1]  # Get the most recently added line (the plot line)
            color = line.get_color()
            ax.plot(
                anthroz["days_since_min_calcdate"],
                p(anthroz["days_since_min_calcdate"]),
                "--",
                color=color,
                label=f"{col} Trend",
            )

            # Create a secondary y-axis
    ax2 = ax.twinx()
    ax2.plot(
        anthroz["days_since_min_calcdate"], anthroz["hfa"], label="hfa", color="green", marker="o"
    )
    if anthroz.shape[0] > 2:
        z = np.polyfit(anthroz["days_since_min_calcdate"], anthroz["hfa"], 1)
        p = np.poly1d(z)
        line = ax2.lines[-1]  # Get the most recently added line (the plot line)
        color = line.get_color()
        ax2.plot(
            anthroz["days_since_min_calcdate"],
            p(anthroz["days_since_min_calcdate"]),
            "--",
            color=color,
            label=f"hfa Trend",
        )
    ax2.set_ylabel("HFA")

    lines, labels = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels + labels2, loc="upper left")

    if rows_with_true_cat2.shape[0] > 0:
        first_cat2_onset = rows_with_true_cat2["calcdate_weekly"].iloc[0]
        first_cat2_onset = pd.to_datetime(first_cat2_onset)
        first_cat2 = anthros[anthros["calcdate"] == first_cat2_onset][
            "days_since_min_calcdate"
        ].iloc[0]

        ax.axvline(x=first_cat2 - 0.5, color="black", linestyle="--")

        # Add text at the midpoint of the y-axis
        y_midpoint = (ax.get_ylim()[1] + ax.get_ylim()[0]) / 2
        cat2_text = rows_with_true_cat2[positive_sum_cols2].T
        ax.text(first_cat2 - 0.6, y_midpoint, f"{cat2_text}", rotation=0, va="center", ha="right")

    onset_date = detn_row[f"{label}_date"].iloc[0]
    filtered_series = anthros[anthros["calcdate"] == onset_date]["days_since_min_calcdate"]
    if not filtered_series.empty:
        first_cat1 = filtered_series.iloc[0]

        ax.axvline(x=first_cat1, color="red", linestyle="--")
        # Add text at the midpoint of the y-axis
        cat1_info = filtered_admit_weekly_rows[positive_sum_cols1].T
        # ax.text(first_cat1+0.1, y_midpoint, f'{cat1_info}', rotation=0, va='center', ha='left')

    ax.set_xlabel("Days Since Admission (Minimum Calcdate)")
    ax.set_ylabel("WFH and WFA")
    plt.title(f"wfh, hfa, and wfa over Time for {pid}")
    # ax.legend()
    plt.show()


def drop_recent_columns(detn, use_cache=True, months=15):
    import pandas as pd

    if use_cache:
        recent_admit_columns = [
            "c_imci_emergency",
            "where_referred_emergency",
            "other_state",
            "other_lga",
            "ts_assessed_malnstatus",
            "ts_assessed_needitp",
            "manual_nvdate",
            "pt_photo",
            "supp_vd_ipv1",
            "supp_vd_rota2",
            "supp_vd_rota3",
            "supp_vd_ipv2",
            "cleaning_note",
        ]
        recent_raw_columns = [
            "b_dpth_sorethroat",
            "b_dpth_diffswallow",
            "b_dpth_bloody",
            "b_dpth_lymph",
            "b_suspecteddipth",
            "ofstaffmember",
            "spec_imci_em_other",
            "other_state",
            "other_lga",
            "b_figurepid",
            "manual_prev_status",
            "c_azafu_symptoms",
            "b_azafu_multivomitepi",
            "b_azafu_vomitl24",
            "c_azafu_vomitl24freq",
            "b_azafu_vomiteveryoral",
            "b_azafu_soughtcare",
            "c_azafu_whycare",
            "b_azafu_overnightcare",
            "c_azafu_whyovernightcare",
            "c_azafu_urinefreq",
            "c_azafu_urinecolor",
            "b_azafu_otherprob",
            "b_azafu_reqclinician",
            "proceed_previnel",
            "inac_weight_measurement",
            "hl_measurement",
            "c_physician_assess",
            "b_phys_req_itp",
            "muac_measurement",
            "indiv_valid_admit",
            "manual_admit_type_other",
            "patient_picture",
            "cg_relationship_other",
            "phone_owner_other",
            "otherlang_text",
            "referring_other",
            "migrate_reason_other",
            "b_receivedsmc",
            "b_bednet",
            "c_bednettype",
            "wall_type_other",
            "drinking_water_other",
            "toilet_other",
            "resp_rate_2",
            "resp_rate_3",
            "cat1_diarrhea",
            "orash_other_text",
            "b_swellingtender",
            "q_conf_override_lref",
            "why_noaccepthts",
            "supp_vd_ipv1",
            "supp_vd_rota2",
            "supp_vd_rota3",
            "supp_vd_ipv2",
            "pp_ipv1_precalc",
            "pp_rota2_precalc",
            "pp_rota3_precalc",
            "pp_ipv2_precalc",
            "override_conf",
            "why_override_rat",
            "manual_treatment",
            "conf_initremover",
            "remdrug_filter",
            "rem_drugs_which",
            "expl_remal_act",
            "expl_remlorat",
            "expl_remnystatin",
            "expl_remtetra",
            "expl_remzincox",
            "expl_remotomed",
            "conf_twoaddover",
            "conf_changedose",
            "confirmmovevisit",
            "manual_nvdate",
            "manual_daystonv",
            "vita_dose",
            "b_isinedc",
        ]
        weekly_columns_to_delete = {
            "wk3_backuplength",
            "wk3_wfh_edc_status",
            "wk2_wfh_edc_status",
            "wk2_backuplength",
            "wk1_wfh_edc_status",
            "wk1_backuplength",
        }
        weekly_raw_columns_to_delete = {
            "wk1_expl_remlorat",
            "wk3_expl_remroutalbend",
            "wk2_calc_anthro_eligibledischarge_enhanced",
            "wk2_why_override_rat",
            "wk1_muac_measurement",
            "wk1_calc_anthro_eligibledischarge_normal",
            "wk3_why_override_rat",
            "wk3_confirmmovevisit",
            "wk1_pull_edc_consent",
            "wk3_expl_addaa_act",
            "wk3_calc_anthro_eligibledischarge",
            "wk1_instance_name",
            "wk3_wfh_edc_threshold",
            "wk2_expl_remroutalbend",
            "wk3_pull_edc_consent",
            "wk3_b_contprogram_possexclucrit",
            "wk1_expl_remotomed",
            "wk3_hl_measurement",
            "wk1_manual_treatment",
            "wk2_hl_measurement",
            "wk1_wfh_edc_threshold",
            "wk3_pull_dischargecriteria",
            "wk2_confirmmovevisit",
            "wk3_muac_measurement",
            "wk2_calc_anthro_eligibledischarge_normal",
            "wk3_expl_remaa_act",
            "wk2_pull_edc_consent",
            "wk3_d_possexclucrit",
            "wk1_expl_remaa_act",
            "wk3_calc_anthro_eligibledischarge_normal",
            "wk1_pull_dischargecriteria",
            "wk2_muac_measurement",
            "wk2_calc_anthro_eligibledischarge",
            "wk2_pull_dischargecriteria",
            "wk3_expl_remotomed",
            "wk3_instance_name",
            "wk1_d_possexclucrit",
            "wk1_calc_anthro_eligibledischarge_enhanced",
            "wk1_b_contprogram_possexclucrit",
            "wk1_why_override_rat",
            "wk2_b_contprogram_possexclucrit",
            "wk2_instance_name",
            "wk2_expl_remaa_act",
            "wk3_expl_addroutalben",
            "wk1_expl_remroutalbend",
            "wk2_manual_treatment",
            "wk1_confirmmovevisit",
            "wk2_d_possexclucrit",
            "wk1_calc_anthro_eligibledischarge",
            "wk2_expl_addroutalben",
            "wk2_expl_remotomed",
            "wk1_expl_addroutalben",
            "wk2_expl_remlorat",
            "wk1_hl_measurement",
            "wk3_manual_treatment",
            "wk2_expl_addaa_act",
            "wk3_expl_remlorat",
            "wk1_expl_addaa_act",
            "wk2_wfh_edc_threshold",
            "wk3_calc_anthro_eligibledischarge_enhanced",
        } - set(weekly_columns_to_delete)
        detn.drop(columns=weekly_columns_to_delete.intersection(set(detn.columns)), inplace=True)
        detn.drop(
            columns=weekly_raw_columns_to_delete.intersection(set(detn.columns)), inplace=True
        )

    else:
        dir = "/content/drive/My Drive/[PBA] Full datasets/"
        raw = pd.read_csv(dir + "FULL_pba_admit_raw_2024-11-15.csv")
        raw["todate"] = pd.to_datetime(raw["todate"])

        admit = pd.read_csv(dir + "FULL_pba_admit_processed_2024-11-15.csv")
        admit["calcdate"] = pd.to_datetime(admit["calcdate"])

        weekly = pd.read_csv(dir + "FULL_pba_weekly_processed_2024-11-15.csv")
        weekly["calcdate"] = pd.to_datetime(weekly["calcdate"])

        weekly_raw = pd.read_csv(dir + "FULL_pba_weekly_raw_2024-11-15.csv")
        weekly_raw["todate"] = pd.to_datetime(weekly_raw["todate"])

        raw.dropna(axis=1, how="all", inplace=True)
        raw_columns = pd.DataFrame()
        for col in raw.columns:
            series1 = (
                raw[col].notnull().groupby(raw["todate"].dt.to_period("M")).sum()
                / raw["todate"].groupby(raw["todate"].dt.to_period("M")).count()
            )
            populated_months = (series1 > 0).sum()
            raw_columns.loc[col, "populated_months"] = populated_months

        recent_raw_columns = raw_columns[raw_columns["populated_months"] < months].index.to_list()
        print("recent_raw_columns", recent_raw_columns)

        admit.dropna(axis=1, how="all", inplace=True)
        admit_columns = pd.DataFrame()
        for col in admit.columns:
            series1 = (
                admit[col].notnull().groupby(admit["calcdate"].dt.to_period("M")).sum()
                / admit["calcdate"].groupby(admit["calcdate"].dt.to_period("M")).count()
            )
            populated_months = (series1 > 0).sum()
            admit_columns.loc[col, "populated_months"] = populated_months
        recent_admit_columns = admit_columns[
            admit_columns["populated_months"] < months
        ].index.to_list()
        print("recent_admit_columns", recent_admit_columns)

        weekly.dropna(axis=1, how="all", inplace=True)
        weekly_columns = pd.DataFrame()
        for col in weekly.columns:
            series1 = (
                weekly[col].notnull().groupby(weekly["calcdate"].dt.to_period("M")).sum()
                / weekly["calcdate"].groupby(weekly["calcdate"].dt.to_period("M")).count()
            )
            populated_months = (series1 > 0).sum()
            weekly_columns.loc[col, "populated_months"] = populated_months
        recent_weekly_columns = weekly_columns[
            weekly_columns["populated_months"] < months
        ].index.to_list()
        weekly_columns_to_delete = set()
        for y in range(1, 4):
            wk_columns = [f"wk{y}_{x}" for x in recent_weekly_columns]
            wk_columns_set = set(wk_columns).intersection(set(detn.columns))
            weekly_columns_to_delete = weekly_columns_to_delete.union(wk_columns_set)
        weekly_columns_to_delete = weekly_columns_to_delete.union(wk_columns_set)
        weekly_columns_to_delete = weekly_columns_to_delete.intersection(set(detn.columns))
        print("weekly_columns_to_delete", weekly_columns_to_delete)
        detn.drop(columns=weekly_columns_to_delete, inplace=True)

        weekly_raw.dropna(axis=1, how="all", inplace=True)
        weekly_raw_columns = pd.DataFrame()
        for col in weekly_raw.columns:
            series1 = (
                weekly_raw[col].notnull().groupby(weekly_raw["todate"].dt.to_period("M")).sum()
                / weekly_raw["todate"].groupby(weekly_raw["todate"].dt.to_period("M")).count()
            )
            populated_months = (series1 > 0).sum()
            weekly_raw_columns.loc[col, "populated_months"] = populated_months
        recent_weekly_raw_columns = weekly_raw_columns[
            weekly_raw_columns["populated_months"] < months
        ].index.to_list()
        weekly_raw_columns_to_delete = set()
        for y in range(1, 4):
            wk_columns = [f"wk{y}_{x}" for x in recent_weekly_raw_columns]
            wk_columns_set = set(wk_columns).intersection(set(detn.columns))
            weekly_raw_columns_to_delete = weekly_raw_columns_to_delete.union(wk_columns_set)
        weekly_raw_columns_to_delete = weekly_raw_columns_to_delete.union(wk_columns_set)
        weekly_raw_columns_to_delete = weekly_raw_columns_to_delete.intersection(set(detn.columns))
        print("weekly_raw_columns_to_delete", weekly_raw_columns_to_delete)
        detn.drop(columns=weekly_raw_columns_to_delete, inplace=True)

    admit_columns_to_delete = set(recent_admit_columns).intersection(set(detn.columns))
    detn.drop(columns=admit_columns_to_delete, inplace=True)

    raw_columns_to_delete = set(recent_raw_columns).intersection(set(detn.columns))
    detn.drop(columns=raw_columns_to_delete, inplace=True)


# prompt: for col in y find the top 5 correlations in X and make a dataframe from it


# Assuming X and y are already defined as in the provided code
# and cat1_notests contains the column names in y


def top_correlations(X, y_col, top_n, detn):
    """
    Finds the top N correlations between columns in X and a specific column in y.

    Args:
        X: DataFrame containing the features.
        y_col: String, name of the column in y to check correlations against.
        top_n: Integer, the number of top correlations to return.

    Returns:
        DataFrame: DataFrame with the top N correlated features and their correlation values.
    """
    correlations = X.corrwith(detn[y_col]).abs().sort_values(ascending=False)
    return pd.DataFrame(
        {"Feature": correlations.index[0:top_n], "Correlation": correlations.values[0:top_n]}
    )


def ag_feature_generator(X_train, X_test):
    from autogluon.features.generators import AutoMLPipelineFeatureGenerator

    auto_ml_pipeline_feature_generator = AutoMLPipelineFeatureGenerator()
    X_train_transformed = auto_ml_pipeline_feature_generator.fit_transform(X=X_train)
    X_test_transformed = auto_ml_pipeline_feature_generator.transform(X_test)
    # Replace whitespace in column names with underscores
    X_train_transformed.columns = [col.replace(" ", "_") for col in X_train_transformed.columns]
    X_test_transformed.columns = [col.replace(" ", "_") for col in X_test_transformed.columns]

    X_train_transformed.columns = X_train_transformed.columns.str.replace(
        "[, ]", "_", regex=True
    )  # Replace commas and spaces
    X_test_transformed.columns = X_test_transformed.columns.str.replace(
        "[, ]", "_", regex=True
    )  # Replace commas and spaces
    # TODO allow periods through
    X_train_transformed.columns = X_train_transformed.columns.str.replace(
        "[^a-zA-Z0-9_]", "_", regex=True
    )
    X_test_transformed.columns = X_test_transformed.columns.str.replace(
        "[^a-zA-Z0-9_]", "_", regex=True
    )

    # prompt: remove duplicate column names in X_train_transformed

    # Drop duplicate columns in X_train_transformed
    X_train_transformed = X_train_transformed.loc[:, ~X_train_transformed.columns.duplicated()]
    X_test_transformed = X_test_transformed.loc[:, ~X_test_transformed.columns.duplicated()]
    return X_train_transformed, X_test_transformed


def ag_regress_model_load(label, model, frac, detn):
    import numpy as np
    import pandas as pd
    import shap
    from autogluon.tabular import TabularDataset, TabularPredictor
    from sklearn.metrics import r2_score, root_mean_squared_error
    from sklearn.model_selection import train_test_split

    MODEL_PATH = "/content/drive/My Drive/[PBA] Code/model/"
    model_path = f"{MODEL_PATH}/{label}{model}/"
    predictor = TabularPredictor.load(
        model_path, require_py_version_match=False, require_version_match=False
    )

    ag_features = predictor.features()
    print("ag features:", ag_features)
    print(detn.shape)
    print("wk1_cg_weight_weekly" in detn.columns)

    X = detn[ag_features]
    y = detn[label]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=43
    )  # Adjust test_size and random_state as needed

    train_data = TabularDataset(X_train[ag_features].join(y_train))
    test_data = TabularDataset(X_test[ag_features].join(y_test))
    y_pred = predictor.predict(test_data.drop(columns=[label]))
    print(predictor.evaluate(test_data, silent=True))
    rmse = root_mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    print(f"AutoGluon rmse: {rmse:f}")

    class AutogluonWrapper2:
        def __init__(self, predictor, feature_names):
            self.ag_model = predictor
            self.feature_names = feature_names

        def predict(self, X):
            if isinstance(X, pd.Series):
                X = X.values.reshape(1, -1)
            if not isinstance(X, pd.DataFrame):
                X = pd.DataFrame(X, columns=self.feature_names)
            return self.ag_model.predict(X)

    ag_wrapper = AutogluonWrapper2(predictor, X_train.columns)

    explainer = shap.KernelExplainer(ag_wrapper.predict, shap.sample(X_train, 100))

    NSHAP_SAMPLES = 100  # how many samples to use to approximate each Shapely value, larger values will be slower
    N_VAL = 30

    X_test_sample = X_test[ag_features].sample(
        frac=frac, random_state=42
    )  # Use random_state for reproducibility
    shap_values = explainer.shap_values(X_test_sample, nsamples=NSHAP_SAMPLES)
    shap.summary_plot(shap_values, X_test_sample)
    return explainer, predictor, ag_features, y_pred, rmse, X_test, y_test


def ag_model_load(label, frac, detn):
    return ag_model_load_suffix(label, frac, detn, "")


def ag_model_load_suffix(label, frac, detn, suffix):
    import os

    import shap
    from autogluon.tabular import TabularDataset, TabularPredictor
    from sklearn.metrics import confusion_matrix, f1_score
    from sklearn.model_selection import train_test_split

    os.chdir("/content/drive/My Drive/[PBA] Code")

    from util import AutogluonWrapper

    os.chdir("/content")
    MODEL_PATH = "/content/drive/My Drive/[PBA] Code/model"

    model_path = f"{MODEL_PATH}/{label}{suffix}/"
    predictor = TabularPredictor.load(
        model_path, require_py_version_match=False, require_version_match=False
    )

    ag_features = predictor.features()
    print("ag features:", len(ag_features), ag_features)

    X = detn[ag_features]
    y = detn[label]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=43
    )  # Adjust test_size and random_state as needed

    train_data = TabularDataset(X_train[ag_features].join(y_train))
    test_data = TabularDataset(X_test[ag_features].join(y_test))
    print(predictor.evaluate(test_data, silent=True))
    y_pred = predictor.predict(X_test[ag_features])
    y_pred_proba = predictor.predict_proba(X_test[ag_features])
    y_pred_proba_all = predictor.predict_proba(X[ag_features])
    ag_model_f1 = f1_score(y_test, y_pred)
    print(f"AutoGluon f1: {ag_model_f1:f}")
    cm = confusion_matrix(y_test, y_pred)
    print("Confusion Matrix:")
    print(cm)

    target_class = 1  # can be any possible value of the label column
    negative_class = 0
    baseline = X_train[ag_features][y_train == negative_class].sample(20, random_state=0)
    ag_wrapper = AutogluonWrapper(predictor, ag_features, target_class)
    explainer = shap.KernelExplainer(ag_wrapper.predict_proba, baseline)

    NSHAP_SAMPLES = 200

    X_test_sample = X_test[ag_features].sample(
        frac=frac, random_state=42
    )  # Use random_state for reproducibility
    shap_values = explainer.shap_values(X_test_sample, nsamples=NSHAP_SAMPLES)
    shap.summary_plot(shap_values, X_test_sample)
    return (
        explainer,
        predictor,
        ag_features,
        y_pred,
        y_pred_proba,
        ag_model_f1,
        X_test,
        y_test,
        y_pred_proba_all,
    )


def explain_regress_ag_model(
    idx, explainer, X_test, label, y_pred, y, rmse, PID, anthropometrics=True
):
    import matplotlib.pyplot as plt
    import shap

    shap_values_single = explainer.shap_values(X_test.loc[idx])  # Calculate SHAP values
    if anthropometrics:
        model_text = "AutoGluon (anthropometrics) model"
    else:
        model_text = "AutoGluon (non-anthropometrics) model"

    plt.text(
        0.3,
        0.5,
        f"{model_text}\n(rmse: {rmse:.4f}) for\npredicting {label}\nfor patient {PID}",
        transform=plt.gca().transAxes,
    )
    plt.text(
        0.3, 0.1, f"predicted value: {y_pred:.4f}\nactual value: {y}", transform=plt.gca().transAxes
    )
    exp = shap.Explanation(
        shap_values_single,
        explainer.expected_value,
        data=X_test.loc[idx].values,
        feature_names=X_test.columns,
    )
    shap.plots.waterfall(exp)  # Pass the Explanation object to waterfall plot


def explain_ag_model(idx, explainer, X_test, label, y_pred_proba, y_pred, y, ag_model_f1, PID):
    import matplotlib.pyplot as plt
    import shap

    shap_values_single = explainer.shap_values(X_test.loc[idx])  # Calculate SHAP values
    plt.text(
        0.3,
        0.5,
        f"AutoGluon model (f1: {ag_model_f1:.4f}) for\nprobability of {label}\nfor patient {PID}",
        transform=plt.gca().transAxes,
    )
    plt.text(
        0.3,
        0.1,
        f"predicted probability: {y_pred_proba:.1%}\npredicted class: {y_pred}\nactual class: {y}",
        transform=plt.gca().transAxes,
    )
    exp = shap.Explanation(
        shap_values_single,
        explainer.expected_value,
        data=X_test.loc[idx].values,
        feature_names=X_test.columns,
    )
    shap.plots.waterfall(exp)  # Pass the Explanation object to waterfall plot


def label_test_gbm(X_test_transformed, y_test, gbm, label, detn):
    # y_pred_proba = predictor.predict_proba(X_test)
    import pandas as pd

    # Join X_test and y_test
    df = X_test_transformed.join(y_test)

    y_pred_proba = gbm.predict_proba(X_test_transformed)
    y_pred = gbm.predict(X_test_transformed)
    # Rename the y_pred Series to 'predicted'
    y_pred = pd.Series(y_pred, name=f"predicted_{label}")

    # Join X_test and y_test
    df = X_test_transformed.join(y_test)

    # prompt: join df to y_pred renaming column to predicted
    # Reset index of both DataFrames before concatenation
    df = df.reset_index(drop=False)
    y_pred = y_pred.reset_index(drop=True)

    # Concatenate the DataFrames
    df = pd.concat([df, y_pred], axis=1)

    # prompt: get the second column in y_pred_proba
    df = df.join(pd.Series(y_pred_proba[:, 1], name=f"probability_{label}"))

    df = df.set_index("index")

    test_labelled_gbm = df.join(detn[["pid"]])
    return test_labelled_gbm


def plot_corr_jitter(detn, variable, label, x_jitter):
    import matplotlib.pyplot as plt
    import seaborn as sns

    null_ct = detn[variable].isnull().sum()
    # Calculate correlation
    corr = detn[[variable, label]].corr()
    if detn[label].nunique() == 2:
        sns.regplot(
            x=variable,
            y=label,
            data=detn,
            logistic=True,
            y_jitter=0.1,
            x_jitter=x_jitter,
            line_kws={"color": "red"},
            scatter_kws={"s": 2},
        )
    else:
        sns.regplot(
            x=variable,
            y=label,
            data=detn,
            logistic=False,
            y_jitter=0.0,
            x_jitter=0.0,
            line_kws={"color": "red"},
            scatter_kws={"s": 2},
        )

    plt.text(
        0.02, 0.3, f"mean {variable} is {detn[variable].mean():.2f}", transform=plt.gca().transAxes
    )
    plt.text(
        0.02,
        0.7,
        f"Correlation of {variable} to {label}:\n{corr[variable][label]:.2f}",
        transform=plt.gca().transAxes,
    )
    if detn[variable].nunique() < 5:
        plt.text(
            0.1,
            0.5,
            f"mean {label} is:\n{detn[[variable,label]].groupby(variable).mean().round(3)}",
            transform=plt.gca().transAxes,
        )

    plt.text(0.02, 0.2, f"Null ct: {variable} {null_ct}", transform=plt.gca().transAxes)

    plt.show()
