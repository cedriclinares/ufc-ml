import pandas as pd
import numpy as np
import pickle
# from sklearnex import patch_sklearn
# patch_sklearn(global_patch=True)
# import sklearn
from sklearn.neural_network import MLPClassifier
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import cross_val_score


df = pd.read_csv("/Users/cedriclinares/Documents/ufc-ml/final-dataset-names-and-ids.csv")

df["date"] = pd.to_datetime(df["date"])
df = df.sort_values("date").reset_index(drop=True)

# Compute rest time strictly from earlier fights.  Same-day bouts are kept in
# the same history window, so they do not count as a previous fight.
for corner in ("r", "b"):
    identity = f"{corner}_id" if f"{corner}_id" in df else f"{corner}_name"
    previous_by_date = {}
    for fighter, dates in df.groupby(identity, dropna=False)["date"]:
        unique_dates = sorted(dates.dropna().unique())
        previous_by_date[fighter] = {
            current: (unique_dates[index - 1] if index else pd.NaT)
            for index, current in enumerate(unique_dates)
        }
    previous_date = pd.Series(
        [previous_by_date.get(fighter, {}).get(day, pd.NaT)
         for fighter, day in zip(df[identity], df["date"])],
        index=df.index,
    )
    valid_previous = previous_date.notna()
    df[f"{corner}_days_since_last_fight"] = (
        (df["date"] - previous_date).dt.days.where(valid_previous)
    )
    df[f"{corner}_is_debut"] = (~valid_previous).astype(float)

feature_cols = ["weight","gender","r_age","r_wins","r_losses","r_draws","r_stance","r_height","r_reach","r_opponent_wins","r_opponent_loses","r_championship_fights","r_sig_str_landed","r_sig_str_attempted","r_sig_str_absorbed","r_sig_str_evaded","r_head_landed","r_head_attempted","r_head_absorbed","r_head_evaded","r_body_landed","r_body_attempted","r_body_absorbed","r_body_evaded","r_legs_landed","r_legs_attempted","r_legs_absorbed","r_legs_evaded","r_distance_landed","r_distance_attempted","r_distance_absorbed","r_distance_evaded","r_clinch_landed","r_clinch_attempted","r_clinch_absorbed","r_clinch_evaded","r_ground_landed","r_ground_attempted","r_ground_absorbed","r_ground_evaded","r_total_str_landed","r_total_str_attempted","r_total_str_absorbed","r_total_str_evaded","r_td_landed","r_td_attempted","r_td_absorbed","r_td_evaded","r_kd_landed","r_kd_absorbed","r_subs_attempted","r_subs_evaded","r_ctrl_time","r_opponent_ctrl_time","r_round_1_sig_str_landed","r_round_1_sig_str_attempted","r_round_1_sig_str_absorbed","r_round_1_sig_str_evaded","r_round_2_sig_str_landed","r_round_2_sig_str_attempted","r_round_2_sig_str_absorbed","r_round_2_sig_str_evaded","r_round_3_sig_str_landed","r_round_3_sig_str_attempted","r_round_3_sig_str_absorbed","r_round_3_sig_str_evaded","r_round_4_sig_str_landed","r_round_4_sig_str_attempted","r_round_4_sig_str_absorbed","r_round_4_sig_str_evaded","r_round_5_sig_str_landed","r_round_5_sig_str_attempted","r_round_5_sig_str_absorbed","r_round_5_sig_str_evaded","r_fight_time","r_opponent_fight_time","r_reversals","b_age","b_wins","b_losses","b_draws","b_stance","b_height","b_reach","b_opponent_wins","b_opponent_loses","b_championship_fights","b_sig_str_landed","b_sig_str_attempted","b_sig_str_absorbed","b_sig_str_evaded","b_head_landed","b_head_attempted","b_head_absorbed","b_head_evaded","b_body_landed","b_body_attempted","b_body_absorbed","b_body_evaded","b_legs_landed","b_legs_attempted","b_legs_absorbed","b_legs_evaded","b_distance_landed","b_distance_attempted","b_distance_absorbed","b_distance_evaded","b_clinch_landed","b_clinch_attempted","b_clinch_absorbed","b_clinch_evaded","b_ground_landed","b_ground_attempted","b_ground_absorbed","b_ground_evaded","b_total_str_landed","b_total_str_attempted","b_total_str_absorbed","b_total_str_evaded","b_td_landed","b_td_attempted","b_td_absorbed","b_td_evaded","b_kd_landed","b_kd_absorbed","b_subs_attempted","b_subs_evaded","b_ctrl_time","b_opponent_ctrl_time","b_round_1_sig_str_landed","b_round_1_sig_str_attempted","b_round_1_sig_str_absorbed","b_round_1_sig_str_evaded","b_round_2_sig_str_landed","b_round_2_sig_str_attempted","b_round_2_sig_str_absorbed","b_round_2_sig_str_evaded","b_round_3_sig_str_landed","b_round_3_sig_str_attempted","b_round_3_sig_str_absorbed","b_round_3_sig_str_evaded","b_round_4_sig_str_landed","b_round_4_sig_str_attempted","b_round_4_sig_str_absorbed","b_round_4_sig_str_evaded","b_round_5_sig_str_landed","b_round_5_sig_str_attempted","b_round_5_sig_str_absorbed","b_round_5_sig_str_evaded","b_fight_time","b_opponent_fight_time","b_reversals"]
# Matchup features are exported from the red/blue total-stat rows. Difference
# columns are red-minus-blue; ratios are retained for both fighters.
ratio_feature_names = [
    "sig_str_accuracy", "total_str_accuracy", "td_accuracy",
    "head_accuracy", "body_accuracy", "legs_accuracy",
    "distance_accuracy", "clinch_accuracy", "ground_accuracy",
]
difference_feature_names = [
    "wins_diff", "losses_diff", "draws_diff", "age_diff", "height_diff",
    "reach_diff", "sig_str_landed_diff", "sig_str_attempted_diff",
    "total_str_landed_diff", "total_str_attempted_diff", "td_landed_diff",
    "td_attempted_diff", "kd_landed_diff", "ctrl_time_diff", "fight_time_diff",
]
feature_cols += [f"r_{name}" for name in ratio_feature_names]
feature_cols += [f"b_{name}" for name in ratio_feature_names]
feature_cols += [f"r_{name}" for name in difference_feature_names]
feature_cols += ["r_days_since_last_fight", "b_days_since_last_fight",
                 "r_is_debut", "b_is_debut"]

# Older CSV exports do not contain the derived columns yet. Calculate them
# from the pre-fight red/blue columns so the model remains runnable; new
# exports from training_fight_totals already contain the same values.
for corner in ("r", "b"):
    for name in ratio_feature_names:
        if f"{corner}_{name}" not in df:
            category = name.removesuffix("_accuracy")
            df[f"{corner}_{name}"] = (
                df[f"{corner}_{category}_landed"] /
                df[f"{corner}_{category}_attempted"].replace(0, np.nan)
            )
for name in difference_feature_names:
    if f"r_{name}" in df:
        continue
    base = name[:-5]
    df[f"r_{name}"] = df[f"r_{base}"] - df[f"b_{base}"]

for corner in ("r", "b"):
    if f"{corner}_is_debut" not in df:
        df[f"{corner}_is_debut"] = (
            df[[f"{corner}_wins", f"{corner}_losses", f"{corner}_draws"]]
            .fillna(0).sum(axis=1).eq(0).astype(float)
        )
    if f"{corner}_days_since_last_fight" not in df:
        # The interval requires fighter identity and must come from the
        # training_fight_totals export; leave it missing rather than inventing
        # a value from unrelated rows.
        df[f"{corner}_days_since_last_fight"] = np.nan

data = df.loc[:, feature_cols].copy()
categorical_cols = ["gender", "r_stance", "b_stance"]

print(df.b_stance.value_counts())
# print("dummy length", data.values)
# for col in data_dummies.values:
#    print(col[157])

# print("data: {}".format(data.shape))
# print("dummies: {}".format(data_dummies.shape))
# print("columns: ", list(data_dummies.columns))

winner_map = {"Red": 0, "Blue": 1}
raw_winners = df["winner"].copy()
df["winner"] = raw_winners.map(winner_map)
if df["winner"].isna().any():
    invalid_winners = raw_winners[df["winner"].isna()].unique().tolist()
    raise ValueError(f"Unexpected or missing winner labels: {invalid_winners}")
df["winner"] = df["winner"].astype(np.int64)
y = df["winner"]
# print(X.shape)
# print(y.shape)
# print(y.head())

train_size = int(len(data) * 0.8)
train_data = data.iloc[:train_size].copy()
test_data = data.iloc[train_size:].copy()
for column in categorical_cols:
    train_data[column] = train_data[column].fillna("Unknown")
    test_data[column] = test_data[column].fillna("Unknown")

train_data = pd.get_dummies(train_data, columns=categorical_cols, dtype=float)
test_data = pd.get_dummies(
    test_data, columns=categorical_cols, dtype=float
).reindex(columns=train_data.columns, fill_value=0.0)

X_train = train_data.to_numpy(dtype=float)
X_test = test_data.to_numpy(dtype=float)
y_train = y.iloc[:train_size]
y_test = y.iloc[train_size:]
train_index = np.arange(train_size)
test_index = np.arange(train_size, len(data))
print("X train:", X_train.shape)
# print("X train:", X_train[:5])

# Impute missing values from training-period medians only. This keeps the
# later test period from influencing preprocessing and makes the matrix
# acceptable to MLPClassifier.
train_medians = np.zeros(X_train.shape[1], dtype=float)
has_observed_value = np.any(~np.isnan(X_train), axis=0)
train_medians[has_observed_value] = np.nanmedian(
    X_train[:, has_observed_value], axis=0
)
train_missing = np.isnan(X_train)
test_missing = np.isnan(X_test)
X_train[train_missing] = train_medians[np.where(train_missing)[1]]
X_test[test_missing] = train_medians[np.where(test_missing)[1]]

mean_on_train = X_train.mean(axis=0)
std_on_train = X_train.std(axis=0)
std_on_train[std_on_train == 0] = 1.0
# print(mean_on_train[157], std_on_train[157], train_data.columns[157])

print("mean", mean_on_train.shape)
print("std", std_on_train.shape)

X_train_scaled = (X_train - mean_on_train) / std_on_train
X_test_scaled = (X_test - mean_on_train) / std_on_train
print("X train scaled:", X_train_scaled.shape)

# print("X train:", X_train_scaled[:5])
'''
param_grid = {'solver': ['lbfgs', 'adam'], 'max_iter': [100, 1000, 2000], 'hidden_layer_sizes': [[10, 10], [100], [100, 100]], 'alpha': [.0001, .001, .1, 1]}
grid_search = GridSearchCV(MLPClassifier(random_state=0), param_grid, cv=5, n_jobs=-1)
grid_search.fit(X_train_scaled, y_train)

print("Test set score: {:.2f}".format(grid_search.score(X_test_scaled, y_test)))
print("Best parameters: {}".format(grid_search.best_params_))
print("Best cross-validation score: {:.2f}".format(grid_search.best_score_))
print("Best estimator:\n{}".format(grid_search.best_estimator_))
'''

print("NaN values:", np.isnan(X_train_scaled).sum())
print("Infinite values:", np.isinf(X_train_scaled).sum())

mlp = MLPClassifier(random_state=0, solver='adam', max_iter=1000, alpha=1, hidden_layer_sizes=[100])
mlp.fit(X_train_scaled, y_train)

# with config_context(target_offload="gpu:0"):
filename = './models/neural_nets/mlps-2026-09-22.sav'
with open(filename, 'wb') as model_file:
    pickle.dump(mlp, model_file)

# gbrt = pickle.load(open('models/gradient_boosted_classifier-2023-09-02.sav', 'rb'))
print("Accuracy on training set: {:.3f}".format(mlp.score(X_train_scaled, y_train)))
print("Accuracy on test set: {:.3f}".format(mlp.score(X_test_scaled, y_test)))

predicted_classes = mlp.predict(X_test_scaled)
predicted_probabilities = mlp.predict_proba(X_test_scaled)
betting_units_won = 0
correct = 0
incorrect = 0

confusion = confusion_matrix(y_test, predicted_classes)
print("Confusion matrix: \n", confusion)
print(classification_report(y_test, predicted_classes))

for i in range(0, len(test_index)):
    original_test_data_row = df.iloc[test_index[i]]
    # print(original_test_data_row)
    # print(predicted_classes[i])
    if original_test_data_row.winner == predicted_classes[i]:
        correct += 1
        if original_test_data_row["winner"] == 0:
            if original_test_data_row["r_fighter_odds"] > 0:
                betting_units_won += original_test_data_row["r_fighter_odds"]/100 # underdog
            else: 
                betting_units_won += abs(100/original_test_data_row["r_fighter_odds"]) # favorite
        elif original_test_data_row["winner"] == 1: 
            if original_test_data_row["b_fighter_odds"] > 0:
                betting_units_won += original_test_data_row["b_fighter_odds"]/100
            else: 
                betting_units_won += abs(100/original_test_data_row["b_fighter_odds"])
    else:
        incorrect += 1
        betting_units_won -= 1
        
print("betting_units_won: ", betting_units_won)
print("correct: ", correct)
print("incorrect: ", incorrect)

# columns = list(data_dummies.columns)
# for i in range(0, len(columns)):
#    print(columns[i],': ', gbrt.feature_importances_[i]) 
