import pandas as pd
import pickle
import numpy as np
# from sklearnex import patch_sklearn
# patch_sklearn(global_patch=True)
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
from sklearn.model_selection import train_test_split


df = pd.read_csv("/Users/cedriclinares/Documents/ufc-ml/datasets/ufc-dataset-2023-12-16.csv")

feature_cols = ["weight","gender","r_age","r_wins","r_losses","r_draws","r_stance","r_height","r_reach","r_opponent_wins","r_opponent_loses","r_championship_fights","r_sig_str_landed","r_sig_str_attempted","r_sig_str_absorbed","r_sig_str_evaded","r_head_landed","r_head_attempted","r_head_absorbed","r_head_evaded","r_body_landed","r_body_attempted","r_body_absorbed","r_body_evaded","r_legs_landed","r_legs_attempted","r_legs_absorbed","r_legs_evaded","r_distance_landed","r_distance_attempted","r_distance_absorbed","r_distance_evaded","r_clinch_landed","r_clinch_attempted","r_clinch_absorbed","r_clinch_evaded","r_ground_landed","r_ground_attempted","r_ground_absorbed","r_ground_evaded","r_total_str_landed","r_total_str_attempted","r_total_str_absorbed","r_total_str_evaded","r_td_landed","r_td_attempted","r_td_absorbed","r_td_evaded","r_kd_landed","r_kd_absorbed","r_subs_attempted","r_subs_evaded","r_ctrl_time","r_opponent_ctrl_time","r_round_1_sig_str_landed","r_round_1_sig_str_attempted","r_round_1_sig_str_absorbed","r_round_1_sig_str_evaded","r_round_2_sig_str_landed","r_round_2_sig_str_attempted","r_round_2_sig_str_absorbed","r_round_2_sig_str_evaded","r_round_3_sig_str_landed","r_round_3_sig_str_attempted","r_round_3_sig_str_absorbed","r_round_3_sig_str_evaded","r_round_4_sig_str_landed","r_round_4_sig_str_attempted","r_round_4_sig_str_absorbed","r_round_4_sig_str_evaded","r_round_5_sig_str_landed","r_round_5_sig_str_attempted","r_round_5_sig_str_absorbed","r_round_5_sig_str_evaded","r_fight_time","r_opponent_fight_time","r_reversals","b_age","b_wins","b_losses","b_draws","b_stance","b_height","b_reach","b_opponent_wins","b_opponent_loses","b_championship_fights","b_sig_str_landed","b_sig_str_attempted","b_sig_str_absorbed","b_sig_str_evaded","b_head_landed","b_head_attempted","b_head_absorbed","b_head_evaded","b_body_landed","b_body_attempted","b_body_absorbed","b_body_evaded","b_legs_landed","b_legs_attempted","b_legs_absorbed","b_legs_evaded","b_distance_landed","b_distance_attempted","b_distance_absorbed","b_distance_evaded","b_clinch_landed","b_clinch_attempted","b_clinch_absorbed","b_clinch_evaded","b_ground_landed","b_ground_attempted","b_ground_absorbed","b_ground_evaded","b_total_str_landed","b_total_str_attempted","b_total_str_absorbed","b_total_str_evaded","b_td_landed","b_td_attempted","b_td_absorbed","b_td_evaded","b_kd_landed","b_kd_absorbed","b_subs_attempted","b_subs_evaded","b_ctrl_time","b_opponent_ctrl_time","b_round_1_sig_str_landed","b_round_1_sig_str_attempted","b_round_1_sig_str_absorbed","b_round_1_sig_str_evaded","b_round_2_sig_str_landed","b_round_2_sig_str_attempted","b_round_2_sig_str_absorbed","b_round_2_sig_str_evaded","b_round_3_sig_str_landed","b_round_3_sig_str_attempted","b_round_3_sig_str_absorbed","b_round_3_sig_str_evaded","b_round_4_sig_str_landed","b_round_4_sig_str_attempted","b_round_4_sig_str_absorbed","b_round_4_sig_str_evaded","b_round_5_sig_str_landed","b_round_5_sig_str_attempted","b_round_5_sig_str_absorbed","b_round_5_sig_str_evaded","b_fight_time","b_opponent_fight_time","b_reversals"]
print("feature_cols lenght: ", len(feature_cols))

data = df.loc[:, feature_cols]

# print(df.winner.value_counts())
data_dummies = pd.get_dummies(data)
# print("dummy length", data_dummies.columns)
# for col in data_dummies.columns:
#    print(col)

# print("data: {}".format(data.shape))
# print("dummies: {}".format(data_dummies.shape))
# print("columns: ", list(data_dummies.columns))

df['winner'] = df['winner'].replace({'Red': 0, 'Blue': 1})

X = data_dummies.values
y = df.winner
print(X.shape)
# print(y.shape)
# print(y.head())
X_train, X_test, y_train, y_test, train_index, test_index = train_test_split(X, y, np.arange(len(X)), random_state=0)

# print(X_train.shape)
# print(X_test.shape)

gbrt = GradientBoostingClassifier(random_state=0, n_estimators=1000, max_depth=3, learning_rate=0.01)
# with config_context(target_offload="gpu:0"):
gbrt.fit(X_train, y_train)
filename = './models/gradient_boosted_classifier' + '-2023-12-16' + '.sav'
pickle.dump(gbrt, open(filename, 'wb'))
# print("gbrt n_estimators: 1000, max_depth: 3, learning_rate: 0.01")


# gbrt = pickle.load(open('models/trained/best/gradient_boosted_classifier-cross-val-grid.sav', 'rb'))

print("Accuracy on training set: {:.3f}".format(gbrt.score(X_train, y_train)))
print("Accuracy on test set: {:.3f}".format(gbrt.score(X_test, y_test)))

predicted_classes = gbrt.predict(X_test)
predicted_probabilities = gbrt.predict_proba(X_test)
betting_units_won = 0
correct = 0
incorrect = 0

confusion = confusion_matrix(y_test, predicted_classes)
print("Confusion matrix: \n", confusion)
print(classification_report(y_test, predicted_classes))
print(test_index)
print(len(test_index))
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
