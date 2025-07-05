
import joblib
import pandas as pd
import time

# Load model and features
model = joblib.load("xgb_pitch_model.joblib")
encoder = joblib.load("pitch_label_encoder.joblib")
feature_names = joblib.load("model_features.joblib")
# Create dummy input row with typical values
sample_input = {
    'outs': 1,
    'inning': 3,
    'batter_id': 671739,
    'pitcher_id': 663623,
    'runners_on_first': 1,
    'runners_on_second': 0,
    'runners_on_third': 0,
    'balls': 2,
    'strikes': 1,
    'home_score': 1,
    'away_score': 2,
    'half_inning_bottom': 0,
    'half_inning_top': 1,
    'batter_side_L': 1,
    'batter_side_R': 0,
    'pitcher_hand_L': 0,
    'pitcher_hand_R': 1
}

# Ensure all expected features are present
sample_input = {key: sample_input.get(key, 0) for key in feature_names}
X = pd.DataFrame([sample_input])

# Predict and decode
predicted_class_index = model.predict(X)[0]
predicted_label = encoder.inverse_transform([predicted_class_index])[0]

print("Predicted class index:", predicted_class_index)
print("Predicted pitch label:", predicted_label)
print("")