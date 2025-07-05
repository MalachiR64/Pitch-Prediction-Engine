import json
import joblib
import pandas as pd
from kafka import KafkaProducer, KafkaConsumer

KAFKA_TOPIC_PITCHES = 'raw-pitches'
KAFKA_TOPIC_RESULTS = 'pitch-predictions'
KAFKA_SERVER = 'localhost:9092'

# Load model, encoder, and training feature names
model = joblib.load('xgb_pitch_model.joblib')
encoder = joblib.load('pitch_label_encoder.joblib')
feature_names = joblib.load('model_features.joblib')  # <-- NEW

def preprocess(pitch):
    """Return a DataFrame matching the trained model's one-hot encoded features"""
    # Base one-hot representation
    base = {
        'inning': pitch['inning'],
        'outs': pitch['outs'],
        'balls': pitch['balls'],
        'strikes': pitch['strikes'],
        'batter_id': pitch['batter_id'],
        'pitcher_id': pitch['pitcher_id'],
        'home_score': pitch['home_score'],
        'away_score': pitch['away_score'],
        'runners_on_first': pitch.get('runners_on_first', 0),
        'runners_on_second': pitch.get('runners_on_second', 0),
        'runners_on_third': pitch.get('runners_on_third', 0),

        # One-hot for half_inning
        'half_inning_top': 1 if pitch['half_inning'] == 'top' else 0,
        'half_inning_bottom': 1 if pitch['half_inning'] == 'bottom' else 0,

        # One-hot for batter_side
        'batter_side_L': 1 if pitch['batter_side'] == 'L' else 0,
        'batter_side_R': 1 if pitch['batter_side'] == 'R' else 0,

        # One-hot for pitcher_hand
        'pitcher_hand_R': 1 if pitch['pitcher_hand'] == 'R' else 0,
        'pitcher_hand_L': 1 if pitch['pitcher_hand'] == 'L' else 0,
    }

    # Fill any missing expected features with 0
    all_features = {col: base.get(col, 0) for col in feature_names}
    return pd.DataFrame([all_features])

if __name__ == '__main__':
    consumer = KafkaConsumer(
        KAFKA_TOPIC_PITCHES,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset='earliest'
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for msg in consumer:
        pitch_data = json.loads(msg.value.decode('utf-8'))
        try:
            X = preprocess(pitch_data)
            pred_int = model.predict(X)[0]
            pred_label = encoder.inverse_transform([pred_int])[0]

            result = {
                'predicted_pitch_type': pred_label,
                'pitch_id': pitch_data.get('pitch_count', None),
                'batter_id': pitch_data.get('batter_id'),
                'pitcher_id': pitch_data.get('pitcher_id')
            }

            producer.send(KAFKA_TOPIC_RESULTS, value=result)
            producer.flush()
            print("Produced:", result)

        except Exception as e:
            print("Prediction error:", str(e))
