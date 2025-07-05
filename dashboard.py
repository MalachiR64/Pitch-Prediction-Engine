import streamlit as st
import requests
from datetime import datetime
import random
import json
import pandas as pd
from kafka import KafkaProducer, KafkaConsumer 
from apitester import extract_pitch_sequences 
import uuid

#kafka information 
KAFKA_TOPIC_PITCHES = 'raw-pitches'
KAFKA_TOPIC_RESULTS = 'pitch-predictions'
KAFKA_SERVER = 'localhost:9092'

KAFKA_TOPIC_RESULTS = 'pitch-predictions'

pitch_map = {
    'FF': 'Fastball', 
    'FA': 'Fastball', 
    'FS': 'Fastball', 
    'SI': 'Fastball',
    'FC': 'Fastball',
    'CH': 'Changeup', 
    'CS': 'Changeup',
    'SL': 'Slider',
    'SV': 'Slider', 
    'ST': 'Slider',
    'CU': 'Curve', 
    'KC': 'Curve' ,
    'KN': 'Knuckleball',  
    'EP': 'Eephus',
    'FO': 'Forkball', 
    'SC': 'Screwball',
    "No description available." : "No description available."
}

def get_latest_prediction(timeout_sec=2):
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC_RESULTS,
            bootstrap_servers=KAFKA_SERVER,
            auto_offset_reset='earliest',  # Start at end of topic
            enable_auto_commit=False,    # Do NOT commit offsets
            group_id=None,               # Start fresh each time
            consumer_timeout_ms=timeout_sec * 1000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

        # Wait for the newest prediction
        predictions = list(consumer)
        if predictions:
            return predictions[-1].value

    except Exception as e:
        st.error(f"Prediction error: {str(e)}")

    return None



producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
#create a kafka consumer function


@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_live_mlb_games():
    """Fetch current live MLB games"""
    today = datetime.now().strftime('%Y-%m-%d')
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={today}"

    try:
        response = requests.get(url)
        data = response.json()

        live_games = []
        for date in data.get('dates', []):
            for game in date.get('games', []):
                if game.get('status', {}).get('abstractGameState') == 'Live':
                    live_games.append({
                        'gamePk': game['gamePk'],
                        'away_team': game['teams']['away']['team']['name'],
                        'home_team': game['teams']['home']['team']['name'],
                        'status': game['status']['detailedState'],
                        'type': 'Live'
                    })
        return live_games
    except Exception as e:
        st.error(f"Error fetching live games: {str(e)}")
        return []

@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_historical_mlb_games(num_games=5):
    """Fetch random historical MLB games from 2024"""
    url = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&startDate=2024-01-01&endDate=2024-12-31"

    try:
        response = requests.get(url)
        data = response.json()

        historical_games = []
        for date in data.get('dates', []):
            for game in date.get('games', []):
                if game.get('status', {}).get('abstractGameState') == 'Final':
                    historical_games.append({
                        'gamePk': game['gamePk'],
                        'away_team': game['teams']['away']['team']['name'],
                        'home_team': game['teams']['home']['team']['name'],
                        'status': 'Final',
                        'type': 'Historical',
                        'date': date.get('date', '')
                    })
        
        # Return random sample
        return random.sample(historical_games, min(num_games, len(historical_games)))
    except Exception as e:
        st.error(f"Error fetching historical games: {str(e)}")
        return []

def display_pitch_info(pitch_data, current_index):
    """Compact UI for displaying pitch information"""
    if 0 <= current_index < len(pitch_data):
        pitch = pitch_data[current_index]

        # Game Situation + Score
        game1, game2, game3, game4 = st.columns(4)
        with game1:
            st.markdown("**Inning**")
            st.write(f"{'Top' if pitch['half_inning']=='top' else 'Bottom'} {pitch['inning']}")
        with game2:
            st.markdown("**Count**")
            st.write(f"{pitch['balls']}-{pitch['strikes']}")
        with game3:
            st.markdown("**Outs**")
            st.write(pitch['outs'])
        with game4:
            st.markdown("**Score**")
            st.write(f"{pitch['home_team']} {pitch['home_score']} - {pitch['away_team']} {pitch['away_score']}")

        # Player Info
        p1, p2, p3, p4 = st.columns(4)
        with p1:
            st.markdown("**Batter**")
            st.write(pitch['batter_name'])
        with p2:
            st.markdown("**Batter Side**")
            st.write(pitch['batter_side'])
        with p3:
            st.markdown("**Pitcher**")
            st.write(pitch['pitcher_name'])
        with p4:
            st.markdown("**Pitcher Hand**")
            st.write(pitch['pitcher_hand'])

        # Base Runners
        b1, b2, b3 = st.columns(3)
        with b1:
            st.markdown("**1B**")
            st.write("🟡" if pitch['runners_on_first'] else "⚪")
        with b2:
            st.markdown("**2B**")
            st.write("🟡" if pitch['runners_on_second'] else "⚪")
        with b3:
            st.markdown("**3B**")
            st.write("🟡" if pitch['runners_on_third'] else "⚪")

        # Pitch Result
        st.markdown("### Play Result")
        st.write(pitch.get('pitch_result', "No description available."))
        st.markdown("### Actual Pitch Result")
        pitch_type_str = pitch.get('pitch_type', "No description available.")
        st.write(pitch_map.get(pitch_type_str, "Unknown Pitch Type"))

    # ML Prediction
    if 'latest_prediction' in st.session_state:
        pred = st.session_state.latest_prediction.get("predicted_pitch_type", "N/A")
        st.markdown("### Predicted Pitch Type")
        st.write(pred)


def main():

    st.set_page_config(
        page_title="MLB Pitcher Data Grabber",
        page_icon="⚾",
        layout="wide"
    )
    
    # Initialize session state variables
    if 'pitch_data' not in st.session_state:
        st.session_state.pitch_data = None
    if 'current_pitch_index' not in st.session_state:
        st.session_state.current_pitch_index = 0
    if 'game_loaded' not in st.session_state:
        st.session_state.game_loaded = False

    # SIDEBAR - Game Selection and Navigation
    with st.sidebar:
        st.header("Game Selection")
        
        # Fetch games
        with st.spinner("Loading games..."):
            current_games = get_live_mlb_games()
            historical_games = get_historical_mlb_games(5)
            all_games = current_games + historical_games
        
        if not all_games:
            st.warning("No games found. Please try again later.")
            return
        
        # Display games in a more interactive way
        game_options = []
        game_data = {}
        
        for i, game in enumerate(all_games):
            game_label = f"{game['away_team']} @ {game['home_team']} ({game['type']})"
            if game['type'] == 'Historical' and 'date' in game:
                game_label += f" - {game['date']}"
            game_options.append(game_label)
            game_data[game_label] = game
        
        selected_game_label = st.selectbox(
            "Select a game:",
            game_options,
            help="Choose from live games or historical games from 2024"
        )
        
        if selected_game_label:
            selected_game = game_data[selected_game_label]
            
            # Display game info
            st.info(f"**{selected_game['away_team']}** @ **{selected_game['home_team']}**\n\n*{selected_game['status']}*")
            
            # Load data button
            if st.button("Get Pitch Data", type="primary", use_container_width=True):
                with st.spinner("Loading pitch data..."):
                    pitch_data = extract_pitch_sequences(selected_game['gamePk'])
                    if pitch_data:
                        st.session_state.pitch_data = pitch_data
                        st.session_state.game_loaded = True
                        st.session_state.current_pitch_index = 0
                        # Send first pitch
                        first_pitch = pitch_data[0]
                        producer.send(KAFKA_TOPIC_PITCHES, value=first_pitch)
                        producer.flush()
                        st.success(f"Retrieved {len(pitch_data)} pitches!")
                        with st.spinner("Predicting next pitch..."):
                            prediction = get_latest_prediction()
                            if prediction:
                                st.session_state.latest_prediction = prediction
                        st.rerun()

        # Navigation Section (only show if game is loaded)
        if st.session_state.game_loaded and st.session_state.pitch_data:
            st.divider()
            st.header("📊 Pitch Navigation")
            
            # Show current pitch info
            total_pitches = len(st.session_state.pitch_data)
            current_pitch = st.session_state.current_pitch_index + 1
            st.metric("Current Pitch", f"{current_pitch} of {total_pitches}")
            
            # Navigation buttons
            col1 = st.columns(1)

            with col1[0]:
                if st.button("Next ⏭️", disabled=(st.session_state.current_pitch_index >= len(st.session_state.pitch_data) - 1), use_container_width=True):
                    st.session_state.current_pitch_index += 1
                    next_pitch = st.session_state.pitch_data[st.session_state.current_pitch_index]
                    producer.send(KAFKA_TOPIC_PITCHES, value=next_pitch)
                    producer.flush()

                    with st.spinner("Predicting next pitch..."):
                        prediction = get_latest_prediction()
                        if prediction:
                            st.session_state.latest_prediction = prediction
                    st.rerun()
            
            # Clear data section
            st.divider()
            st.header("🗑️ Reset")
            if st.button("Clear Data & Select New Game", type="secondary", use_container_width=True):
                st.session_state.pitch_data = None
                st.session_state.game_loaded = False
                st.session_state.current_pitch_index = 0
                st.rerun()

    # MAIN CONTENT - Pitch Information Display
    st.markdown("## ⚾ MLB Pitcher Prediction Engine")
    st.markdown("Pick a game and the engine will try to predict the next pitch based on data from the 2024 MLB Season.")
    
    # Main pitch display area
    if st.session_state.game_loaded and st.session_state.pitch_data:
        display_pitch_info(st.session_state.pitch_data, st.session_state.current_pitch_index)
    else:
        # Welcome message when no game is loaded
        st.info("👈 Select a game from the sidebar to get started!")
        
        # Show some helpful information
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🔴 Live Games")
            st.write("Experience real-time baseball action with current MLB games in progress.")
            
        with col2:
            st.subheader("📚 Historical Games")
            st.write("Explore completed games from the 2024 MLB season to analyze pitch patterns and strategies.")

if __name__ == "__main__":
    main()