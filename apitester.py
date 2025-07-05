

import statsapi
import pandas as pd
import json
import requests
import random
from datetime import datetime

def extract_pitch_sequences(game_pk):
    """Extract detailed pitch-by-pitch data with sequential count information"""
    meta = statsapi.get('game', {'gamePk': game_pk})
    home_team_name = meta['gameData']['teams']['home']['name']
    away_team_name = meta['gameData']['teams']['away']['name']    
    pbp = statsapi.get('game_playByPlay', {'gamePk': game_pk})
    pitch_data = []
    pitch_count = 1

    # Track outs throughout the game - reset at start of each half-inning

    current_outs = 0
    home_team_score = 0
    away_team_score = 0
    previous_inning = None
    previous_half = None


    for play in pbp['allPlays']:
        # Get matchup info
        batter_id = play['matchup']['batter']['id']
        batter_name = play['matchup']['batter']['fullName']
        pitcher_id = play['matchup']['pitcher']['id']
        pitcher_name = play['matchup']['pitcher']['fullName']
        batter_side = play['matchup']['batSide']['code']  # L/R/S
        pitcher_hand = play['matchup']['pitchHand']['code']  # L/R
        
        # Get situational data
        inning = play['about']['inning']
        half_inning = play['about']['halfInning']  # 'top' or 'bottom'
        
        # Reset outs at the start of each half-inning
        if previous_inning != inning or previous_half != half_inning:
            current_outs = 0
            previous_inning = inning
            previous_half = half_inning


        # Track pitch sequence within this at-bat
        pitch_in_ab = 0
        
        count_b=0
        count_s=0

        # Store the outs at the START of this at-bat
        outs_when_ab_started = current_outs
        for event in play['playEvents']:
            is_runner_on_first = 0
            if play['matchup'].get('postOnFirst', False):
                is_runner_on_first = 1
            is_runner_on_second = 0
            if play['matchup'].get('postOnSecond', False):
                is_runner_on_second = 1
            is_runner_on_third = 0
            if play['matchup'].get('postOnThird', False):
                is_runner_on_third = 1


            if event['type'] == 'pitch':
                pitch_in_ab += 1
                
                # Extract pitch details
                pitch_type = event['details'].get('type', {}).get('code', None)
                pitch_description = event['details'].get('type', {}).get('description', None)
                

                #is_in_play = event['details'].get('isInPlay', False),
                #is_strike = event['details'].get('isStrike', False),
                #is_ball = event['details'].get('isBall', False)


                
                # Extract pitch result
                pitch_call = event['details']['call']['code']  # B, S, X, etc.
                pitch_result = event['details']['call']['description']
                

                
                # Extract velocity and location if available
                velocity = None
                strike_zone_x = None
                strike_zone_z = None
                spin_rate = None
                
                if 'pitchData' in event:
                    pitch_data_obj = event['pitchData']
                    
                    if 'startSpeed' in pitch_data_obj:
                        velocity = pitch_data_obj['startSpeed']
                    
                    if 'coordinates' in pitch_data_obj:
                        coords = pitch_data_obj['coordinates']
                        if 'pX' in coords:
                            strike_zone_x = coords['pX']
                        if 'pZ' in coords:
                            strike_zone_z = coords['pZ']
                    
                    if 'breaks' in pitch_data_obj and 'spinRate' in pitch_data_obj['breaks']:
                        spin_rate = pitch_data_obj['breaks']['spinRate']
                                
                pitch_data.append({
                    'pitch_count': pitch_count,
                    'outs': outs_when_ab_started,
                    'inning': inning,
                    'half_inning': half_inning,
                    'batter_id': batter_id,
                    'batter_name': batter_name,
                    'pitcher_id': pitcher_id,
                    'pitcher_name': pitcher_name,
                    'batter_side': batter_side,
                    'pitcher_hand': pitcher_hand,
                    'runners_on_first': is_runner_on_first,
                    'runners_on_second': is_runner_on_second,
                    'runners_on_third': is_runner_on_third,
                    'balls': count_b,
                    'strikes': count_s,
                    'pitch_type': pitch_type,
                    'pitch_description': pitch_description,
                    'pitch_call': pitch_call,
                    'pitch_result': pitch_result,
                    'velocity': velocity,
                    'spin_rate': spin_rate,
                    'strike_zone_x': strike_zone_x,
                    'strike_zone_z': strike_zone_z,
                    'home_team': home_team_name,
                    'away_team': away_team_name,
                    'home_score': home_team_score,
                    'away_score': away_team_score,
                })
                # Update counts
                pitch_count += 1
                # Calculate count AFTER this pitch
                if pitch_call == 'B' or pitch_call == 'I':  # Ball or Intentional Ball
                    count_b += 1
                elif pitch_call in ['S', 'C', 'T', 'F', 'L', 'M', 'Q', 'R']:  # Strike variants
                    count_s= min(count_s + 1, 2)  # Limit to 2 strikes
                home_team_score = play['result'].get('homeScore', None)
                away_team_score = play['result'].get('awayScore', None)
        # After processing all pitches in this at-bat, update outs based on the play result
        # Check if this play resulted in any outs
        if 'result' in play and 'event' in play['result']:
            play_result = play['result']['event']
            
            # Count outs from common play results
            if play_result in ['Strikeout', 'Groundout', 'Flyout', 'Pop Out', 'Lineout', 'Forceout']:
                current_outs += 1
            elif play_result in ['Grounded Into DP', 'Double Play']:
                current_outs += 2
            elif play_result == 'Triple Play':
                current_outs += 3
            # Add more specific out-counting logic as needed
            
            # Cap outs at 3 per half-inning
            current_outs = min(current_outs, 3)

    return pitch_data


def get_historical_mlb_games(num_games):
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
        return []

