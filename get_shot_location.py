import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import nba_api
import time
import os
from nba_api.stats.endpoints import shotchartdetail
from nba_api.stats.static import teams

# Function to get team ID

def get_game_ids(games):
    game_ids = games['GAME_ID'].unique().tolist()
    return game_ids

def get_team_id(team_name):
    all_teams = teams.get_teams()
    for team in all_teams:
        if team_name.lower() in team['full_name'].lower():
            return team['id']
    return None

# Function to get team shot data for a range of games 
def get_team_shots(team_name, game_ids, season_type_all_star):
    team_id = get_team_id(team_name)
    if not team_id:
        print("Team not found.")
        return None
    
    print(f'Retrieving Shot Locations for {team_name}')
    if type(game_ids) == str:
        shot_chart = shotchartdetail.ShotChartDetail(
            team_id=team_id,
            player_id=0,  # 0 means all players on the team
            game_id_nullable=game_ids,
            context_measure_simple='FGA',
            season_type_all_star=season_type_all_star
        )
        df = shot_chart.get_data_frames()
    else: 
        shot_df = pd.DataFrame()
        for game_id in game_ids:
            
            # Delay to avoid rate limits
            time.sleep(1)

            shot_chart = shotchartdetail.ShotChartDetail(
                team_id=team_id,
                player_id=0,  # 0 means all players on the team
                game_id_nullable=game_id,
                context_measure_simple='FGA',
                season_type_all_star=season_type_all_star
            )

            df = shot_chart.get_data_frames()
            shot_df = pd.concat([shot_df, df], axis = 0)
            
    return df


## Example 1: Get The Minnesota Timberwolves Shot Locations for game id '0042400161', from 2024-25 playoffs

df = get_team_shots('Minnesota Timberwolves', game_ids = '0042400161', season_type_all_star='Playoffs') 
df[0] # Returns Minnesota Shot Locations
df[1] # Returns the League Avergae stats for those locations
df[0].to_csv('MIN_Shots_example.csv', index= False)


## Example 2: Get the Minnesota Timberwolves Shpot Locations for the Entire 2024-25 Regular Season
'''
games = pd.read_csv('games/2024-25_games.csv')
game_ids = get_game_ids(games)
df = get_team_shots('Minnesota Timberwolves', game_ids = game_ids, season_type_all_star='Regular Season') 
'''