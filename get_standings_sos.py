import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import nba_api
import time
import os
import requests
import datetime

from nba_api.stats.endpoints import LeagueStandings
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import ScoreboardV2
from nba_api.stats.library.parameters import SeasonAll


def load_games():
    return pd.read_csv('games.csv')

def update_standings(season, league_id = '00', season_type = 'Regular Season' ):
    path = f'standings/{season}.csv'
    standings = LeagueStandings(season = season, league_id = league_id, season_type = 'Regular Season')
    data = standings.get_data_frames()[0]
    data.to_csv(path, index= False)


def update_schedule(season, league_id = '00'):
    games = load_games()
    path = f'schedule/{season}.csv'
    schedule = ScoreboardV2(season = seaosn, league_id = league_id)
    data = schedule.get_data_frames()[0]
    data.to_csv(path, index = False)
    
def update_schedule_standings(season = '2024', league_id = '00'):
    os.makedirs('schedule_standings', exist_ok=True)
    
    # URL to fetch the full NBA schedule for the 2024 season
    url = f'https://data.nba.com/data/10s/v2015/json/mobile_teams/nba/{season}/league/{league_id}_full_schedule.json'

    # Send a GET request to fetch the data
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON data
        data = response.json()

    else:
        print(f"Error fetching data: {response.status_code}")
        
    sd_data = pd.DataFrame()
    
    # Extract schedule from all months
    for x in data['lscd']:
        monthly_data = pd.DataFrame(x['mscd']['g'])
        
        sd_data = pd.concat([sd_data,monthly_data])
    sd_data = sd_data.reset_index(drop = True)
    
    sd_data= sd_data[['gid','gdte', 'v', 'h']] # Select columns 
    
    # Extract values from dictionaries in 'v' and 'h' columns 
    for team in ['v', 'h']:
        for added_column in ['tid', 'ta', 'tn', 're', 's']:
            sd_data[f'{team}_{added_column}'] = sd_data[team].apply(lambda x: x.get(added_column) if isinstance(x, dict) else None)
    
    # Ensure game date is in datetime
    sd_data['gdte'] = pd.to_datetime(sd_data['gdte'])
    
    # Select desire
    sd_data = sd_data.drop(['v', 'h'], axis = 1)
    
    path = f'schedule_standings/{season}-{str(int(season)+1)}.csv'
    sd_data.to_csv(path, index = False)
    
    print(f'Saved standings and schedule data to: {path}')


# Returns the Strength of Season for every team (If its Playoffs, it will only return the strength of the remaining games in the current Series)
def update_sos(season):
    os.makedirs('schedule_standings', exist_ok=True)
    sd = pd.read_csv(f'schedule_standings/{season}-{str(int(season)+1)}.csv')
    
    sd['h_wr'] = sd['h_re'].apply(lambda re: int(re.split('-')[0]) / (int(re.split('-')[0]) + int(re.split('-')[1])))
    sd['v_wr'] = sd['v_re'].apply(lambda re: int(re.split('-')[0]) / (int(re.split('-')[0]) + int(re.split('-')[1])))
    
    today = pd.Timestamp.today().date()
    sd['gdte'] = pd.to_datetime(sd['gdte'])

    # Group by home team ('h_tn') and calculate the mean and count of 'v_wr'
    team_home = sd[sd['gdte'].dt.date >= today].groupby(['h_tn'])[['v_wr']].agg(['mean', 'count']).reset_index()
    team_home.columns = ['h_tn', 'mean_v_wr', 'count_v_wr']  # Renaming columns for clarity

    # Group by visiting team ('v_tn') and calculate the mean and count of 'h_wr'
    team_versus = sd[sd['gdte'].dt.date >= today].groupby(['v_tn'])['h_wr'].agg(['mean', 'count']).reset_index()
    team_versus.columns = ['v_tn', 'mean_h_wr', 'count_h_wr']  # Renaming columns for clarity
    
    sched_strength = pd.merge(team_home, team_versus, how = 'inner', left_on = 'h_tn', right_on = 'v_tn')
    sched_strength['sched_strength'] = (sched_strength['mean_v_wr']*sched_strength['count_v_wr']+ sched_strength['mean_h_wr']*sched_strength['count_h_wr'])/(sched_strength['count_h_wr'] + sched_strength['count_v_wr'])
    
    # Final formating and save as csv
    sos = sched_strength.rename(columns = {'h_tn': 'Team Name'}).drop(['v_tn'], axis = 1).sort_values(by = ['sched_strength'], ascending = False )
    sos.to_csv('schedule_standings/sos.csv', index = False)
        
update_schedule_standings()

