import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import nba_api
import time
import os
import openpyxl
import re

from nba_api.stats.endpoints import BoxScoreTraditionalV3
from nba_api.stats.endpoints import BoxScoreAdvancedV3
from nba_api.stats.endpoints import leaguegamefinder

# Retrieves "Games" Dataframe. Primary Key is [Game ID, Team ID]; i.e. for every game, there is a row contains boxscore for team 1 and team 2 

def get_games(seasons ='2024-25', league_id_nullable = '00', season_segment = 'Regular Season'):
    # Extract Seasons 
    seasons_format_match = re.match(r'(\d+)-(\d+)', seasons)
    if seasons_format_match:
        start_year, end_year = seasons_format_match.groups()
    else:
        print('Make sure seasons format is "YYYY-YY". E.g.: 2024-25 is a single season, but 2022-24 is both the 2022-23 and 2023-24 seasons')
        return 
    
    # Create season array 
    season_range = np.arange(int(start_year), int(end_year)+1)
    
    games = pd.DataFrame()
    if len(season_range) > 0:
        for season in season_range:
            
            # Format season for game finder
            season_formatted = str(season) + '-' + str(int(season) + 1-2000)
            print(f'Loading Games for {season_formatted}')
            # Find games for season
            season_games_finder = leaguegamefinder.LeagueGameFinder(season_nullable=season_formatted, league_id_nullable=league_id_nullable, season_type_nullable = season_segment)
            season_games = season_games_finder.get_data_frames()[0]
            
            
            # Filtergames for specific phase
            season_games = season_games[season_games['SEASON_ID'] == str(phase) + str(season)]
            
            # Add season games to total games 
            games = pd.concat([games, season_games], axis = 0)   
        return games 
    else:
        print(f'Getting {season_segment} games from {seasons}')
        games_finder = leaguegamefinder.LeagueGameFinder(season_nullable=seasons, league_id_nullable=league_id_nullable, season_type_nullable = season_segment)
        season_games = games_finder.get_data_frames()[0] 
        return season_games
                           
    
# Function to explicitly index games based off of Team and Season (e.g.: the 10th game of the 2024-25 season for OKC is labeled '10')

def add_game_number(games,playoffs = False):
    if playoffs:
        games['GAME_DATE'] = pd.to_datetime(games['GAME_DATE'])
        games = games.sort_values(by = ['TEAM_ID','GAME_DATE'])
        games['GAME_NUMBER'] = 82 + games.groupby(['TEAM_ID','SEASON_ID']).cumcount() + 1
        games['GAME_NUMBER_REV'] = (82 + games.groupby(['TEAM_ID','SEASON_ID'])['TEAM_ID'].transform('size')) - games.groupby(['TEAM_ID','SEASON_ID']).cumcount()
    else:
        games['GAME_DATE'] = pd.to_datetime(games['GAME_DATE'])
        games = games.sort_values(by = ['TEAM_ID','GAME_DATE'])
        games['GAME_NUMBER'] = games.groupby(['TEAM_ID']).cumcount() + 1
        games['GAME_NUMBER_REV'] = games.groupby('TEAM_ID')['TEAM_ID'].transform('size') - games.groupby('TEAM_ID').cumcount()
    return games

def get_game_ids(games):
    game_ids = games['GAME_ID'].unique().tolist()
    return game_ids

# Function to request large amount of boxscores (one or more seasons worth)

def get_historical_boxscores(game_ids, season_range, playoffs = False, time_buffer = 30, advanced = False, team = False):

    # make sure time_buffer is  equal to 0.01 or greater 
    if time_buffer <= 0.01:
        time_buffer  = 0.01
        
    if team:
        df_index = 1
        team_label = 'team'
    else:
        df_index = 0
        team_label = 'player'
        
    if advanced:
        directory = 'adv_boxscores'
    else:
        directory = 'trad_boxscores'
    
    if playoffs:
        p_label = 'playoffs'
    else:
        p_label = ''
        
    os.makedirs(directory, exist_ok=True)
    # Define file paths
    boxscores_path = f'{directory}/{season_range}_{team_label}_{p_label}.csv'
    last_game_id_path = f'{directory}/last_game_id_{season_range}_{team_label}_{p_label}.csv'
    
    # Base Values
    boxscores_data = pd.DataFrame(columns=['gameId'])
    remaining_game_ids = game_ids

    # Iterate through gameIds and repeatedly make requests, even through timeout errors 
    while len(boxscores_data['gameId'].unique()) < len(game_ids): # Iterate until we have requested for every game_id
        
        # See last game Id since last save
        if os.path.exists(last_game_id_path):
            with open(last_game_id_path, 'r') as file:
                last_game_id = str(file.read())  

            # Get Remaining Game Ids
            last_game_id_idx = game_ids.index(last_game_id)
            remaining_game_ids = game_ids[last_game_id_idx:]
            print("Resuming from", last_game_id)

        # Load past save
        if os.path.exists(boxscores_path):
            boxscores_data = pd.read_csv(boxscores_path, dtype={'gameId': object})
        
        # Iterate through remaining game ids not in last save
        for game_id in remaining_game_ids:
            
            try:
                if advanced:
                    boxscore = BoxScoreAdvancedV3(game_id=game_id)
                elif advanced == False:
                    boxscore = BoxScoreTraditionalV3(game_id=game_id)
                boxscore_data = boxscore.get_data_frames()[df_index]

                # Add boxscore to table of boxscores
                boxscores_data = pd.concat([boxscores_data, boxscore_data])
                gameIds_left = len(remaining_game_ids) - remaining_game_ids.index(game_id)
                
                print(f'Loading for gameId: {game_id}, Boxscores left: {gameIds_left}' )

                time.sleep(time_buffer) # Add time to avoid timeout
                
            except Exception as e:
                print(f'Timed out while loading {game_id}, saving current progress as {boxscores_path}', e)
                
                # Save progress
                boxscores_data.to_csv(boxscores_path)
                
                # Save current game_id
                with open(last_game_id_path, 'w') as file:
                    file.write(str(game_id))  # Write the number as a string
                
                # Initate time break to avoid timeout
                print('Waiting 30 minutes')
                time.sleep(1800) # wait 30 minutes 
                print('30 Minutes Elapsed, proceeding with loop')
                
                # Increase time buffer to avoid timeout
                print(f'Time buffer increased from {time_buffer} to {round(time_buffer*1.5, 2)}')
                time_buffer = round(time_buffer*1.5, 2)
                break
                
    print(f'Finished loading {team_label} {p_label} {directory}')
    boxscores_data.to_csv(boxscores_path, index = False)

# Function to update file of boxscores (Typically for the current season)
def update_boxscores(game_ids, boxscores_path, time_buffer = 1, advanced = False, team = False):
    # Check if Team or Player
    if team:
        df_index = 1
        team_print = 'Team'
    else:
        df_index = 0
        team_print = 'Player'
        
    # Load past save
    if os.path.exists(boxscores_path):
        boxscores_data = pd.read_csv(boxscores_path, dtype={'gameId': object})
        missing_game_ids = list(set(game_ids) - set(boxscores_data['gameId']))
    else:
        print(f'No file under name: {boxscores_path}. Creating new file.')
        missing_game_ids = game_ids
        boxscores_data = pd.DataFrame()
    
    
     # Iterate through remaining game ids not in last save
    for game_id in missing_game_ids:
        
        try:
            if advanced:
                boxscore = BoxScoreAdvancedV3(game_id = game_id)
            elif advanced == False:
                boxscore = BoxScoreTraditionalV3(game_id=game_id)
                
            boxscore_data = boxscore.get_data_frames()[df_index]

            # Add boxscore to table of boxscores
            boxscores_data = pd.concat([boxscores_data, boxscore_data])
            gameIds_left = len(missing_game_ids) - missing_game_ids.index(game_id)
            
            print(f'Loading for gameId: {game_id}, Boxscores left: {gameIds_left}' )

            time.sleep(time_buffer) # Add time to avoid timeout
        except IndexError as e:
            try:
                if advanced:
                    boxscore = BoxScoreAdvanvedV3(game_id = '00' + str(game_id))
                elif advanced == False:
                    boxscore = BoxScoreTraditionalV3(game_id= '00' + str(game_id))
                
                boxscore_data = boxscore.get_data_frames()[df_index]
                
                boxscores_data = pd.concat([boxscores_data, boxscore_data])
                gameIds_left = len(missing_game_ids) - missing_game_ids.index(game_id)
                print(f'Loading for gameId: 00{game_id}')
            except Exception as e:
                print(f"No data for game_id {game_id}. Skipping.", e)
        
        except Exception as e:
            print(f'Timed out while loading {game_id}, saving current progress as boxscoress.csv', e)
            
            # Save progress
            boxscores_data.to_csv(boxscores_path, index=False)
        
            # Save current game_id
            with open('last_game_id.txt', 'w') as file:
                file.write(str(game_id))  # Write the number as a string
            break
    if advanced:
        adv_print = 'advanced'
    else:
        adv_print = 'traditional'
    print(f'Finished updating {team_print} {adv_print} boxscores')
    boxscores_data.to_csv(boxscores_path, index = False)



# Update games, player, and team boxscores, as well as their advanced variants for a given season

def update_all(season ='2024-25', league_id_nullable = '00'):
    os.makedirs('games', exist_ok=True)
    games = get_games(seasons = season)
    games = add_game_number(games)
    game_ids = get_game_ids(games)
    games.to_csv(f'games/{season}.csv', index = False)
    update_boxscores(game_ids, f'trad_boxscores/{season_nullable}_player.csv', time_buffer = 1)

    pgames = get_games(seasons = season_nullable, season_segment = 'Playoffs')
    pgames = add_game_number(pgames, playoffs = True)
    pgame_ids = get_game_ids(pgames)
    pgames.to_csv(f'games/{season}_playoff.csv', index = False)
    update_boxscores(pgame_ids, f'adv_boxscores/{season}_team_playoff.csv', time_buffer = 1, advanced = True, team = True)
    update_boxscores(pgame_ids, f'adv_boxscores/{season}_player_playoff.csv', time_buffer = 1, advanced = True)
    update_boxscores(pgame_ids, f'/Users/connerkhudaverdyan/Desktop/Projects/analytics_account/trad_boxscores/{season_nullable}_player_playoff.csv', time_buffer = 1, advanced =False, team = False)
    print('Finished Updating all Boxscores')


    


## Example: Retrieve traditional PLAYER boxscores for all games from the 2020-21, 2021-22, 2022-23, 2023-24, and  2024-25 playoffs 
'''
playoff_games_2020_to_2025 = get_games('2020-2025', playoffs= True)
gameIds = get_game_ids(playoff_games_2020_to_2025)
df = get_historical_boxscores(game_ids = gameIds, season_range = '2020-2025',playoffs = True, time_buffer = 3, team = False)
'''
