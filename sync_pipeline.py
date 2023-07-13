import dlt
import requests

pipeline = dlt.pipeline(pipeline_name="chess_pipeline",
                        destination="duckdb")
player_list = ['magnuscarlsen', 'vincentkeymer',
               'dommarajugukesh', 'rpragchess']
data = {}

player_details = []
for player in player_list:
    response = requests.get(f'https://api.chess.com/pub/player/{player}')
    response.raise_for_status()
    player_details.append(response.json())
data.update({"details": player_details})

player_stats = []
for player in player_list:
    response = requests.get(f'https://api.chess.com/pub/player/{player}/stats')
    response.raise_for_status()
    player_stats.append(response.json())
data.update({"stats": player_stats})

pipeline.run([data], table_name='player')
print("Data has been loaded!")
