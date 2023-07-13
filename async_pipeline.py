import asyncio
import aiohttp
import dlt

pipeline_name = "chess_pipeline"
destination = "duckdb"
pipeline = dlt.pipeline(pipeline_name=pipeline_name,
                        destination=destination)
player_list = ['magnuscarlsen', 'vincentkeymer',
               'dommarajugukesh', 'rpragchess']
data = {}


async def get_player_data(session, player):
    async with session.get(f'https://api.chess.com/pub/player/{player}') as response:
        response.raise_for_status()
        return await response.json()


async def main():
    async with aiohttp.ClientSession() as session:
        player_details = await asyncio.gather(*[get_player_data(session, player) for player in player_list])
        data.update({"details": player_details})

        player_stats = await asyncio.gather(*[get_player_data(session, f"{player}/stats") for player in player_list])
        data.update({"stats": player_stats})

asyncio.run(main())
pipeline.run([data], table_name="player")
print("Data has been loaded!")
