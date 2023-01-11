from multiprocessing.pool import ThreadPool
from kafka import KafkaConsumer
from data_process_util import *
from database_util import *
from datetime import datetime
from tqdm import tqdm
from collections import OrderedDict
from psycopg2.errors import InFailedSqlTransaction
import re
import psycopg2
import psycopg2.extras
import os
import traceback

def process_batch(connect_string, games_columns, filename, file_number, start, batch_size):
	conn = psycopg2.connect(connect_string)  
	batch = []  #database writes are done in batches to minimize server roundtrips
	game = OrderedDict()
	id_dict = load_id_dict(conn)    #load dict to assign user IDs to usernames
	new_id_dict = {}
	lines = read_lines_plain(filename, start=start, batch_size=batch_size)

	try: #if any exception, write the id_dict to "user_IDs" database table to record new user_IDs before raising error
		for line in tqdm(lines):
			if len(line) <= 1: continue
			if line == '\n' or line[0] == ' ': continue
			try:
				key = re.search("\[(.*?) ",line).group(1)
				val = re.search(" \"(.*?)\"\]", line).group(1)
				if key in ("Date", "Round", "Opening", "WhiteTitle", "BlackTitle"): continue    #skip irrelevant data (adjust if you prefer) 
				if key not in games_columns + ["UTCDate", "UTCTime"]: continue   #if some unforseen data type not in table, skip it
				if key in ("White", "Black"):
					(val, id_dict, new_id_dict) = assign_user_ID(file_number*(start%100)*batch_size, batch_size, val, id_dict, new_id_dict)   #converts username to user ID and updates id_dict
				key, val = format_data(key, val)
				game[key] = val
			except AttributeError:
				pass

			#checks if the line is describing the moves of a game (the line starts with "1"). 
			#If so, all the data for the game has been read and we can format the game data
			if line[0] == '1':
				if 'eval' in line:
					game["Analyzed"] = True
				else:
					game["Analyzed"] = False 
				game = format_game(game)
				if game:
					batch.append(game)
				game = OrderedDict()   #reset game dict variable for the next set of game data
		# write games data and id_dict values to database
		try:
			copy_data(conn, batch, "games", get_columns())
			dump_dict(new_id_dict, conn)
		except Exception as e:
			print(e)
			return 1
		return 0
	except (Exception, KeyboardInterrupt) as e: #on consumer shutdown, write remaining games data and id_dict values to database
		print(f"{e} exception raised, writing id_dict to database")
		traceback.print_exc()
		dump_dict(id_dict, conn)
		copy_data(conn, batch, "games", get_columns())
		return 1


def process_file(url):
	"""python function for airflow dag. takes a url who's file has been downloaded and loads data into database"""
	DAG_PATH = os.path.realpath(__file__)
	DAG_PATH = '/' + '/'.join(DAG_PATH.split('/')[1:-1]) + '/'
	DAG_PATH = "/media/pafrank/Backup/other/Chess/lichess/database.lichess.org/standard/"
	DB_NAME = os.getenv('POSTGRES_DB', 'lichess_games') #env variables come from docker-compose.yml
	DB_USER = os.getenv('POSTGRES_USER','postgres')
	DB_PASSWORD = os.getenv('POSTGRES_PASSWORD','postgres')
	HOSTNAME = os.getenv('HOSTNAME','localhost')
	PORT = os.getenv('POSTGRES_PORT', '5432')
	BATCH_SIZE = int(os.getenv('BATCH_SIZE', 100000))
	connect_string = "host=" + HOSTNAME + " dbname=" + DB_NAME + " user=" + DB_USER + " password=" + DB_PASSWORD \
			+ " port=" + PORT
	conn = psycopg2.connect(connect_string)  
	games_columns = initialize_tables(conn)         #create necessary tables in postgresql if they don't already exist
	#consumer will read data until it's read a full game's data, then add the game data to batch
	data_path = DAG_PATH #+"../lichess_data/"
	filename = url.split('/')[-1]
	file_number = int(filename.split('.')[-3][-2:])
	filepath = data_path + filename
	os.system(f"zstdcat {filepath} > {filepath[:-4]}")
	n_lines = number_lines(filepath[:-4])
	partitions_start = [i*BATCH_SIZE for i in range(n_lines//BATCH_SIZE)]
	print(file_number, partitions_start)
	with ThreadPool(processes=12) as p:
		results = [p.apply_async(process_batch, (connect_string, games_columns, filepath[:-4], file_number, start, BATCH_SIZE)) for start in partitions_start]
		for r in results:
			print(r.get())
	# for start in partitions_start:
	# 	process_batch(connect_string, games_columns, filepath[:-4], file_number, start, BATCH_SIZE)
	os.system(f"rm {filepath[:-4]}")

if __name__ == "__main__":
	process_file("https://database.lichess.org/standard/lichess_db_standard_rated_2013-01.pgn.zst")
