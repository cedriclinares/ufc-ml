import psycopg2
from unidecode import unidecode
from gazpacho import get, Soup
import scrape_fight_data
import make_total_stats
import scrape_fight_odds
import combine_tables

# get fight data
# if fighter is not in DB create fighter
    # scrape fighter page after scraping fight details
    # Change NULL values to 0 in fight_stats table


# get odds data
# for mismatching names create a dictionary
#

# combine tables

# scrape_fight_data.scrape_fight_data()
# make_total_stats.create_cumulative_fight_data()
# scrape_fight_odds.scrape_fight_odds()
# combine_tables.get_fight_odds_ids()

'''
12/16/2023 Instructions
    1) scrape fight data
        - Figure out which cards need to be fetched
        - Check db for latests fight data and compare to fight card date
        - Scrape for those fights
    
    2) make total stats
        - Update date in get_fight_data query
        - Change null values in fight stats db to zero (Update fight stats Null to 0)
        - Run total stats script
    3) get odds for fights
        - Figure out which cards need to be fetched
        - Check db for latests fight data and compare to fight card date
        - Scrape odds
    4) Combine odds table with fights table (Links odds to fights)
        - Update date in get_all_fight_odds
        - Run combine script and manually update the name map 
        - 
    5) Export csv dataset
        - Run "join fight with total stats query"
        - Export result as csv file
    
    6) Train brf with brf_with_train_test_split.py
        - Make sure the correct dataset is used
        - Currently the best params for brf is estimators=1000, max_depth = 3, and learning_rate = .01
    7) Train neural net with mlp_classifier.py
         - Make sure the correct dataset is used
        - Currently the best params for brf is max_iter=28, alpha=.1, hidden_layer_sizes=[8]
'''