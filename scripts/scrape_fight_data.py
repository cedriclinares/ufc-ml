from gazpacho import get, Soup
import psycopg2
import combine_tables
import scrape_fighter_data
# from datetime import datetime
# from dateutil.parser import parse


def get_soup(url):
    html = get(url)
    return Soup(html)


def get_fighter_names_and_result(fighters_info):
    winner = ''
    r_fighter = fighters_info[0]
    r_fighter_name = r_fighter.find('a', {'class': 'b-fight-details__person-link'}).text
    r_fighter_url = r_fighter.find('a', {'class': 'b-fight-details__person-link'}).attrs['href']
    r_result = r_fighter.find('i', {'class': 'b-fight-details__person-status'}).text
    if r_result == 'W':
        winner = r_fighter_name
    b_fighter = fighters_info[1]
    b_fighter_name = b_fighter.find('a', {'class': 'b-fight-details__person-link'}).text
    b_fighter_url = b_fighter.find('a', {'class': 'b-fight-details__person-link'}).attrs['href']
    b_result = b_fighter.find('i', {'class': 'b-fight-details__person-status'}).text
    if b_result == 'W':
        winner = b_fighter_name
    
    r_fighter_id = get_fighter_id(r_fighter_name, r_fighter_url)
    b_fighter_id = get_fighter_id(b_fighter_name, b_fighter_url)

    return r_fighter_id, r_fighter_name, b_fighter_id , b_fighter_name, winner


def get_fight_finish_details(fight_finish_info):
    fight_title = fight_finish_info.find('i', {'class': 'b-fight-details__fight-title'})
    title_icons = fight_title.find('img', mode='all')
    is_title_fight = False
    for icon in title_icons:
        if icon.attrs['src'] == 'http://1e49bc5171d173577ecd-1323f4090557a33db01577564f60846c.r80.cf1.rackcdn.com/belt.png':
            is_title_fight = True
    detail_items = fight_finish_info.find('i', {'class': 'b-fight-details__text-item'}, partial=True, mode='all')
    method = detail_items[0].find('i', attrs={'style': 'font-style: normal'}).text
    finish_round = detail_items[1].html.split('</i>')[1].strip()
    finish_time = detail_items[2].html.split('</i>')[1].strip()
    number_of_rounds = detail_items[3].html.split('</i>')[1].strip().split(' ')[0]
    # print("number_of_rounds {}".format(number_of_rounds))
    fight_referee = detail_items[4].find('span').text
    win_details = fight_finish_info.find('p', {'class': 'b-fight-details__text'})[1].html.split('\n')[6].strip()
    return method, win_details, finish_round, finish_time, number_of_rounds, fight_referee, is_title_fight


def get_total_stats(total_stats_columns, r_stats, b_stats):
    knockdowns = total_stats_columns[0].find('p', mode='all')
    r_stats['KD_landed'] = knockdowns[0].text
    b_stats['KD_landed'] = knockdowns[1].text

    sig_str = total_stats_columns[1].find('p', mode='all')
    r_sig_str = sig_str[0].text.split(' of ')
    r_stats['sig_str_landed'] = r_sig_str[0]
    r_stats['sig_str_attempted'] = r_sig_str[1]
    b_sig_str = sig_str[1].text.split(' of ')
    b_stats['sig_str_landed'] = b_sig_str[0]
    b_stats['sig_str_attempted'] = b_sig_str[1]

    total_str = total_stats_columns[3].find('p', mode='all')
    r_total_str = total_str[0].text.split(' of ')
    r_stats['total_str_landed'] = r_total_str[0]
    r_stats['total_str_attempted'] = r_total_str[1]
    b_total_str = total_str[1].text.split(' of ')
    b_stats['total_str_landed'] = b_total_str[0]
    b_stats['total_str_attempted'] = b_total_str[1]

    takedowns = total_stats_columns[4].find('p', mode='all')
    r_td = takedowns[0].text.split(' of ')
    r_stats['TD_landed'] = r_td[0]
    r_stats['TD_attempted'] = r_td[1]
    b_td = takedowns[1].text.split(' of ')
    b_stats['TD_landed'] = b_td[0]
    b_stats['TD_attempted'] = b_td[1]

    sub_att = total_stats_columns[6].find('p', mode='all')
    r_stats['subs_attempted'] = sub_att[0].text
    b_stats['subs_attempted'] = sub_att[1].text

    reversals = total_stats_columns[7].find('p', mode='all')
    r_stats['reversals'] = reversals[0].text
    b_stats['reversals'] = reversals[1].text

    control_time = total_stats_columns[8].find('p', mode='all')
    r_ctrl = control_time[0].text.split(':')
    r_ctrl_seconds = int(r_ctrl[0]) * 60 + int(r_ctrl[1])
    r_stats['ctrl_time'] = r_ctrl_seconds
    b_ctrl = control_time[1].text.split(':')
    b_ctrl_seconds = int(b_ctrl[0]) * 60 + int(b_ctrl[1])
    b_stats['ctrl_time'] = b_ctrl_seconds


def get_sig_str_stats(sig_str_columns, r_stats, b_stats):
    head_str = sig_str_columns[2].find('p', mode='all')
    r_head_str = head_str[0].text.split(' of ')
    r_stats['head_landed'] = r_head_str[0]
    r_stats['head_attempted'] = r_head_str[1]
    b_head_str = head_str[1].text.split(' of ')
    b_stats['head_landed'] = b_head_str[0]
    b_stats['head_attempted'] = b_head_str[1]

    body_str = sig_str_columns[3].find('p', mode='all')
    r_body_str = body_str[0].text.split(' of ')
    r_stats['body_landed'] = r_body_str[0]
    r_stats['body_attempted'] = r_body_str[1]
    b_body_str = body_str[1].text.split(' of ')
    b_stats['body_landed'] = b_body_str[0]
    b_stats['body_attempted'] = b_body_str[1]

    leg_str = sig_str_columns[4].find('p', mode='all')
    r_leg_str = leg_str[0].text.split(' of ')
    r_stats['leg_landed'] = r_leg_str[0]
    r_stats['leg_attempted'] = r_leg_str[1]
    b_leg_str = leg_str[1].text.split(' of ')
    b_stats['leg_landed'] = b_leg_str[0]
    b_stats['leg_attempted'] = b_leg_str[1]

    distance_str = sig_str_columns[5].find('p', mode='all')
    r_distance_str = distance_str[0].text.split(' of ')
    r_stats['distance_landed'] = r_distance_str[0]
    r_stats['distance_attempted'] = r_distance_str[1]
    b_distance_str = distance_str[1].text.split(' of ')
    b_stats['distance_landed'] = b_distance_str[0]
    b_stats['distance_attempted'] = b_distance_str[1]

    clinch_str = sig_str_columns[6].find('p', mode='all')
    r_clinch_str = clinch_str[0].text.split(' of ')
    r_stats['clinch_landed'] = r_clinch_str[0]
    r_stats['clinch_attempted'] = r_clinch_str[1]
    b_clinch_str = clinch_str[1].text.split(' of ')
    b_stats['clinch_landed'] = b_clinch_str[0]
    b_stats['clinch_attempted'] = b_clinch_str[1]

    ground_str = sig_str_columns[7].find('p', mode='all')
    r_ground_str = ground_str[0].text.split(' of ')
    r_stats['ground_landed'] = r_ground_str[0]
    r_stats['ground_attempted'] = r_ground_str[1]
    b_ground_str = ground_str[1].text.split(' of ')
    b_stats['ground_landed'] = b_ground_str[0]
    b_stats['ground_attempted'] = b_ground_str[1]


def get_round_stats(round_stats, r_stats, b_stats):
    for idx, round_html in enumerate(round_stats):
        round_num = idx
        red_stats = round_html.find('i', {'class': 'js-red'})
        red_stats_text = red_stats.find('i', {'class': 'b-fight-details__bar-chart-text'}).text
        red_round_strikes = red_stats_text.split(' - ')[0].split(' of ')
        r_stats[f"round_{idx + 1}_sig_str_landed"] = red_round_strikes[0]
        r_stats[f"round_{idx + 1}_sig_str_attempted"] = red_round_strikes[1]
        blue_stats = round_html.find('i', {'class': 'js-blue'})
        blue_stats_text = blue_stats.find('i', {'class': 'b-fight-details__bar-chart-text'}).text
        blue_round_strikes = blue_stats_text.split(' - ')[0].split(' of ')
        b_stats[f"round_{idx + 1}_sig_str_landed"] = blue_round_strikes[0]
        b_stats[f"round_{idx + 1}_sig_str_attempted"] = blue_round_strikes[1]
    round_num = len(round_stats) + 1
    while round_num < 6:
        r_stats[f"round_{round_num}_sig_str_landed"] = None
        r_stats[f"round_{round_num}_sig_str_attempted"] = None
        b_stats[f"round_{round_num}_sig_str_landed"] = None
        b_stats[f"round_{round_num}_sig_str_attempted"] = None
        round_num += 1


def get_fight_stats(fight_stats_info):
    r_stats = {}
    b_stats = {}

    sections = fight_stats_info.find('section', {'class': 'b-fight-details__section'}, partial=True, mode='all')
    totals_section = sections[1]
    totals_stats_table = totals_section.find('tbody', {'class': 'b-fight-details__table-body'})
    total_stats_columns = totals_stats_table.find('td', {'class': 'b-fight-details__table-col'}, partial=False, mode='all')

    get_total_stats(total_stats_columns, r_stats, b_stats)

    sig_str_table = fight_stats_info.find('table', partial=False, mode='all')[2]
    sig_str_stats = sig_str_table.find('tbody', {'class': 'b-fight-details__table-body'})
    sig_str_columns = sig_str_stats.find('td', {'class': 'b-fight-details__table-col'}, partial=False, mode='all')

    get_sig_str_stats(sig_str_columns, r_stats, b_stats)

    per_round_section = fight_stats_info.find('div', {'class': 'b-fight-details__charts-col_pos_right'})
    round_stats = per_round_section.find('div', {'class': 'b-fight-details__bar-charts-row'}, mode='all')

    get_round_stats(round_stats, r_stats, b_stats)

    return r_stats, b_stats

def save_fighter_stats(data):
    try:
        #Establishing the connection
        conn = psycopg2.connect(
            database="ufc-data", user='cedriclinares', password='funkmaster123', host='127.0.0.1', port= '5432'
        )

        print("Saving fight stats data {}".format(data))
        #Setting auto commit false
        # conn.autocommit = True

        #Creating a cursor object using the cursor() method
        cursor = conn.cursor()

        query = """
                    INSERT INTO fight_stats (
                        name, fighter_id, sig_str_landed, sig_str_attempted,
                        head_landed, head_attempted, body_landed, body_attempted,
                        leg_landed, leg_attempted, distance_landed, distance_attempted,
                        clinch_landed, clinch_attempted, ground_landed, ground_attempted,
                        total_str_landed, total_str_attempted, TD_landed, TD_attempted,
                        KD_landed, subs_attempted, ctrl_time, reversals,
                        round_1_sig_str_landed, round_1_sig_str_attempted,
                        round_2_sig_str_landed, round_2_sig_str_attempted,
                        round_3_sig_str_landed, round_3_sig_str_attempted,
                        round_4_sig_str_landed, round_4_sig_str_attempted,
                        round_5_sig_str_landed, round_5_sig_str_attempted
                    )
                    VALUES (
                        %(name)s, %(fighter_id)s, %(sig_str_landed)s, %(sig_str_attempted)s,
                        %(head_landed)s, %(head_attempted)s, %(body_landed)s, %(body_attempted)s,
                        %(leg_landed)s, %(leg_attempted)s, %(distance_landed)s, %(distance_attempted)s,
                        %(clinch_landed)s, %(clinch_attempted)s, %(ground_landed)s, %(ground_attempted)s,
                        %(total_str_landed)s, %(total_str_attempted)s, %(TD_landed)s, %(TD_attempted)s,
                        %(KD_landed)s, %(subs_attempted)s, %(ctrl_time)s, %(reversals)s,
                        %(round_1_sig_str_landed)s, %(round_1_sig_str_attempted)s,
                        %(round_2_sig_str_landed)s, %(round_2_sig_str_attempted)s,
                        %(round_3_sig_str_landed)s, %(round_3_sig_str_attempted)s,
                        %(round_4_sig_str_landed)s, %(round_4_sig_str_attempted)s,
                        %(round_5_sig_str_landed)s, %(round_5_sig_str_attempted)s
                    )
                    RETURNING id;
                """

        # Execute the query with the provided data
        cursor.execute(query, data)
        fighter_stats_id = cursor.fetchone()[0]
        # Commit your changes in the database
        conn.commit()
        print("Row inserted successfully!")
        return fighter_stats_id
    except (Exception, psycopg2.Error) as error:
        print("Error while inserting row:", error)
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def save_fight(data):
    try:
        # Replace the connection parameters with your actual database credentials
        conn = psycopg2.connect(
            database="ufc-data", user='cedriclinares', password='funkmaster123', host='127.0.0.1', port= '5432'
        )
        print("Saving fight data {}".format(data))

        # Open a cursor to perform database operations
        cursor = conn.cursor()

        # Construct the SQL query with dynamic values
        query = """
            INSERT INTO fights (
                r_fighter_id, b_fighter_id, location, date, championship_fight,
                winner, win_method, win_method_details, number_of_rounds,
                finish_round, finish_time, total_fight_time, referee,
                r_fight_stats_id, b_fight_stats_id, r_name, b_name
            )
            VALUES (
                %(r_fighter_id)s, %(b_fighter_id)s, %(location)s, %(date)s, %(championship_fight)s,
                %(winner)s, %(win_method)s, %(win_method_details)s, %(number_of_rounds)s,
                %(finish_round)s, %(finish_time)s, %(total_fight_time)s, %(referee)s,
                %(r_fight_stats_id)s, %(b_fight_stats_id)s, %(r_name)s, %(b_name)s
            )
            RETURNING id;
        """

        # Execute the query with the provided data
        cursor.execute(query, data)
        fight_id = cursor.fetchone()

        # Commit the changes to the database
        conn.commit()
        print("Row inserted successfully!")
        return fight_id
    except (Exception, psycopg2.Error) as error:
        print("Error while inserting row:", error)
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def get_fighter_id(name, url):
    fighter_id = combine_tables.get_fighter_id(name)
    if fighter_id is None:
        fighter_id = scrape_fighter_data.scrape_fighter_data(url)
    print("FIGHTER_ID: ", fighter_id, name)
    return fighter_id
        
def scrape_fight_data():
    # print("scraping fight data")
    ufc_cards_url = 'http://ufcstats.com/statistics/events/completed?page=all'
    soup = get_soup(ufc_cards_url)
    cards = soup.find('tr', {'class': 'b-statistics__table-row'}, partial=False, mode='all')
    # print(cards[2:9])

    for card in cards[2:9]:
        card_details_url = card.find('a', {'class': 'b-link_style_black'}).attrs['href']
        card_date = card.find('span', {'class': 'b-statistics__date'}).text
        card_location = card.find('td', {'class': 'b-statistics__table-col_style_big-top-padding'}).text
        
        print('Card date: {}'.format(card_date))
        print('Card location: {}'.format(card_location))

        fights = get_soup(card_details_url)
        fight_details = fights.find('tbody', {'class': 'b-fight-details__table-body'}).find('tr', {'class': 'b-fight-details__table-row'}, partial=True, mode='all')
        fight_winner = ""

        for fight in fight_details:
            fight_details_url = fight.attrs['data-link']
            fight_page = get_soup(fight_details_url)
            fight_finish_details = fight_page.find('div', {'class': 'b-fight-details__fight'})
            fighters = fight_page.find('div', {'class': 'b-fight-details__person'}, partial=False, mode='all')

            fight_data = {}
            fight_data["date"] = card_date
            fight_data["location"] = card_location

            r_fighter_id, r_name, b_fighter_id, b_name, fight_winner = get_fighter_names_and_result(fighters)
            
            fight_data["winner"] = fight_winner
            fight_data["r_name"] = r_name
            fight_data["b_name"] = b_name

            # print("r name: {}".format(r_name))
            # print("b name: {}".format(b_name))
            # print("winner: {}".format(fight_winner))
            win_method, win_method_details, end_round, end_time, number_of_rounds, referee, is_championship_fight = \
                get_fight_finish_details(fight_finish_details)
            fight_data["win_method"] = win_method
            fight_data["win_method_details"] = win_method_details
            fight_data["finish_round"] = end_round
            fight_data["finish_time"] = end_time
            fight_data["referee"] = referee
            fight_data["number_of_rounds"] = number_of_rounds
            fight_data["championship_fight"] = is_championship_fight
            time_split = end_time.split(':')
            fight_data["total_fight_time"] = (int(end_round)-1) * 300 + int(time_split[0]) * 60 + int(time_split[1])
            # print("win_method: {}".format(win_method))
            # print("win_method_details: {}".format(win_method_details))
            # print("end_round: {}".format(end_round))
            # print("time: {}".format(end_time))
            # print("referee: {}".format(referee))
            # print("is championship fight: {}".format(is_championship_fight))
            fight_stats_html = fight_page.find('div', {'class': 'b-fight-details'}, partial=False, mode='first')
            r_fighter_stats, b_fighter_stats = get_fight_stats(fight_stats_html)
            r_fighter_stats["name"] = r_name
            b_fighter_stats["name"] = b_name
            # print("r_fighter_stats: {}\n".format(r_fighter_stats))
            # print("b_fighter_stats: {}\n".format(b_fighter_stats))
            # print("fight_data: {}".format(fight_data))
            r_fighter_stats["fighter_id"] = r_fighter_id
            b_fighter_stats["fighter_id"] = b_fighter_id
            r_stats_id = save_fighter_stats(r_fighter_stats)
            b_stats_id = save_fighter_stats(b_fighter_stats)
            # print("r_stats_id {}".format(r_stats_id))
            # print("b_stats_id {}".format(b_stats_id))
            fight_data["r_fight_stats_id"] = r_stats_id
            fight_data["b_fight_stats_id"] = b_stats_id
            fight_data["r_fighter_id"] = r_fighter_id
            fight_data["b_fighter_id"] = b_fighter_id

            print("fight_data: ", fight_data)
            print("R_FIGHTER_STATS: {}\n".format(r_fighter_stats))
            print("B_FIGHTER_STATS: {}\n".format(b_fighter_stats))
            
            fight_id = save_fight(fight_data)
            print("fight_id ", fight_id) 

scrape_fight_data()

