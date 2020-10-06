import pickle
import requests
from bs4 import BeautifulSoup
import pandas as pd
from time import sleep

url_base = "https://fbref.com/en/comps/24/schedule/Serie-A-Scores-and-Fixtures"

match_url = url_base

request = requests.get(match_url)
page_content = BeautifulSoup(request.content, 'html.parser')

all_matches = pd.read_html(str(page_content.findAll('table')))[0]


def get_match_infos(page_content):
    url_matches_list = []
    rows = page_content.findAll('table')[0].findAll('tr')
    for row in rows:
        try:
            match_url = row.findAll('td', {'class': 'center'})[
                0].findAll('a', href=True)[0]['href']
            url_matches_list.append(match_url)
        except Exception as e:  # noqa
            pass
    return url_matches_list


url_matches_list = get_match_infos(page_content)


def get_match_stats(match_page_content):
    stats = match_page_content.findAll('div', {'id': 'team_stats_extra'})[
        0].findAll('div')

    match_stats = []
    for stats_divs in stats:
        for stats_infos in stats_divs:
            if 'div' in str(stats_infos):
                for stats_infos_divs in stats_infos:
                    if str(stats_infos_divs) not in words_black_list:
                        match_stats.append(stats_infos_divs)
            else:
                if str(stats_infos) not in words_black_list:
                    match_stats.append(stats_infos)

    home_team = match_stats[0]
    away_team = match_stats[1]

    match_stats = [stats for stats in match_stats if (
        stats != home_team and stats != away_team)]

    return match_stats, home_team, away_team


def get_stats_tables(match_page_content, home_team, away_team):
    gk_away_stats = pd.read_html(
        str(match_page_content.findAll('table')[-1]))[0]
    gk_away_stats.columns = [col[1] for col in gk_away_stats.columns]
    gk_away_stats['team'] = away_team

    team_away_stats = pd.read_html(
        str(match_page_content.findAll('table')[-2]))[0][:-1]
    team_away_stats.columns = [col[1] for col in team_away_stats.columns]
    team_away_stats['team'] = away_team

    gk_home_stats = pd.read_html(
        str(match_page_content.findAll('table')[-3]))[0]
    gk_home_stats.columns = [col[1] for col in gk_home_stats.columns]
    gk_home_stats['team'] = home_team

    team_home_stats = pd.read_html(
        str(match_page_content.findAll('table')[-4]))[0][:-1]
    team_home_stats.columns = [col[1] for col in team_home_stats.columns]
    team_home_stats['team'] = home_team

    gk_stats = gk_away_stats.append(gk_home_stats)
    team_stats = team_away_stats.append(team_home_stats)

    return team_stats, gk_stats


url = "https://fbref.com/"
words_black_list = ['', '\xa0', '\n']

match_team_stats = pd.DataFrame()
match_gk_stats = pd.DataFrame()
df_match_infos = pd.DataFrame()
for idx, match_url in enumerate(url_matches_list):
    request = requests.get(url + match_url)
    page_content = BeautifulSoup(request.content, 'html.parser')

    match_stats, home_team, away_team = get_match_stats(page_content)

    stats_dict = {}
    for stats_index in range(0, len(match_stats), 3):
        home_team_stats = match_stats[stats_index]
        stats_name = match_stats[stats_index + 1]
        away_team_stats = match_stats[stats_index + 2]

        stats_dict[stats_name.lower().replace(
            ' ', '_') + '_home'] = [home_team_stats]
        stats_dict[stats_name.lower().replace(
            ' ', '_') + '_away'] = [away_team_stats]

    home_score = [score for score in page_content.findAll(
        'div', {'class': 'score'})[0]]
    away_score = [score for score in page_content.findAll(
        'div', {'class': 'score'})[1]]
    match_infos = [info for info in page_content.findAll(
        'div', {'class': 'scorebox_meta'})[0] if info not in words_black_list]

    geo_infos = [geo_info for geo_info in match_infos[3].findAll('small')[
        1]][0].split(', ')
    if len(geo_infos) == 3:
        stadium, state, UF = [
            geo_info for geo_info in match_infos[3].findAll('small')[1]
        ][0].split(', ')
    else:
        stadium, state = [
            geo_info for geo_info in match_infos[3].findAll('small')[1]
        ][0].split(', ')
        UF = ''

    round = [info for info in match_infos[1]][1].replace(' ', '').replace(
        '(', '').replace(')', '').replace('Matchweek', '')
    date = [info for info in match_infos[0]][2]['data-venue-date']
    time = [info for info in match_infos[0]][2]['data-venue-time']

    stats_dict['home_team'] = home_team
    stats_dict['away_team'] = away_team
    stats_dict['home_score'] = home_score
    stats_dict['away_score'] = away_score
    stats_dict['stadium'] = stadium
    stats_dict['state'] = state
    stats_dict['UF'] = UF
    stats_dict['matchweek'] = round
    stats_dict['date'] = date
    stats_dict['time'] = time
    stats_dict['match_id'] = idx

    df_match_infos = df_match_infos.append(
        pd.DataFrame(stats_dict)
    )

    team_stats, gk_stats = get_stats_tables(page_content, home_team, away_team)
    team_stats['match_id'] = idx
    gk_stats['match_id'] = idx

    match_team_stats = match_team_stats.append(team_stats)
    match_gk_stats = match_gk_stats.append(gk_stats)

    sleep(8)


with open('url_matches_list.pkl', 'wb') as f:
    pickle.dump(url_matches_list, f)

columns_names = {
    "#": "shirt_number",
    "team": "team",
    "match_id": "match_id",
    "player": "player",
    "Nation": "nation",
    "Pos": "position",
    "Min": "minutes",
    "Gls": "goals",
    "Ast": "assists",
    "PK": "penalty_kicks_made",
    "PKatt": "penalty_kicks_attempted",
    "Sh": "shots",
    "SoT": "shots_on_target",
    "CrdY": "yellow_cards",
    "CrdR": "red_cards",
    "Fls": "fouls_commited",
    "Fld": "fouls_drawn",
    "Off": "offsides",
    "Crs": "crosses",
    "TklW": "tackles_on",
    "Int": "interceptions",
    "OG": "own_goals",
    "PKwon": "penalty_kicks_won",
    "PKcon": "penalty_kicks_conceded",
    'SoTA': 'shots_on_target',
    'GA': 'goals_againts'
}

match_team_stats.columns = [columns_names[col] if col in columns_names.keys(
) else col.lower() for col in match_team_stats]
match_gk_stats.columns = [columns_names[col] if col in columns_names.keys(
) else col.lower() for col in match_gk_stats]

df_match_infos.to_csv('match_infos.csv', index=False)
match_team_stats.to_csv('match_team_stats.csv', index=False)
match_gk_stats.to_csv('match_gk_stats.csv', index=False)
all_matches.to_csv('all_matches.csv', index=False)
