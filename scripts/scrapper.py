import requests
from bs4 import BeautifulSoup
import pandas as pd
from time import sleep

FIXTURES_URL = 'https://fbref.com/en/comps/1/schedule/World-Cup-Scores-and-Fixtures'  # noqa


class Scrapper():
    _URL = "https://fbref.com/"
    _DENIED_WORDS_LIST = ['', '\xa0', '\n']
    _COLUMNS_RENAME = {
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

    def __init__(self, fixtures_url):
        self.fixtures_url = fixtures_url

    def get_match_infos(self, page_content):
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

    def get_match_stats(self, match_page_content):
        try:
            stats = match_page_content.findAll(
                'div', {'id': 'team_stats_extra'}
            )[0].findAll('div')

            match_stats = []
            for stats_divs in stats:
                for stats_infos in stats_divs:
                    if 'div' in str(stats_infos):
                        for stats_infos_divs in stats_infos:
                            if (
                                str(stats_infos_divs) not in
                                self._DENIED_WORDS_LIST
                            ):
                                match_stats.append(stats_infos_divs)
                    else:
                        if str(stats_infos) not in self._DENIED_WORDS_LIST:
                            match_stats.append(stats_infos)

            home_team = match_stats[0]
            away_team = match_stats[1]

            # match_stats = [
            #     stats for stats in match_stats
            #     if (stats != home_team and stats != away_team)
            # ]
        except Exception:
            # match_stats = []
            home_team = away_team = ''

        return home_team, away_team

    def get_stats_tables(self, match_page_content, home_team, away_team):
        gk_away_stats = pd.read_html(
            str(match_page_content.findAll('table')[-4]))[0]
        gk_away_stats.columns = [col[1] for col in gk_away_stats.columns]
        gk_away_stats['team'] = away_team

        team_away_stats = pd.read_html(
            str(match_page_content.findAll('table')[-5]))[0][:-1]
        team_away_stats.columns = [col[1] for col in team_away_stats.columns]
        team_away_stats['team'] = away_team

        gk_home_stats = pd.read_html(
            str(match_page_content.findAll('table')[-11]))[0]
        gk_home_stats.columns = [col[1] for col in gk_home_stats.columns]
        gk_home_stats['team'] = home_team

        team_home_stats = pd.read_html(
            str(match_page_content.findAll('table')[-12]))[0][:-1]
        team_home_stats.columns = [col[1] for col in team_home_stats.columns]
        team_home_stats['team'] = home_team

        gk_stats = pd.concat([gk_away_stats, gk_home_stats])
        team_stats = pd.concat([team_away_stats, team_home_stats])

        return team_stats, gk_stats

    def run(self):
        request = requests.get(self.fixtures_url)
        page_content = BeautifulSoup(request.content, 'html.parser')
        all_matches = pd.read_html(str(page_content.findAll('table')))[0]

        match_team_stats = pd.DataFrame()
        match_gk_stats = pd.DataFrame()

        url_matches_list = self.get_match_infos(page_content)
        for idx, match_url in enumerate(url_matches_list):
            request = requests.get(self._URL + match_url)
            page_content = BeautifulSoup(request.content, 'html.parser')

            home_team, away_team = (self.get_match_stats(page_content))

            team_stats, gk_stats = self.get_stats_tables(
                page_content, home_team, away_team
            )
            team_stats['match_id'] = idx
            gk_stats['match_id'] = idx

            team_stats['match_url'] = match_url
            gk_stats['match_url'] = match_url

            match_team_stats = pd.concat([match_team_stats, team_stats])
            match_gk_stats = pd.concat([match_gk_stats, gk_stats])

            sleep(8)
            break  # #################################

        match_team_stats.columns = [
            self._COLUMNS_RENAME[col] if col in self._COLUMNS_RENAME.keys()
            else col.lower() for col in match_team_stats]
        match_gk_stats.columns = [
            self._COLUMNS_RENAME[col] if col in self._COLUMNS_RENAME.keys()
            else col.lower() for col in match_gk_stats
        ]

        match_team_stats.to_csv(
            'data/match_team_stats.csv',
            index=False
        )
        match_gk_stats.to_csv(
            'data/match_gk_stats.csv',
            index=False
        )
        all_matches.to_csv(
            'data/all_matches.csv',
            index=False
        )

        print('DONE!!')
        # sleep(60 * 5)


if __name__ == "__main__":
    Scrapper(fixtures_url=FIXTURES_URL).run()
