import pandas as pd
import os
import logging


class DataTransform():
    def __init__(self):
        self.files = os.listdir('../data/')
        self.cols_to_use = [
            'goals',
            'assists',
            'penalty_kicks_made',
            'penalty_kicks_attempted',
            'shots',
            'shots_on_target',
            'yellow_cards',
            'red_cards',
            'fouls_commited',
            'fouls_drawn',
            'offsides',
            'crosses',
            'tackles_on',
            'interceptions',
            'own_goals',
            'penalty_kicks_won',
            'penalty_kicks_conceded',
        ]
        self.cols_splitted = [
            'fouls_{}',
            'corners_{}',
            'crosses_{}',
            'touches_{}',
            'tackles_{}',
            'interceptions_{}',
            'aerials_won_{}',
            'clearances_{}',
            'offsides_{}',
            'goal_kicks_{}',
            'throw_ins_{}',
            'long_balls_{}',
            '{}_team',
            '{}_score'
        ]
        self.others_columns = [
            'stadium',
            'state',
            'UF',
            'matchweek',
            'date',
            'time',
            'match_id',
            'team',
            'position',
            'assists_sum',
            'penalty_kicks_made_sum',
            'penalty_kicks_attempted_sum',
            'shots_sum',
            'shots_on_target_sum',
            'yellow_cards_sum',
            'red_cards_sum',
            'fouls_commited_sum',
            'fouls_drawn_sum',
            'offsides_sum',
            'crosses_sum',
            'tackles_on_sum',
            'interceptions_sum',
            'own_goals_sum',
            'penalty_kicks_won_sum',
            'penalty_kicks_conceded_sum',
            'assists_mean',
            'penalty_kicks_made_mean',
            'penalty_kicks_attempted_mean',
            'shots_mean',
            'shots_on_target_mean',
            'yellow_cards_mean',
            'red_cards_mean',
            'fouls_commited_mean',
            'fouls_drawn_mean',
            'offsides_mean',
            'crosses_mean',
            'tackles_on_mean',
            'interceptions_mean',
            'own_goals_mean',
            'penalty_kicks_won_mean',
            'penalty_kicks_conceded_mean'
        ]

    def get_input_dataframes(self, files):
        serieA_table = pd.DataFrame()
        gk_stats = pd.DataFrame()
        player_stats = pd.DataFrame()
        matches = pd.DataFrame()

        logging.info('Merging match datasets from all years.')
        for df in [f for f in files if f.endswith('all_matches.csv')]:
            logging.info(f'Appending {df} dataset...')
            serieA_table = serieA_table.append(pd.read_csv(f'../data/{df}'))

        logging.info('Merging match gks datasets from all years.')
        for df in [f for f in files if f.endswith('match_gk_stats.csv')]:
            logging.info(f'Appending {df} dataset...')
            gk_stats = gk_stats.append(pd.read_csv(f'../data/{df}'))

        logging.info('Merging match players datasets from all years.')
        for df in [f for f in files if f.endswith('match_team_stats.csv')]:
            logging.info(f'Appending {df} dataset...')
            player_stats = player_stats.append(pd.read_csv(f'../data/{df}'))

        logging.info('Merging match infos datasets from all years.')
        for df in [f for f in files if f.endswith('match_infos.csv')]:
            logging.info(f'Appending {df} dataset...')
            matches = matches.append(pd.read_csv(f'../data/{df}'))

        return serieA_table, gk_stats, player_stats, matches

    def transform_player_positions(self, player_stats):
        # Spliting position into defense, mid and attack
        player_stats['position'] = (player_stats['position']
                                    .replace('CB', 'Defensive')
                                    .replace('LB', 'Defensive')
                                    .replace('RB', 'Defensive')
                                    .replace('LWB', 'Defensive')
                                    .replace('RWB', 'Defensive')
                                    .replace('SW', 'Defensive')
                                    .replace('DF', 'Defensive')
                                    .replace('DF,MF', 'Defensive')
                                    .replace('DF,FW', 'Defensive'))

        player_stats['position'] = (player_stats['position']
                                    .replace('DM', 'Midfielder')
                                    .replace('CM', 'Midfielder')
                                    .replace('AM', 'Midfielder')
                                    .replace('LM', 'Midfielder')
                                    .replace('RM', 'Midfielder')
                                    .replace('MF', 'Midfielder'))

        player_stats['position'] = (player_stats['position']
                                    .replace('CF', 'Offensive')
                                    .replace('S', 'Offensive')
                                    .replace('SS', 'Offensive')
                                    .replace('FW', 'Offensive')
                                    .replace('LW', 'Offensive')
                                    .replace('RW', 'Offensive')
                                    .replace('FW,MF', 'Offensive'))

        player_stats.drop(
            player_stats[player_stats['position'].isna()].index, inplace=True
        )

        return player_stats

    def get_information_by_player_area(self, player_stats):
        player_stats_sum = (player_stats
                            .groupby(
                                ['team', 'match_id', 'position']
                            )[self.cols_to_use]
                            .sum()
                            .reset_index()
                            )
        player_stats_mean = (player_stats
                             .groupby(
                                 ['team', 'match_id', 'position']
                             )[self.cols_to_use]
                             .mean()
                             .reset_index()
                             )

        player_stats_sum.columns = [
            col + '_sum' if col in self.cols_to_use else col
            for col in player_stats_sum.columns
        ]
        player_stats_mean.columns = [
            col + '_mean' if col in self.cols_to_use else col
            for col in player_stats_mean.columns
        ]

        player_stats = player_stats_sum.merge(
            player_stats_mean, on=['team', 'match_id', 'position'])

        return player_stats

    def transform_df_matchs(self, df):
        df['year'] = df['date'].apply(
            lambda date: int(date.split('-')[0])
        )

        df_matches = pd.DataFrame()
        for match_id in df['match_id'].unique():
            for year in df['year'].unique():
                try:
                    df_match = df[
                        (df['match_id'] == match_id) &
                        (df['year'] == year)
                    ]

                    home_team = df_match[
                        [col.format('home') for col in self.cols_splitted] +
                        ['team', 'match_id', 'away_score']
                    ].copy()
                    home_team['result'] = (
                        'W' if (
                            home_team['home_score'].iloc[0] >
                            home_team['away_score'].iloc[0]
                        )
                        else (
                            'L' if (
                                home_team['home_score'].iloc[0] <
                                home_team['away_score'].iloc[0]
                            )
                            else 'D'
                        )
                    )

                    home_team = home_team[
                        home_team['team'] == home_team['home_team'].iloc[0]
                    ].copy()
                    home_team.drop(
                        columns=['home_team', 'away_score'], inplace=True)
                    home_team.columns = [
                        col.replace('_home', '').replace('home_', '')
                        for col in home_team.columns
                    ]

                    away_team = df_match[
                        [col.format('away') for col in self.cols_splitted] +
                        ['team', 'match_id', 'home_score']
                    ].copy()
                    away_team['result'] = (
                        'W' if (
                            away_team['away_score'].iloc[0] >
                            away_team['home_score'].iloc[0]
                        )
                        else (
                            'L' if (
                                away_team['away_score'].iloc[0] <
                                away_team['home_score'].iloc[0]
                            )
                            else 'D'
                        )
                    )
                    away_team = away_team[
                        away_team['team'] == away_team['away_team'].iloc[0]
                    ].copy()
                    away_team.drop(
                        columns=['away_team', 'home_score'], inplace=True)
                    away_team.columns = [
                        col.replace('_away', '').replace('away_', '')
                        for col in away_team.columns
                    ]

                    df_matches = df_matches.append(
                        home_team.append(away_team)
                    )
                except:  # noqa
                    pass

        return df_matches

    def group_player_positions_informations(self, df_final):
        position_cols = [
            col for col in df_final.columns if 'sum' in col or 'mean' in col
        ]
        position_stats = df_final[
            position_cols + ['position', 'match_id', 'team']
        ]

        defense_informations = position_stats[
            position_stats['position'] == 'Defensive'
        ].copy()
        defense_informations.drop(columns=['position'], inplace=True)
        defense_informations.columns = [
            col + '_defense'
            if col in position_cols else col
            for col in defense_informations.columns
        ]

        mid_informations = position_stats[
            position_stats['position'] == 'Midfielder'
        ].copy()

        mid_informations.drop(columns=['position'], inplace=True)
        mid_informations.columns = [
            col + '_mid' if col in position_cols else col
            for col in mid_informations.columns
        ]

        attack_informations = position_stats[
            position_stats['position'] == 'Offensive'
        ].copy()

        attack_informations.drop(columns=['position'], inplace=True)
        attack_informations.columns = [
            col + '_attack' if col in position_cols else col
            for col in attack_informations.columns
        ]

        df_final.drop(columns=position_cols + ['position'], inplace=True)

        df_final.drop_duplicates(inplace=True)

        df_final = df_final.merge(
            defense_informations, on=['team', 'match_id'])
        df_final = df_final.merge(mid_informations, on=['team', 'match_id'])
        df_final = df_final.merge(attack_informations, on=['team', 'match_id'])

        return df_final

    def run(self):
        (
            serieA_table, gk_stats, player_stats, matches
        ) = self.get_input_dataframes(self.files)

        player_stats = self.transform_player_positions(player_stats)
        player_stats = self.get_information_by_player_area(player_stats)

        df = matches.merge(player_stats, on='match_id')
        df_matches = self.transform_df_matchs(df)

        df_players = df[self.others_columns].copy()
        df_final = df_matches.merge(
            df_players, on=['match_id', 'team']
        ).drop_duplicates()

        df_final = self.group_player_positions_informations(df_final)

        df_final['year'] = df_final['date'].apply(
            lambda date: date.split('-')[0]
        )

        df_final.to_csv('../data/serieA_matches.csv', index=False)


if __name__ == "__main__":
    DataTransform().run()
