import pandas as pd


serieA_table = pd.read_csv('../data/all_matches.csv')
gk_stats = pd.read_csv('../data/match_gk_stats.csv')
player_stats = pd.read_csv('../data/match_team_stats.csv')
matches = pd.read_csv('../data/match_infos.csv')


# Spliting position into defense, mid and attack
player_stats['position'] = (player_stats['position']
                            .replace('CB', 'Defensive')
                            .replace('LB', 'Defensive')
                            .replace('RB', 'Defensive')
                            .replace('LWB', 'Defensive')
                            .replace('RWB', 'Defensive')
                            .replace('SW', 'Defensive')
                            .replace('DF', 'Defensive')
                            .replace('DF,MF', 'Defensive'))

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
    player_stats[player_stats['position'].isna()].index, inplace=True)


# Group and extract informations

cols_to_use = [
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

player_stats_sum = player_stats.groupby(['team', 'match_id', 'position'])[
    cols_to_use].sum().reset_index()
player_stats_mean = player_stats.groupby(['team', 'match_id', 'position'])[
    cols_to_use].mean().reset_index()

player_stats_sum.columns = [
    col + '_sum' if col in cols_to_use else col
    for col in player_stats_sum.columns
]
player_stats_mean.columns = [
    col + '_mean' if col in cols_to_use else col
    for col in player_stats_mean.columns
]

player_stats = player_stats_sum.merge(
    player_stats_mean, on=['team', 'match_id', 'position'])


# Merge all informations
df = matches.merge(player_stats, on='match_id')
cols_splitted = [
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

others_columns = [
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

df_matches = pd.DataFrame()
for match_id in df['match_id'].unique():
    df_match = df[df['match_id'] == match_id]

    home_team = df_match[
        [col.format('home') for col in cols_splitted] +
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
    home_team = home_team[home_team['team'] ==
                          home_team['home_team'].iloc[0]].copy()
    home_team.drop(columns=['home_team', 'away_score'], inplace=True)
    home_team.columns = [col.replace('_home', '').replace(
        'home_', '') for col in home_team.columns]

    away_team = df_match[
        [col.format('away') for col in cols_splitted] +
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
    away_team = away_team[away_team['team'] ==
                          away_team['away_team'].iloc[0]].copy()
    away_team.drop(columns=['away_team', 'home_score'], inplace=True)
    away_team.columns = [col.replace('_away', '').replace(
        'away_', '') for col in away_team.columns]

    df_matches = df_matches.append(
        home_team.append(away_team)
    )

df_players = df[others_columns].copy()

df_final = df_matches.merge(
    df_players, on=['match_id', 'team']).drop_duplicates()

position_cols = [
    col for col in df_final.columns if 'sum' in col or 'mean' in col]
position_stats = df_final[position_cols + ['position', 'match_id', 'team']]

defense_informations = position_stats[
    position_stats['position'] == 'Defensive'
].copy()
defense_informations.drop(columns=['position'], inplace=True)
defense_informations.columns = [
    col + '_defense'
    if col in position_cols else col
    for col in defense_informations.columns
]

mid_informations = position_stats[position_stats['position']
                                  == 'Midfielder'].copy()
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

df_final = df_final.merge(defense_informations, on=['team', 'match_id'])
df_final = df_final.merge(mid_informations, on=['team', 'match_id'])
df_final = df_final.merge(attack_informations, on=['team', 'match_id'])

df_final.to_csv('serieA_matches.csv', index=False)
