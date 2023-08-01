import pyspark.sql.types as st

schema = st.StructType(
    [
        st.StructField(name="player", dataType=st.StringType(), nullable=False),
        st.StructField(name="nationality", dataType=st.StringType(), nullable=False),
        st.StructField(name="age", dataType=st.StringType(), nullable=False),
        st.StructField(name="minutes", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="gk_shots_on_target_against", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="gk_goals_against", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="gk_saves", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="gk_save_pct", dataType=st.FloatType(), nullable=True),
        st.StructField(name="dummy1", dataType=st.StringType(), nullable=True),
        st.StructField(name="gk_passes_completed_launched", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="gk_passes_launched", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="gk_passes_pct_launched", dataType=st.FloatType(), nullable=True),
        st.StructField(name="gk_passes", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="gk_passes_throws", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="gk_pct_passes_launched", dataType=st.FloatType(), nullable=True),
        st.StructField(name="gk_passes_length_avg", dataType=st.FloatType(), nullable=True),
        st.StructField(name="gk_goal_kicks", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="gk_pct_goal_kicks_launched", dataType=st.FloatType(), nullable=True),
        st.StructField(name="gk_goal_kick_length_avg", dataType=st.FloatType(), nullable=True),
        st.StructField(name="gk_crosses", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="gk_crosses_stopped", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="gk_crosses_stopped_pct", dataType=st.FloatType(), nullable=True),
        st.StructField(name="gk_def_actions_outside_pen_area", dataType=st.IntegerType(), nullable=True,),
        st.StructField(name="gk_avg_distance_def_actions", dataType=st.FloatType(), nullable=True),
        st.StructField(name="match_id", dataType=st.IntegerType(), nullable=False),
    ]
)
