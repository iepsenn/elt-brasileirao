import pyspark.sql.types as st

schema = st.StructType(
    [
        st.StructField(name="player_name", dataType=st.StringType(), nullable=False),
        st.StructField(name="shirtnumber", dataType=st.StringType(), nullable=False),
        st.StructField(name="nationality", dataType=st.StringType(), nullable=False),
        st.StructField(name="position", dataType=st.StringType(), nullable=False),
        st.StructField(name="age", dataType=st.StringType(), nullable=True),
        st.StructField(name="minutes", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="tackles", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="tackles_won", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="tackles_defense_3rd", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="tackles_mid_3rd", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="tackles_attack_3rd", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="challenge_tackles", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="challenges", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="challenge_tackles_pct", dataType=st.FloatType(), nullable=True),
        st.StructField(name="challenges_lost", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="blocks", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="blocked_shots", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="blocked_passes", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="interceptions", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="tackles_interceptions", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="clearances", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="errors", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="match_id", dataType=st.IntegerType(), nullable=False),
    ]
)
