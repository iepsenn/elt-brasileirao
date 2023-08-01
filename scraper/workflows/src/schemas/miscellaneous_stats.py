import pyspark.sql.types as st

schema = st.StructType(
    [
        st.StructField(name="player_name", dataType=st.StringType(), nullable=False),
        st.StructField(name="shirtnumber", dataType=st.StringType(), nullable=False),
        st.StructField(name="nationality", dataType=st.StringType(), nullable=False),
        st.StructField(name="position", dataType=st.StringType(), nullable=False),
        st.StructField(name="age", dataType=st.StringType(), nullable=False),
        st.StructField(name="minutes", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="cards_yellow", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="cards_red", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="cards_yellow_red", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="fouls", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="fouled", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="offsides", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="crosses", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="interceptions", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="tackles_won", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="pens_won", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="penalties_conceded", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="own_goals", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="ball_recoveries", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="aerials_won", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="aerials_lost", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="aerials_won_pct", dataType=st.FloatType(), nullable=True),
        st.StructField(name="match_id", dataType=st.IntegerType(), nullable=False),
    ]
)
