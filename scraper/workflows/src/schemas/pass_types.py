import pyspark.sql.types as st

schema = st.StructType(
    [
        st.StructField(name="player_name", dataType=st.StringType(), nullable=False),
        st.StructField(name="shirtnumber", dataType=st.StringType(), nullable=False),
        st.StructField(name="nationality", dataType=st.StringType(), nullable=False),
        st.StructField(name="position", dataType=st.StringType(), nullable=False),
        st.StructField(name="age", dataType=st.StringType(), nullable=False),
        st.StructField(name="minutes", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="passes", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="passes_live", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="passes_dead", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="passes_free_kicks", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="through_balls", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="passes_switches", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="crosses", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="throw_ins", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="corner_kicks", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="corner_kicks_in", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="corner_kicks_out", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="corner_kicks_straight", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="passes_completed", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="passes_offsides", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="passes_blocked", dataType=st.IntegerType(), nullable=True),
        st.StructField(name="match_id", dataType=st.IntegerType(), nullable=False),
    ]
)
