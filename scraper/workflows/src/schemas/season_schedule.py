import pyspark.sql.types as st

schema = st.StructType([
    st.StructField(name="season_week", dataType=st.IntegerType(), nullable=False),
    st.StructField(name="day", dataType=st.StringType(), nullable=False),
    st.StructField(name="date", dataType=st.StringType(), nullable=False),
    st.StructField(name="time", dataType=st.StringType(), nullable=True),
    st.StructField(name="home_team", dataType=st.StringType(), nullable=False),
    st.StructField(name="dummy1", dataType=st.StringType(), nullable=True),
    st.StructField(name="score", dataType=st.StringType(), nullable=True),
    st.StructField(name="dummy2", dataType=st.StringType(), nullable=True),
    st.StructField(name="away_team", dataType=st.StringType(), nullable=False),
    st.StructField(name="attendance", dataType=st.IntegerType(), nullable=True),
    st.StructField(name="venue", dataType=st.StringType(), nullable=True),
    st.StructField(name="referee", dataType=st.StringType(), nullable=True),
    st.StructField(name="dummy3", dataType=st.StringType(), nullable=True),
    st.StructField(name="dummy4", dataType=st.StringType(), nullable=True),
    st.StructField(name="url", dataType=st.StringType(), nullable=True),
])
