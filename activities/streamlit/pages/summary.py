import streamlit as st
import pandas as pd

st.markdown("# Main page")



df = pd.read_parquet('nyc.parquet').head(100)

data = st.data_editor(df)


st.dataframe(data)


# st.balloons()

