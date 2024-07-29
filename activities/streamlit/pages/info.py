import streamlit as st
import pandas as pd
import numpy as np


number = st.number_input('Enter a number')

st.write('The number is ', number)

token = st.text_input("Enter a password", type="password")

name =  st.text_input("Enter a your name")

if len(name) > 0:
    st.write(f'Hello {name} your password is {token}')


map_data = pd.DataFrame(
    np.random.randn(1000, 2) / [50, 50] + [37.76, -122.4],
    columns=['lat', 'lon'])

st.map(map_data)