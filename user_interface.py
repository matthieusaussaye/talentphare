import streamlit as st
import pandas as pd
import numpy as np
from audiorecorder import audiorecorder
import os
from io import BytesIO
import streamlit.components.v1 as components
from st_custom_components import st_audiorec

st.set_page_config(layout="wide")
# DESIGN implement changes to the standard streamlit UI/UX
# Design move app further up and remove top padding
st.markdown('''<style>.css-1egvi7u {margin-top: -3rem;}</style>''',
            unsafe_allow_html=True)
# Design change st.Audio to fixed height of 45 pixels
st.markdown('''<style>.stAudio {height: 45px;}</style>''',
            unsafe_allow_html=True)
# Design change hyperlink href link color
st.markdown('''<style>.css-v37k9u a {color: #ff4c4b;}</style>''',
            unsafe_allow_html=True)  # darkmode
st.markdown('''<style>.css-nlntq9 a {color: #ff4c4b;}</style>''',
            unsafe_allow_html=True)  # lightmode

#######################################################################################################################################################
def generate_audio_form():
    q = 1
    question_holder = st.empty()
    wav_audio_data = st_audiorec()
    question_holder.title(f'Question {q}')
    if wav_audio_data is not None :
        wav_file = open(f"audio{q}.wav","wb")
        wav_file.write(wav_audio_data)
        save=True

def generate_text_form():
    q1 = st.text_input("Q 1")
    q2 = st.text_input("Q 2")
    q3 = st.text_input("Q 3")
    if q1 and q2 and q3:
        button = st.button(label="submit")
        if button:
            user_response = {
                "Q 1": q1,
                "Q 2": q2,
                "Q 3": q3,
            }

st.sidebar.title("Navigation")
page = st.sidebar.radio("Select a page:", ("Generate Audio Form","Dashboard evaluation"))

if page == "Generate Audio Form":
    generate_audio_form()

elif page == "Generate Text Form":
    generate_text_form()
