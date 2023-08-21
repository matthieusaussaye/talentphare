#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 18 09:02:05 2023

@author: paulmargain
"""

import os
import numpy as np
import streamlit as st
from io import BytesIO
import streamlit.components.v1 as components
import openai
import tempfile
import wave
import time
from st_custom_components import st_audiorec
import wave
import io
import os.path


openai.api_key = "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"
def bytes_to_wav(audio_bytes, output_filename, sample_width=2, frame_rate=44100, channels=2):
    with wave.open(output_filename, 'wb') as wav_file:
        wav_file.setnchannels(channels)
        wav_file.setsampwidth(sample_width)
        wav_file.setframerate(frame_rate)
        wav_file.writeframes(audio_bytes)

messages=[]
name,age=None,None
audio_bytes=None
first_question = True

if "disabled" not in st.session_state:
    st.session_state["disabled"] = False

if len(messages) == 0:
    st.write("Drop me your name and your age please :)")
    col1, col2 = st.columns(2)
    with col1:
        name = st.text_input("Your Name & Surname ",
                             disabled=st.session_state.disabled)
    with col2:
        age = st.text_input("Age",
                            disabled=st.session_state.disabled)
# Preparing the interview
if age and name :
    st.session_state["disabled"] = True
    audio_bytes = st_audiorec()
    with open('surveys/prompt_Brio_demo.txt', 'r') as f:
        messages = eval(f.read())
        messages.append({"role": "assistant", "content": "What is for you Brio Maté ?"})
    if not os.path.isfile(f'surveys/{name}.txt'):
        with open(f'surveys/{name}.txt', 'w') as f:
            f.write(str(messages))
        with open(f'surveys/{name}.txt', 'r') as f:
            messages = eval(f.read())
    if os.path.isfile(f'surveys/{name}.txt'):
        with open(f'surveys/{name}.txt', 'r') as f:
            messages = eval(f.read())
            st.write(messages[len(messages)-1]['content'])


if audio_bytes is not None and len(messages) < 11:
    # display audio data as received on the backend
    # st.audio(audio_bytes, format='audio/wav')
    # Now use the function
    bytes_to_wav(audio_bytes, 'output.wav')  # Replace audio_bytes with your audio data
    # The name of the .wav file
    filename = 'output.wav'
    # Open the .wav file
    wav_audio_data = open(filename, "rb")
    transcript = openai.Audio.transcribe("whisper-1", wav_audio_data)
    messages.append({"role": "user", "content": transcript["text"]})
    st.write(transcript["text"])
    response = openai.ChatCompletion.create(model="gpt-3.5-turbo",
                                            messages=messages)
    #st.write(response['choices'][0]['message']['content'])
    messages.append({"role": "assistant", "content": response['choices'][0]['message']['content']})
    st.write(messages[len(messages)-1]['content'])
    with open(f'surveys/{name}.txt', 'w') as f:
        f.write(str(messages))



elif len(messages) >= 11 :
    st.write("This is the end of the survey - Thanks a lot for your time !")

