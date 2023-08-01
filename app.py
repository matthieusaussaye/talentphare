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

def bytes_to_wav(audio_bytes, output_filename, sample_width=2, frame_rate=44100, channels=2):
    with wave.open(output_filename, 'wb') as wav_file:
        wav_file.setnchannels(channels)
        wav_file.setsampwidth(sample_width)
        wav_file.setframerate(frame_rate)
        wav_file.writeframes(audio_bytes)

messages=[]
first_input = True
st.write("What is for you Brio Maté ?")
audio_bytes = st_audiorec()
openai.api_key = "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"
if audio_bytes is not None and len(messages) < 5:
    # display audio data as received on the backend
    #st.audio(audio_bytes, format='audio/wav')
    # Now use the function
    bytes_to_wav(audio_bytes, 'output.wav')  # Replace audio_bytes with your audio data
    
    # The name of the .wav file
    filename = 'output.wav'
    # Open the .wav file
    wav_audio_data = open(filename, "rb")
    
    with open('surveys/prompt_Brio_demo.txt', 'r') as f:
        messages = eval(f.read())
        with open('surveys/user1.txt', 'r') as f:
            messages = eval(f.read())
    transcript = openai.Audio.transcribe("whisper-1", wav_audio_data)
    messages.append( {"role": "user", "content": transcript["text"]})
   
    #st.write(transcript["text"])
    
    response=openai.ChatCompletion.create(model="gpt-3.5-turbo",
                                          messages=messages)
   
    #st.write(response['choices'][0]['message']['content'])
    messages.append( {"role": "assistant", "content": response['choices'][0]['message']['content']})
    for k in range(2,len(messages)):
        st.write(messages[k]['content'])
    with open('surveys/user1.txt', 'w') as f:
        f.write(str(messages))
