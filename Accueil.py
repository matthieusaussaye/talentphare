import streamlit as st
from streamlit_option_menu import option_menu
from app_utils import switch_page
import streamlit as st
from PIL import Image

# ————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————
im_icon=Image.open("icc.png")
im = Image.open("idiap.jpeg")
#st.image(im,width=50)
st.set_page_config(page_title = "AI Interviewer", layout = "centered",page_icon=im_icon)

home_title = "Smart Interview"
home_introduction = "Welcome to HR.AI, empowering your candidate screening with AI."

with st.sidebar:
    st.markdown("IDIAP Create Challenge 2023")
    

st.markdown(
    "<style>#MainMenu{visibility:hidden;}</style>",
    unsafe_allow_html=True
)
st.image(im, width=700)
st.markdown(f"""# {home_title} <span style=color:#2E9BF5><font size=5>Beta</font></span>""",unsafe_allow_html=True)

st.markdown("""\n""")
#st.markdown("#### Greetings")
st.markdown("""\n""")

st.markdown("#### Valorisez votre candidature en répondant à nos 3 questions:")

st.markdown("""\n""")
selected = option_menu(
        menu_title= None,
        options=["Culture", "Expérience", "Technique"],
        icons = ["cast", "cloud-upload", "cast"],
        default_index=0,
        orientation="horizontal",
    )
if selected == 'Technique':
    st.info("""
        Nous évaluerons vos compétences techniques par rapport à la description du poste.""")
        
    if st.button("Commencer l'entretien !"):
        switch_page("Technique")

if selected == 'Expérience':
    st.info("""
    Nous examinerons votre CV et discuterons de vos expériences passées.""")
    
    if st.button("Commencer l'entretien !"):
        switch_page("Experience")

if selected == 'Culture':
    st.info("""
    Nous évaluerons vos compétences interpersonnelles par rapport à la description du poste.""")
    
    if st.button("Commencer l'entretien !"):
        switch_page("Culture")


# ——————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————


#st.write(
#        f'<iframe src="https://17nxkr0j95z3vy.embednotionpage.com/AI-Interviewer-Wiki-8d962051e57a48ccb304e920afa0c6a8" style="width:100%; height:100%; min-height:500px; border:0; padding:0;"/>',
#        unsafe_allow_html=True,
#    )