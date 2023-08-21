from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.http import FileResponse
import openpyxl
import pandas as pd
import os

interviews = {
    "Matthieu Saussaye": {
        "job_post": "Senior AI Software Developer",
        "Infos": "Matthieu has a background in software development, with a passion for integrating AI solutions into everyday applications.",
        "Q1": "Matthieu, with your experience in software development, what inspired you to delve into AI integration?",
        "A1": "Over the years, I recognized the transformative power of AI. I saw it as the next logical step in advancing software capabilities and making applications smarter.",
        "Q2": "Can you share one of your most challenging projects involving AI?",
        "A2": "Certainly. I once worked on a project where we had to predict customer behavior in real-time. The data was vast and varied, but with a robust AI model, we could achieve an accuracy rate of over 85%.",
        "Q3": "What advice would you give to budding developers interested in AI?",
        "A3": "Stay curious and never stop learning. The field of AI is constantly evolving, and the best way to stay ahead is to continuously explore and experiment.",
        "Summarization": "The candidate provides detailed and insightful answers, showcasing their depth of knowledge in AI and software development. Matthieu confidently discusses his projects and experience, particularly his time at EPFL and Datapred. He also showcases his passion by discussing interests outside of work, such as skateboarding.\n\nPros: The candidate demonstrates fluency in AI concepts, draws upon relevant past experiences, and introduces personal interests to connect on a human level.\nCons: No significant cons noted.\nScore: 90/100"
    },
    "Paul Margain": {
        "job_post": "Sustainability Strategy Consultant",
        "Infos": "Paul is an expert in sustainable business strategies and has consulted for several Fortune 500 companies to make their operations more environmentally friendly.",
        "Q1": "Paul, what sparked your interest in sustainable business strategies?",
        "A1": "I've always been passionate about the environment. As I delved into the corporate world, I saw an opportunity to make a real difference by aligning business goals with environmental conservation.",
        "Q2": "Can you mention a project where your sustainable strategy made a significant impact?",
        "A2": "Absolutely. I worked with a leading textile firm to transition them to sustainable sourcing. This not only reduced their carbon footprint by 40% but also resulted in cost savings in the long run.",
        "Q3": "What's the biggest misconception businesses have about sustainability?",
        "A3": "Many believe it's expensive to implement sustainable practices. In reality, while there might be an initial investment, the long-term benefits in terms of cost savings, brand reputation, and environmental impact are immense.",
        "Summarization": "Paul displayed a clear and deep understanding of sustainable business strategies. He supported his claims with tangible examples, highlighting his impactful role with Fortune 500 companies. His passion for environmental conservation is evident in his responses.\n\nPros: Paul provides well-articulated and concrete examples from his past, demonstrating expertise and passion in his field.\nCons: No significant cons noted.\nScore: 95/100"
    }
}

folder = "/Users/matthieusaussaye/PycharmProjects/talentphare/surveys/"
dict_test = {}
#for file in os.listdir(folder) :
#    filepath = os.path.join(folder, file)
#    if ' ' in file :
#        with open(filepath, 'r') as f:
#            new_consumer_dict = eval(f.read())
#            dict_test[file[:-4]] = {}
#            for q in range(1,len(new_consumer_dict)):
#                if q%2 != 0:
#                    n=int((q+1)/2)
#                    dict_test[file[:-4]][f"Q{n}"] = new_consumer_dict[q]['content']
#                else :
#                    n=int(q/2)
#                    dict_test[file[:-4]][f"A{n}"] = new_consumer_dict[q]['content']

DICT_SURVEY = interviews
@login_required
def dashboard(request):
    dict_test= DICT_SURVEY
    return render(request, 'dashboard/dashboard.html',{'data':dict_test})

@login_required
def export_dict(request):
    dict_test = DICT_SURVEY

    df = pd.DataFrame.from_dict(dict_test, orient='index')

    file_path = 'dict_test.xlsx'
    df.to_excel(file_path, index=False)

    response = FileResponse(open(file_path, 'rb'), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = f'attachment; filename={file_path}'

    return response
