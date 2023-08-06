from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.http import FileResponse
import openpyxl
import pandas as pd
import os

dict_test = {
        "Matthieu Saussaye": {
            "Infos": "Matthieu is a regular consumer of Brio Maté and enjoys its unique flavor.",
            "Q1": "What are the main ingredients in Brio Maté?",
            "A1": "Brio Maté is primarily made from yerba mate, water, and various natural flavorings.",
            "Q2": "What are the health benefits of Brio Maté?",
            "A2": "Brio Maté can boost energy levels, help with weight loss, and provide various antioxidants.",
            "Q3": "How is Brio Maté produced?",
            "A3": "Brio Maté is made by steeping yerba mate in hot water, then adding various other ingredients for flavor.",
        },
        "Paul Margain": {
            "Infos": "Paul recently started drinking Brio Maté and is curious about its production process.",
            "Q1": "What makes Brio Maté different from other beverages?",
            "A1": "Brio Maté is unique because of its blend of yerba mate and natural flavorings, providing a refreshing and energizing experience.",
            "Q2": "Is Brio Maté suitable for vegans?",
            "A2": "Yes, Brio Maté is suitable for vegans. It doesn't contain any animal products.",
            "Q3": "Where can I buy Brio Maté?",
            "A3": "Brio Maté is available in various grocery stores and online marketplaces.",
        }}

folder = "/Users/matthieusaussaye/PycharmProjects/talentphare/surveys/"
for file in os.listdir(folder) :
    filepath = os.path.join(folder, file)
    if ' ' in file :
        with open(filepath, 'r') as f:
            new_consumer_dict = eval(f.read())
            dict_test[file[:-4]] = {}
            for q in range(1,len(new_consumer_dict)):
                if q%2 != 0:
                    n=int((q+1)/2)
                    dict_test[file[:-4]][f"Q{n}"] = new_consumer_dict[q]['content']
                else :
                    n=int(q/2)
                    dict_test[file[:-4]][f"A{n}"] = new_consumer_dict[q]['content']

DICT_SURVEY = dict_test
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