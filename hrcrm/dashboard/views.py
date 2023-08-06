from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.http import FileResponse
import openpyxl
import pandas as pd
@login_required
def dashboard(request):
    with open('/Users/matthieusaussaye/PycharmProjects/talentphare/surveys/user1.txt', 'r') as f:
        new_consumer_dict = eval(f.read())
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
        },
        "new_consumer": {
            "Infos": "",
            "Q1": "What is for you Brio Maté ?",
            "A1": new_consumer_dict[1]['content'],
            "Q2": new_consumer_dict[2]['content'],
            "A2": new_consumer_dict[3]['content'],
            "Q3": new_consumer_dict[4]['content'],
            "A3": new_consumer_dict[5]['content'],
    }}

    return render(request, 'dashboard/dashboard.html',{'data':dict_test})


@login_required
def export_dict(request):
    with open('/Users/matthieusaussaye/PycharmProjects/talentphare/surveys/user1.txt', 'r') as f:
        new_consumer_dict = eval(f.read())
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
        },
        "new_consumer": {
            "Infos": "",
            "Q1": "What is for you Brio Maté ?",
            "A1": new_consumer_dict[1]['content'],
            "Q2": new_consumer_dict[2]['content'],
            "A2": new_consumer_dict[3]['content'],
            "Q3": new_consumer_dict[4]['content'],
            "A3": new_consumer_dict[5]['content']}

    }

    df = pd.DataFrame.from_dict(dict_test, orient='index')

    file_path = 'dict_test.xlsx'
    df.to_excel(file_path, index=False)

    response = FileResponse(open(file_path, 'rb'), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = f'attachment; filename={file_path}'

    return response