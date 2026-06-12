import joblib
import pandas as pd

model = joblib.load('ml/modelo_turnover.pkl')

template = pd.read_csv('data/ibm_attrition.csv', nrows=1)
if 'Attrition' in template.columns:
    template = template.drop(columns=['Attrition'])

plant = {
    'Salário Base': 6000,
    'Age': 35,
    'BusinessTravel': 'Travel_Rarely',
    'Department': 'Sales',
    'Gender': 'Female',
    'JobRole': 'Sales Executive',
    'MaritalStatus': 'Single',
    'OverTime': 'Yes'
}

for pct in [0, 2, 5, 8, 10, 12, 15]:
    df = template.copy()
    df.at[df.index[0], 'MonthlyIncome'] = int(round(plant['Salário Base'] * (1 + pct / 100)))
    df.at[df.index[0], 'Age'] = plant['Age']
    df.at[df.index[0], 'BusinessTravel'] = plant['BusinessTravel']
    df.at[df.index[0], 'Department'] = plant['Department']
    df.at[df.index[0], 'Gender'] = plant['Gender']
    df.at[df.index[0], 'JobRole'] = plant['JobRole']
    df.at[df.index[0], 'MaritalStatus'] = plant['MaritalStatus']
    df.at[df.index[0], 'OverTime'] = plant['OverTime']
    if 'PercentSalaryHike' in df.columns:
        df.at[df.index[0], 'PercentSalaryHike'] = pct

    prob = float(model.predict_proba(df)[0][1])
    print(f'Pct {pct:>2}% -> {prob:.6f}')
