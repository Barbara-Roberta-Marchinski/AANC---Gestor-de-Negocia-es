"""Script para treinar um modelo de classificação de Turnover usando IBM HR Analytics.

Este script utiliza pandas, scikit-learn e flaml para treinar um modelo de classificação
que prevê a coluna alvo 'Attrition'. O modelo treinado é salvo em 'modelo_turnover.pkl'.
"""

from pathlib import Path
import joblib
import pandas as pd
from flaml.automl import AutoML
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline


def carregar_dataset():
    """Carrega o dataset IBM HR Analytics Employee Attrition & Performance localmente."""
    base_dir = Path(__file__).resolve().parent.parent
    caminho_arquivo = base_dir / 'data' / 'ibm_attrition.csv'
    print(f"Carregando dados de: {caminho_arquivo}")
    df = pd.read_csv(caminho_arquivo)
    return df


def preprocessar_dados(df):
    """Pré-processa dados transformando o target e preparando features numéricas e categóricas."""
    if 'Attrition' not in df.columns:
        raise ValueError("A coluna 'Attrition' não foi encontrada no dataset.")

    df = df.copy()
    df['Attrition'] = df['Attrition'].map({'Yes': 1, 'No': 0})
    if df['Attrition'].isnull().any():
        raise ValueError("Foram encontrados valores inesperados em 'Attrition'. Use apenas Yes/No.")

    # Separar features e target
    X = df.drop(columns=['Attrition'])
    y = df['Attrition']

    # Identificar colunas numéricas e categóricas
    numeric_features = X.select_dtypes(include=['int64', 'float64']).columns.tolist()
    categorical_features = X.select_dtypes(include=['object', 'category', 'string']).columns.tolist()

    # Transformações básicas para variáveis categóricas (Atualizado sparse_output)
    preprocessor = ColumnTransformer(
        transformers=[
            ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), categorical_features),
        ],
        remainder='passthrough'
    )

    return X, y, preprocessor


def treinar_modelo(X_train, y_train, preprocessor):
    """Treina um modelo de classificação usando FLAML AutoML."""
    automl = AutoML()
    automl_settings = {
        'time_budget': 60,  # 60 segundos de treinamento
        'task': 'classification',
        'metric': 'f1',
        'log_file_name': 'flaml_turnover.log',
        # 'verbosity': 0,
    }

    print('Iniciando treino com FLAML AutoML (Isso vai levar 60 segundos)...')
    X_train_preprocessed = preprocessor.fit_transform(X_train)
    automl.fit(X_train=X_train_preprocessed, y_train=y_train, **automl_settings)
    print('Treinamento concluído.')

    pipeline_final = Pipeline(
        steps=[
            ('preprocessamento', preprocessor),
            ('automl', automl),
        ]
    )

    return pipeline_final


def avaliar_modelo(pipeline, X_test, y_test):
    """Avalia o modelo no conjunto de teste e calcula métricas relevantes."""
    y_pred = pipeline.predict(X_test)
    y_prob = pipeline.predict_proba(X_test)[:, 1] if hasattr(pipeline, 'predict_proba') else None

    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, zero_division=0)
    recall = recall_score(y_test, y_pred, zero_division=0)
    f1 = f1_score(y_test, y_pred, zero_division=0)
    roc_auc = roc_auc_score(y_test, y_prob) if y_prob is not None else float('nan')

    print('\n===== Avaliação do Modelo no Conjunto de Teste =====')
    print(f'Accuracy : {accuracy:.4f}')
    print(f'Precision: {precision:.4f}')
    print(f'Recall   : {recall:.4f}')
    print(f'F1-Score : {f1:.4f}')
    print(f'ROC-AUC  : {roc_auc:.4f}')

    return {
        'accuracy': accuracy,
        'precision': precision,
        'recall': recall,
        'f1_score': f1,
        'roc_auc': roc_auc,
    }


def salvar_modelo(pipeline, caminho=None):
    """Salva o pipeline treinado em disco usando joblib."""
    if caminho is None:
        caminho = Path(__file__).resolve().parent / 'modelo_turnover.pkl'
    joblib.dump(pipeline, caminho)
    print(f'\nModelo salvo em: {caminho}')


if __name__ == '__main__':
    print('Iniciando rotina de treinamento de modelo de turnover...')

    df = carregar_dataset()
    X, y, preprocessor = preprocessar_dados(df)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.20, random_state=42, stratify=y
    )

    pipeline = treinar_modelo(X_train, y_train, preprocessor)
    metrics = avaliar_modelo(pipeline, X_test, y_test)
    salvar_modelo(pipeline)

    print('\n===== Justificativa de Negócio =====')
    print(
        'Em People Analytics, controlar o turnover é crucial. O Recall é essencial porque um falso negativo '
        'significa não identificar um colaborador que pode deixar a empresa, o que representa perda de talento e '
        'custos de substituição e recontratação. O F1-Score equilibra Recall e Precision, trazendo confiança de que '
        'o modelo identifica bem os casos de saída sem gerar excesso de alarmes falsos.'
    )
    print('Portanto, em RH, priorizamos Recall e F1-Score para minimizar o risco de perder pessoas em risco de attrition.')