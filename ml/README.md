# Treinamento de Modelo de Turnover

Este guia descreve como executar o script `ml/treinar_modelo.py`, que treina um modelo de classificação para prever turnover (Attrition) usando o dataset IBM HR Analytics.

## Requisitos

- Python 3.11+ ou 3.14
- Ambiente virtual ativo (recomendado)
- Dependências instaladas:
  - `pandas`
  - `scikit-learn`
  - `flaml`
  - `joblib`

## Instalação de dependências

```bash
pip install pandas scikit-learn flaml joblib
```

## Execução do script

```bash
python ml/treinar_modelo.py
```

## O que o script faz

1. baixa o dataset público IBM HR Analytics Employee Attrition & Performance
2. transforma a coluna `Attrition` de `Yes`/`No` para `1`/`0`
3. divide os dados em treino e teste (80/20)
4. treina um modelo usando `flaml.AutoML` com `time_budget=60`
5. imprime as métricas de avaliação:
   - Accuracy
   - Precision
   - Recall
   - F1-Score
   - ROC-AUC
6. salva o pipeline treinado em `modelo_turnover.pkl`

## Uso em produção

O modelo salvo em `modelo_turnover.pkl` pode ser carregado com `joblib.load` para fazer previsões em novos dados.
