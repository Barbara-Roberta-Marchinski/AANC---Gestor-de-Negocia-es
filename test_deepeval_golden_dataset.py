import csv
import json
from pathlib import Path

from src.deepeval_evaluation import MockJudge, load_golden_dataset, run_golden_dataset_evaluation


class DummyChatbot:
    def ask(self, question: str):
        return f"Resposta simulada para: {question}", ["contexto de teste"]


def test_golden_dataset_evaluation_runs_successfully():
    dataset_path = Path("data/deepeval_golden_dataset.json")
    dataset = load_golden_dataset(dataset_path)

    assert len(dataset) >= 4

    results = run_golden_dataset_evaluation(dataset_path)

    assert results["total_cases"] == len(dataset)
    assert results["passed_cases"] == len(dataset)
    assert results["pass_rate"] == 1.0


def test_golden_dataset_evaluation_uses_chatbot_output_and_generates_report():
    dataset_path = Path("data/deepeval_golden_dataset.json")
    dataset = load_golden_dataset(dataset_path)

    results = run_golden_dataset_evaluation(
        dataset_path,
        chatbot=DummyChatbot(),
        judge=MockJudge(),
    )

    assert results["total_cases"] == len(dataset)
    assert results["results"][0]["question"] == dataset[0]["input"]
    assert results["results"][0]["actual_output"] == f"Resposta simulada para: {dataset[0]['input']}"
    assert results["results"][0]["retrieval_context"] == ["contexto de teste"]
    assert "approved" in results["results"][0]["evaluation"]


def test_golden_dataset_evaluation_writes_report_files(tmp_path):
    dataset_path = Path("data/deepeval_golden_dataset.json")

    results = run_golden_dataset_evaluation(
        dataset_path,
        chatbot=DummyChatbot(),
        judge=MockJudge(),
        output_dir=tmp_path,
        verbose=False,
    )

    report_path = tmp_path / "evaluation_report.csv"
    summary_path = tmp_path / "evaluation_summary.json"

    assert report_path.exists()
    assert summary_path.exists()

    with report_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == results["total_cases"]

    with summary_path.open("r", encoding="utf-8") as handle:
        summary = json.load(handle)

    assert summary["total_cases"] == results["total_cases"]
    assert summary["pass_rate"] == results["pass_rate"]
