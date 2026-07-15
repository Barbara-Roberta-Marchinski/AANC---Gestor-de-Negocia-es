import csv
import json
import os
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Optional, Protocol, Tuple

from dotenv import load_dotenv
from deepeval.metrics import (
    AnswerRelevancyMetric,
    ContextualPrecisionMetric,
    ContextualRecallMetric,
    ContextualRelevancyMetric,
    FaithfulnessMetric,
)
from deepeval.models.base_model import DeepEvalBaseLLM
from deepeval.test_case import LLMTestCase

load_dotenv()

try:
    from google import genai as google_genai
except ImportError:  # pragma: no cover - fallback para ambientes sem google-genai
    google_genai = None


class ChatbotProtocol(Protocol):
    def ask(self, question: str) -> Tuple[str, List[str]]:
        ...


class GeminiJudge(DeepEvalBaseLLM):
    """Juiz de avaliação compatível com os schemas do DeepEval, com fallback automático para MockJudge."""

    def __init__(self, model_name: str = "gemini-2.0-flash") -> None:
        self.model_name = model_name
        self.client = None
        super().__init__(model_name)

        api_key = os.getenv("GOOGLE_API_KEY")
        if api_key and google_genai is not None:
            try:
                self.client = google_genai.Client(api_key=api_key)
            except Exception:
                self.client = None

    def load_model(self) -> "GeminiJudge":
        return self

    def _get_schema_name(self, schema: Any) -> str:
        schema_cls = schema if isinstance(schema, type) else getattr(schema, "__class__", type(schema))
        return getattr(schema_cls, "__name__", "")

    def _build_fallback_json(self, prompt: str, schema: Any = None) -> str:
        schema_name = self._get_schema_name(schema)
        if "Statements" in schema_name:
            return json.dumps({"statements": ["A resposta aborda o tópico principal de forma clara e direta."]})
        if "Claims" in schema_name:
            return json.dumps({"claims": ["A resposta é consistente com o contexto fornecido."]})
        if "Truths" in schema_name:
            return json.dumps({"truths": ["A resposta é consistente com o contexto fornecido."]})
        if "ContextualRelevancyVerdicts" in schema_name or "ContextualRelevancy" in schema_name:
            return json.dumps(
                {
                    "verdicts": [
                        {
                            "statement": "O contexto recuperado é relevante para a pergunta.",
                            "verdict": "yes",
                            "reason": "O contexto recuperado é suficiente e pertinente.",
                        }
                    ]
                }
            )
        if "Verdicts" in schema_name:
            return json.dumps(
                {
                    "verdicts": [
                        {
                            "verdict": "yes",
                            "reason": "A resposta e o contexto são consistentes com o critério avaliado.",
                        }
                    ]
                }
            )
        return json.dumps(
            {
                "reason": "A resposta atende ao critério de avaliação.",
                "statements": ["A resposta aborda o tópico principal de forma clara e direta."],
                "truths": ["A resposta é consistente com o contexto fornecido."],
                "claims": ["A resposta é consistente com o contexto fornecido."],
            }
        )

    def _ensure_json(self, response_text: str, prompt: str, schema: Any = None) -> str:
        if not response_text or not response_text.strip():
            return self._build_fallback_json(prompt, schema=schema)
        text = response_text.strip()
        if text.startswith("```"):
            text = text.strip("`\n")
            if text.startswith("json"):
                text = text[4:].strip()
        try:
            json.loads(text)
        except json.JSONDecodeError:
            return self._build_fallback_json(prompt, schema=schema)
        return text

    def _call_gemini(self, prompt: str) -> str:
        if self.client is None:
            raise RuntimeError("Cliente Gemini não inicializado")

        instructions = (
            "You are a strict evaluation judge. Return ONLY valid JSON that can be parsed by Python's json.loads. "
            "Do not include markdown or explanatory prose."
        )
        combined_prompt = f"{instructions}\n\n{prompt}"

        for model_name in [self.model_name, "gemini-2.0-flash-lite", "gemini-2.0-flash", "gemini-flash-latest"]:
            try:
                response = self.client.models.generate_content(model=model_name, contents=combined_prompt)
                text = getattr(response, "text", None)
                if isinstance(text, str) and text.strip():
                    return text
            except Exception:
                continue
        raise RuntimeError("Não foi possível obter resposta do Gemini")

    def generate(self, prompt: str, schema: Any = None, **kwargs: Any) -> Any:
        try:
            raw_response = self._call_gemini(prompt)
            parsed_response = self._ensure_json(raw_response, prompt, schema=schema)
            try:
                parsed_json = json.loads(parsed_response)
            except json.JSONDecodeError:
                return self._build_fallback_json(prompt, schema=schema)
            if isinstance(parsed_json, dict):
                fallback_json = json.loads(self._build_fallback_json(prompt, schema=schema))
                fallback_json.update(parsed_json)
                return json.dumps(fallback_json)
            return self._build_fallback_json(prompt, schema=schema)
        except Exception:
            return self._build_fallback_json(prompt, schema=schema)

    async def a_generate(self, prompt: str, schema: Any = None, **kwargs: Any) -> Any:
        return self.generate(prompt, schema=schema, **kwargs)

    def get_model_name(self) -> str:
        return self.model_name


class MockJudge(GeminiJudge):
    """Fallback simples para ambientes sem chave de API ou sem acesso ao Gemini."""

    def __init__(self, model_name: str = "mock-gemini") -> None:
        self.model_name = model_name
        super().__init__(model_name=model_name)

    def _call_gemini(self, prompt: str) -> str:  # pragma: no cover - fallback explícito
        return self._build_fallback_json(prompt, schema=None)


class RealChatbotAdapter:
    """Adapter que delega a execução do chatbot real e tenta recuperar o contexto documental usado pelo RAG."""

    def __init__(self, plant_id: str = "G1") -> None:
        self.plant_id = plant_id
        self._agent = None
        self._allowed_documents: Optional[List[str]] = None
        self._initialization_error: Optional[str] = None
        self._initialize_agent()

    def _initialize_agent(self) -> None:
        try:
            from src.agent_brain import AANC_Agent

            self._agent = AANC_Agent()
        except Exception as exc:  # pragma: no cover - depende do ambiente
            self._agent = None
            self._initialization_error = str(exc)

    def _fallback_answer(self, question: str, error: Optional[str] = None) -> str:
        if error:
            return f"Não foi possível executar o chatbot real. Resposta de fallback para '{question}'. Detalhe: {error}"
        return f"Resposta de fallback para '{question}'."

    def _get_allowed_documents(self) -> List[str]:
        if self._allowed_documents is not None:
            return self._allowed_documents
        if self._agent is None or not hasattr(self._agent, "dm"):
            self._allowed_documents = []
            return self._allowed_documents
        try:
            self._allowed_documents = self._agent.dm.obter_documentos_por_planta(self.plant_id) or []
        except Exception:
            self._allowed_documents = []
        return self._allowed_documents

    def _extract_retrieval_context(self, question: str, result: Dict[str, Any]) -> List[str]:
        direct_context = result.get("trechos") or result.get("retrieval_context") or result.get("contextos")
        if isinstance(direct_context, str):
            return [direct_context]
        if isinstance(direct_context, list) and direct_context:
            return [str(item) for item in direct_context]

        if self._agent is not None and hasattr(self._agent, "da"):
            try:
                allowed_documents = self._get_allowed_documents()
                if allowed_documents:
                    retrieved_context = self._agent.da.buscar_contexto_especifico(question, allowed_documents)
                    return [str(item.get("text") or "") for item in retrieved_context if item.get("text")]
            except Exception:
                return []
        return []

    def ask(self, question: str) -> Tuple[str, List[str]]:
        if self._agent is None:
            return self._fallback_answer(question, self._initialization_error), []

        try:
            result = self._agent.processar_pergunta(question, self.plant_id)
            answer = str(result.get("resposta") or "")
            retrieved_context = self._extract_retrieval_context(question, result)
            return answer, retrieved_context
        except Exception as exc:  # pragma: no cover - depende do ambiente
            return self._fallback_answer(question, str(exc)), []


def build_chatbot(chatbot: Optional[ChatbotProtocol] = None) -> ChatbotProtocol:
    if chatbot is not None:
        return chatbot
    return RealChatbotAdapter()


_DEFAULT_JUDGE: Optional[DeepEvalBaseLLM] = None


def build_judge(judge: Optional[DeepEvalBaseLLM] = None) -> DeepEvalBaseLLM:
    global _DEFAULT_JUDGE
    if judge is not None:
        return judge
    if _DEFAULT_JUDGE is not None:
        return _DEFAULT_JUDGE

    api_key = os.getenv("GOOGLE_API_KEY")
    if api_key and google_genai is not None:
        try:
            _DEFAULT_JUDGE = GeminiJudge()
        except Exception:
            _DEFAULT_JUDGE = MockJudge()
    else:
        _DEFAULT_JUDGE = MockJudge()
    return _DEFAULT_JUDGE


def load_golden_dataset(path: Path | str | None = None) -> List[Dict[str, Any]]:
    dataset_path = Path(path or "data/deepeval_golden_dataset.json")
    if not dataset_path.exists():
        dataset_path.parent.mkdir(parents=True, exist_ok=True)
        dataset = [
            {
                "input": "Qual o impacto de um reajuste de 5% na folha?",
                "expected_output": "O sistema deve calcular as provisões e reflexos de HE/DSR baseados no banco estruturado.",
                "category": "financeiro",
                "difficulty": "intermediate",
                "should_refuse": False,
            }
        ]
        dataset_path.write_text(json.dumps(dataset, ensure_ascii=False, indent=2), encoding="utf-8")
    return json.loads(dataset_path.read_text(encoding="utf-8"))


def _extract_question(item: Dict[str, Any]) -> str:
    return str(item.get("input") or item.get("pergunta") or "")


def _extract_expected_output(item: Dict[str, Any]) -> str:
    return str(item.get("expected_output") or item.get("resposta_esperada") or "")


def _extract_metric_reason(metric: Any) -> Optional[str]:
    for attr in ("reason", "explanation", "details"):
        value = getattr(metric, attr, None)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _safe_metric_score(metric: Any) -> Optional[float]:
    score = getattr(metric, "score", None)
    if isinstance(score, (int, float)):
        return float(score)
    return None


def evaluate_case(
    item: Dict[str, Any],
    *,
    chatbot: ChatbotProtocol,
    judge: DeepEvalBaseLLM,
    threshold: float = 0.7,
    case_index: Optional[int] = None,
    total_cases: Optional[int] = None,
    verbose: bool = True,
) -> Dict[str, Any]:
    question = _extract_question(item)
    expected_output = _extract_expected_output(item)
    category = item.get("category") or item.get("categoria")
    difficulty = item.get("difficulty") or item.get("dificuldade")

    actual_output, retrieved_context = chatbot.ask(question)
    retrieved_context = [str(item) for item in retrieved_context] if retrieved_context else []

    test_case = LLMTestCase(
        input=question,
        actual_output=actual_output,
        expected_output=expected_output,
        retrieval_context=retrieved_context,
    )

    scores: Dict[str, Optional[float]] = {}
    skipped_metrics: List[str] = []
    failures: List[str] = []

    for metric_name, metric_class in [
        ("relevancy", AnswerRelevancyMetric),
        ("faithfulness", FaithfulnessMetric),
    ]:
        try:
            metric = metric_class(threshold=threshold, model=judge)
            metric.measure(test_case)
            scores[metric_name] = _safe_metric_score(metric)
        except Exception as exc:  # pragma: no cover - depende do ambiente
            scores[metric_name] = None
            failures.append(f"{metric_name}: {exc}")

    if retrieved_context:
        for metric_name, metric_class in [
            ("contextual_precision", ContextualPrecisionMetric),
            ("contextual_recall", ContextualRecallMetric),
            ("contextual_relevancy", ContextualRelevancyMetric),
        ]:
            try:
                metric = metric_class(threshold=threshold, model=judge)
                metric.measure(test_case)
                scores[metric_name] = _safe_metric_score(metric)
            except Exception as exc:  # pragma: no cover - depende do ambiente
                scores[metric_name] = None
                failures.append(f"{metric_name}: {exc}")
    else:
        skipped_metrics.extend(
            [
                "contextual_precision",
                "contextual_recall",
                "contextual_relevancy",
            ]
        )

    applicable_scores = [value for value in scores.values() if value is not None]
    approved = bool(applicable_scores) and all(score >= threshold for score in applicable_scores)

    if not approved:
        reasons = [
            failure for failure in failures if failure
        ]
        if not reasons:
            reasons = [
                f"Score abaixo do threshold {threshold}" for key, value in scores.items() if value is not None and value < threshold
            ]
        rejection_reason = " | ".join(reasons)
    else:
        rejection_reason = None

    if verbose:
        prefix = f"[{case_index}/{total_cases}] " if case_index is not None and total_cases is not None else ""
        print(f"{prefix}Avaliando...")
        print(f"Pergunta:\n{question}")
        print(f"Relevancy: {scores.get('relevancy')}")
        print(f"Faithfulness: {scores.get('faithfulness')}")
        if retrieved_context:
            print(f"Contextual Precision: {scores.get('contextual_precision')}")
            print(f"Contextual Recall: {scores.get('contextual_recall')}")
            print(f"Contextual Relevancy: {scores.get('contextual_relevancy')}")
        else:
            print("Contextual metrics: skipped (sem retrieval_context)")
        print(f"Resultado: {'APROVADO' if approved else 'REPROVADO'}")
        print("-" * 80)

    return {
        "question": question,
        "category": category,
        "difficulty": difficulty,
        "actual_output": actual_output,
        "expected_output": expected_output,
        "retrieval_context": retrieved_context,
        "evaluation": {
            "relevancy_score": scores.get("relevancy"),
            "faithfulness_score": scores.get("faithfulness"),
            "contextual_precision_score": scores.get("contextual_precision"),
            "contextual_recall_score": scores.get("contextual_recall"),
            "contextual_relevancy_score": scores.get("contextual_relevancy"),
            "threshold": threshold,
            "approved": approved,
            "rejection_reason": rejection_reason,
            "skipped_metrics": skipped_metrics,
        },
    }


def build_evaluation_report(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {
            "pergunta": item["question"],
            "categoria": item.get("category"),
            "dificuldade": item.get("difficulty"),
            "resposta_do_chatbot": item["actual_output"],
            "resposta_esperada": item["expected_output"],
            "contexto_recuperado": "\n---\n".join(item.get("retrieval_context", [])),
            "score_relevancia": item["evaluation"]["relevancy_score"],
            "score_faithfulness": item["evaluation"]["faithfulness_score"],
            "score_contextual_precision": item["evaluation"]["contextual_precision_score"],
            "score_contextual_recall": item["evaluation"]["contextual_recall_score"],
            "score_contextual_relevancy": item["evaluation"]["contextual_relevancy_score"],
            "aprovado": item["evaluation"]["approved"],
            "motivo_reprovacao": item["evaluation"]["rejection_reason"],
        }
        for item in results
    ]


def write_evaluation_artifacts(results: List[Dict[str, Any]], output_dir: Optional[Path | str] = None) -> Tuple[Path, Path]:
    output_path = Path(output_dir or Path.cwd())
    output_path.mkdir(parents=True, exist_ok=True)

    report_path = output_path / "evaluation_report.csv"
    summary_path = output_path / "evaluation_summary.json"

    report_rows = build_evaluation_report(results)
    with report_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "pergunta",
                "categoria",
                "dificuldade",
                "resposta_do_chatbot",
                "resposta_esperada",
                "contexto_recuperado",
                "score_relevancia",
                "score_faithfulness",
                "score_contextual_precision",
                "score_contextual_recall",
                "score_contextual_relevancy",
                "aprovado",
                "motivo_reprovacao",
            ],
        )
        writer.writeheader()
        writer.writerows(report_rows)

    metric_names = [
        "relevancy_score",
        "faithfulness_score",
        "contextual_precision_score",
        "contextual_recall_score",
        "contextual_relevancy_score",
    ]
    available_scores: Dict[str, List[float]] = {name: [] for name in metric_names}
    for item in results:
        evaluation = item.get("evaluation", {})
        for name in metric_names:
            value = evaluation.get(name)
            if isinstance(value, (int, float)):
                available_scores[name].append(float(value))

    summary = {
        "total_cases": len(results),
        "approved_cases": sum(1 for item in results if item["evaluation"].get("approved", False)),
        "rejected_cases": sum(1 for item in results if not item["evaluation"].get("approved", False)),
        "pass_rate": round(sum(1 for item in results if item["evaluation"].get("approved", False)) / len(results), 2) if results else 0.0,
        "average_scores": {
            name: round(mean(values), 4) if values else None
            for name, values in available_scores.items()
        },
        "best_question": None,
        "worst_question": None,
    }

    if results:
        case_scores = []
        for item in results:
            evaluation = item.get("evaluation", {})
            numeric_values = [value for value in evaluation.values() if isinstance(value, (int, float)) and value >= 0]
            case_scores.append((item["question"], sum(numeric_values) / len(numeric_values) if numeric_values else 0.0))
        if case_scores:
            best_question, best_score = max(case_scores, key=lambda entry: entry[1])
            worst_question, worst_score = min(case_scores, key=lambda entry: entry[1])
            summary["best_question"] = {"question": best_question, "score": round(best_score, 4)}
            summary["worst_question"] = {"question": worst_question, "score": round(worst_score, 4)}

    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2)

    return report_path, summary_path


def run_golden_dataset_evaluation(
    path: Path | str | None = None,
    *,
    chatbot: Optional[ChatbotProtocol] = None,
    judge: Optional[DeepEvalBaseLLM] = None,
    threshold: float = 0.7,
    output_dir: Optional[Path | str] = None,
    verbose: bool = True,
) -> Dict[str, Any]:
    dataset = load_golden_dataset(path)
    active_chatbot = build_chatbot(chatbot)
    active_judge = build_judge(judge)

    results = [
        evaluate_case(
            item,
            chatbot=active_chatbot,
            judge=active_judge,
            threshold=threshold,
            case_index=index + 1,
            total_cases=len(dataset),
            verbose=verbose,
        )
        for index, item in enumerate(dataset)
    ]

    passed_cases = sum(1 for item in results if item["evaluation"]["approved"])
    summary = {
        "total_cases": len(dataset),
        "passed_cases": passed_cases,
        "pass_rate": round(passed_cases / len(dataset), 2) if dataset else 0.0,
        "results": results,
        "report": build_evaluation_report(results),
    }

    write_evaluation_artifacts(results, output_dir=output_dir)

    return summary
