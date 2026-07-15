import re
from typing import Dict, Tuple


class SecurityGuard:
    """Aplicação simples de guardrails para entrada e saída de LLMs."""

    def __init__(self) -> None:
        self.pii_patterns = {
            "cpf": re.compile(r"\b\d{3}\.\d{3}\.\d{3}-\d{2}\b"),
            "email": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
            "telefone": re.compile(r"\(?\d{2}\)?\s?9?\d{4}-?\d{4}"),
        }
        self.injection_patterns = [
            r"ignore (previous|prior|system|developer|all) instructions",
            r"bypass (security|policy|rules)",
            r"act as (an?|the) (system|developer|admin)",
            r"reveal (system|internal|secret|prompt)",
            r"jailbreak",
            r"ignore policy",
            r"pretend to be",
        ]
        self.unsafe_keywords = [
            "burlar",
            "fraude",
            "evadir",
            "sequestrar",
            "contornar controles",
            "fingir identidade",
            "exfiltrar",
        ]
        self.block_message = (
            "Não posso ajudar com esse tipo de solicitação. "
            "A mensagem foi bloqueada por segurança."
        )

    def _mask_pii(self, text: str) -> str:
        masked = text
        for pattern in self.pii_patterns.values():
            masked = pattern.sub("[REDACTED]", masked)
        return masked

    def _contains_injection(self, text: str) -> bool:
        lowered = text.lower()
        return any(re.search(pattern, lowered) for pattern in self.injection_patterns)

    def sanitize_user_input(self, text: str) -> Tuple[str, Dict[str, object]]:
        if not text or not str(text).strip():
            return "", {"blocked": False, "pii_detected": False, "reason": None}

        original = str(text)
        pii_detected = any(pattern.search(original) for pattern in self.pii_patterns.values())
        masked_text = self._mask_pii(original)
        blocked = self._contains_injection(masked_text)

        if blocked:
            return self.block_message, {
                "blocked": True,
                "pii_detected": pii_detected,
                "reason": "prompt_injection",
                "sanitized": self.block_message,
            }

        return masked_text, {
            "blocked": False,
            "pii_detected": pii_detected,
            "reason": None,
            "sanitized": masked_text,
        }

    def sanitize_model_output(self, text: str) -> Tuple[str, Dict[str, object]]:
        if not text or not str(text).strip():
            return "", {"unsafe": False, "pii_detected": False, "blocked": False}

        original = str(text)
        pii_detected = any(pattern.search(original) for pattern in self.pii_patterns.values())
        masked_text = self._mask_pii(original)
        lowered = masked_text.lower()
        unsafe = any(keyword in lowered for keyword in self.unsafe_keywords)

        if unsafe:
            return (
                "Não posso fornecer orientação para atividades inseguras ou contrárias a políticas."
                f" {self.block_message}",
                {
                    "unsafe": True,
                    "pii_detected": pii_detected,
                    "blocked": True,
                    "reason": "unsafe_output",
                    "sanitized": masked_text,
                },
            )

        return masked_text, {
            "unsafe": False,
            "pii_detected": pii_detected,
            "blocked": False,
            "reason": None,
            "sanitized": masked_text,
        }
