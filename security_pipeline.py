import re

class SecurityPipeline:
    """
    Pipeline de Segurança do AANC.
    Implementa scanners de entrada (Prompt Injection, Tópicos Proibidos),
    scanner de saída e anonimização de PII (LGPD).
    """
    def __init__(self):
        self.forbidden_topics = [
            # Lista calibrada com precisão (Bloqueia receita, mas deixa atestado/convênio passar)
        
            "diagnóstico", "receita médica", "suicídio", "como me matar",
            "hackear", "burlar o sistema", "senha do banco", "lavagem de dinheiro",
            "piada ofensiva", "vírus", "código fonte"
        ]
        
        
        self.jailbreak_patterns = [
            "ignore as instruções", "ignore todas", "aja como", 
            "desative seus filtros", "bot sem regras", "esqueça o que",
            "sistema primário", "modo desenvolvedor"
        ]

    def scan_input(self, prompt: str):
        prompt_lower = prompt.lower()

        for pattern in self.jailbreak_patterns:
            if pattern in prompt_lower:
                return False, "🛡️ **ALERTA DE SEGURANÇA:** Tentativa de *Prompt Injection* detectada e bloqueada pelo firewall do AANC."

        for topic in self.forbidden_topics:
            if topic in prompt_lower:
                return False, "🛑 **GUARDRAIL ATIVADO:** O tema abordado está fora do escopo corporativo. Sou um assistente estrito de Negociação Coletiva e RH."

        return True, self.anonimizar_pii(prompt)

    def scan_output(self, response_text: str) -> str:
        return self.anonimizar_pii(response_text)

    def anonimizar_pii(self, texto: str) -> str:
        texto = re.sub(r'\b\d{3}\.\d{3}\.\d{3}-\d{2}\b', '[CPF_ANONIMIZADO]', texto)
        texto = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '[EMAIL_ANONIMIZADO]', texto)
        
        return texto