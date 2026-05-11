"""Módulo responsável pela indexação de documentos PDF e busca semântica filtrada por planta."""

import os
from txtai import Embeddings
from PyPDF2 import PdfReader

class DocumentAssistant:
    """
    Classe responsável por gerenciar a indexação e busca semântica de documentos usando txtai.
    """

    def __init__(self):
        """
        Inicializa o índice de embeddings do txtai.
        """
        try:
            self.embeddings = Embeddings()
            self.data = []  # Armazenar os dados indexados
            print("Índice de embeddings inicializado com sucesso.")
        except Exception as e:
            raise Exception(f"Erro ao inicializar embeddings: {str(e)}")

    def indexar_documentos(self, caminho_docs):
        """
        Indexa os documentos PDF da pasta especificada.

        Percorre a pasta, extrai o texto dos PDFs, divide em trechos e adiciona ao índice
        com o nome do arquivo como metadado.

        Args:
            caminho_docs (str): Caminho para a pasta contendo os PDFs.
        """
        if not os.path.exists(caminho_docs):
            raise Exception(f"Pasta '{caminho_docs}' não encontrada. Verifique o caminho.")

        pdf_files = [f for f in os.listdir(caminho_docs) if f.endswith('.pdf')]
        if not pdf_files:
            raise Exception(f"Nenhum arquivo PDF encontrado na pasta '{caminho_docs}'.")

        self.data = []
        for pdf_file in pdf_files:
            pdf_path = os.path.join(caminho_docs, pdf_file)
            try:
                text = self._extract_text_from_pdf(pdf_path)
                if not text.strip():
                    print(f"Aviso: Arquivo '{pdf_file}' não contém texto extraível.")
                    continue

                # Dividir texto em trechos de 1000 caracteres
                chunks = [text[i:i+1000] for i in range(0, len(text), 1000)]
                for chunk in chunks:
                    if chunk.strip():
                        self.data.append({"text": chunk, "file": pdf_file})
            except Exception as e:
                print(f"Erro ao processar '{pdf_file}': {str(e)}")
                continue

        if self.data:
            self.embeddings.index([d["text"] for d in self.data])  # Indexar apenas textos
            print(f"Indexação concluída: {len(self.data)} trechos indexados de {len(pdf_files)} arquivos.")
        else:
            raise Exception("Nenhum texto válido encontrado para indexar.")

    def _extract_text_from_pdf(self, pdf_path):
        """
        Extrai texto de um arquivo PDF.

        Args:
            pdf_path (str): Caminho para o PDF.

        Returns:
            str: Texto extraído.
        """
        reader = PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            text += page.extract_text() + "\n"
        return text

    def buscar_contexto_especifico(self, pergunta, arquivos_permitidos):
        """
        Busca contexto relevante para a pergunta, filtrado pelos arquivos permitidos.

        Args:
            pergunta (str): A pergunta do usuário.
            arquivos_permitidos (list): Lista de nomes de arquivos permitidos.

        Returns:
            list: Lista dos 3 trechos mais relevantes dos arquivos permitidos.

        Raises:
            Exception: Se não houver índice ou arquivos permitidos.
        """
        if not arquivos_permitidos:
            raise Exception("Lista de arquivos permitidos está vazia. Verifique os filtros por planta.")

        if not self.data:
            raise Exception("Índice de documentos não encontrado. Execute a indexação primeiro.")

        try:
            # Buscar e obter ids e scores
            results = self.embeddings.search(pergunta, limit=10)
            if not results:
                raise Exception("Nenhum contexto relevante encontrado. Tente reformular a pergunta.")

            # Filtrar e construir resultados
            filtered_results = []
            for id_, score in results:
                item = self.data[id_]
                if item.get('file') in arquivos_permitidos:
                    filtered_results.append({
                        "text": item["text"],
                        "file": item["file"],
                        "score": score
                    })

            if not filtered_results:
                raise Exception("Nenhum contexto relevante encontrado nos arquivos permitidos. Verifique os documentos ou os filtros.")

            return filtered_results[:3]
        except Exception as e:
            raise Exception(f"Erro na busca: {str(e)}")

if __name__ == '__main__':
    # Instanciar a classe
    da = DocumentAssistant()

    # Indexar documentos da pasta 'docs/'
    try:
        da.indexar_documentos('docs/')
    except Exception as e:
        print(f"Erro na indexação: {e}")
        exit(1)

    # Simular busca para Planta G1
    arquivos_g1 = ['CCT_Metalurgicos_Grande_Curitiba.pdf']
    pergunta_exemplo = "Quais são os direitos trabalhistas básicos?"

    try:
        resultados = da.buscar_contexto_especifico(pergunta_exemplo, arquivos_g1)
        print("Resultados da busca para G1:")
        for i, res in enumerate(resultados):
            print(f"{i+1}. Arquivo: {res.get('file', 'N/A')}")
            print(f"   Texto: {res.get('text', 'N/A')[:200]}...")
            print()
    except Exception as e:
        print(f"Erro na busca: {e}")