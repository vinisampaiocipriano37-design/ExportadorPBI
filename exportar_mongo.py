import os
import time
import shutil
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime

# 1. Carrega as variaveis de ambiente
load_dotenv()

# Pegando a string de conexao segura
MONGO_URI = os.getenv("MONGO_URI")

# Variaveis globais de configuracao - Puxando do .env
CLIENTE_NOME = os.getenv("CLIENTE", "AEROFLEX")
AMBIENTE = os.getenv("AMBIENTE", "homologacao").strip().lower()

# Lógica de Nome de Banco (Se for produção tira o _test)
PREFIXO_AMB = "_test" if AMBIENTE == "homologacao" else ""

# 2. LISTA DE TABELAS (COLECOES) PARA EXPORTAR
# Agora todas as coleções são buscadas do banco consolidado: QRS_{CLIENTE_NOME}{PREFIXO_AMB}_Relatorio
DATABASE_NAME = f"QRS_{CLIENTE_NOME}{PREFIXO_AMB}_Relatorio"

TABELAS_PARA_EXPORTAR = [
    {
        "nome_tarefa": "1 - Documentos Fiscais",
        "collection": "DocumentosFiscais",
        "campos_desejados": {
            "_id": 0,
            "id": 1,
            "campanha": 1,
            "usuario": 1,
            "cabecalho": 1,
            "emitente": 1,
            "destinatario": 1,
            "produtorServicos": 1,
            "dataHoraLeitura": 1,
            "dataHoraProcessamento": 1,
            "consumidor": 1
        },
        "palavras_escondidas_limpeza": [
            "apuracao", "apuração", 
            "hash_imagens", "hashimagem", "hash_imagem", 
            "imagem", "urlimagem", "url_imagem", 
            "primeiraurl", "primeira_url", 
            "eventos", "evento"
        ],
        "csv_final": f"DocumentosFiscais_{CLIENTE_NOME}.csv"
    },
    {
        "nome_tarefa": "2 - Associados",
        "collection": "Associados",
        "campos_desejados": {
            "_id": 0,
            "cnpjCpf": 1,
            "nome": 1,
            "razaoSocial": 1,
            "status": 1,
            "loja": 1
        },
        "palavras_escondidas_limpeza": [],
        "csv_final": f"Associados_{CLIENTE_NOME}.csv"
    },
    {
        "nome_tarefa": "3 - Consumidores",
        "collection": "Consumidores",
        "campos_desejados": {
            "_id": 0,
            "id": 1,
            "telefoneContato": 1,
            "nome": 1,
            "dataNascimento": 1,
            "cpf": 1,
            "genero": 1,
            "endereco": 1,
            "email": 1,
            "dataHoraCadastro": 1,
            "primeiraLoja": 1,
            "resposta": 1
        },
        "palavras_escondidas_limpeza": [],
        "csv_final": f"Consumidores_{CLIENTE_NOME}.csv"
    },
    {
        "nome_tarefa": "4 - Cupons",
        "collection": "Cupons",
        "campos_desejados": {
            "_id": 0,
            "id": 1,
            "documentoFiscal.emitente": 1,
            "documentoFiscal.destinatario": 1,
            "consumidor.cpf": 1,
            "numeroDaSorte": 1
        },
        "palavras_escondidas_limpeza": [],
        "csv_final": f"Cupons_{CLIENTE_NOME}.csv"
    },
    {
        "nome_tarefa": "5 - RaspadinhaAlternativa",
        "collection": "RaspadinhasAlternativas",
        "campos_desejados": {
            "_id": 0,
            "id": 1,
            "raspada": 1,
            "dataCriada": 1,
            "entrega": 1,
            "premio": 1,
            "premioCancelado": 1,
            "apuracao": 1,
            "dataLiberacao": 1,
            "consumidor": 1
        },
        "palavras_escondidas_limpeza": [],
        "csv_final": f"RaspadinhasAlternativas_{CLIENTE_NOME}.csv"
    }
]

def export_mongo_to_csv():
    # Configurações de Confiabilidade
    MAX_RETRIES = 3
    RETRY_DELAY = 10  # segundos entre tentativas
    BATCH_SIZE = 5000 # Quantidade de linhas processadas por vez (RAM-friendly)
    
    client = None
    
    try:
        # === ETAPA 1: CONEXÃO COM RETENTATIVA (Resiliência de Rede/VPN) ===
        for attempt in range(MAX_RETRIES):
            try:
                print(f"[{datetime.now()}] Tentativa de conexão {attempt + 1}/{MAX_RETRIES}...")
                # serverSelectionTimeoutMS garante que não ficaremos esperando 30s se o IP estiver bloqueado
                client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
                client.admin.command('ping') # Força o teste real de conexão
                print(f"[{datetime.now()}] Conectado ao MongoDB com sucesso.")
                break
            except Exception as e:
                print(f"[{datetime.now()}] Falha na tentativa {attempt + 1}: {e}")
                if attempt < MAX_RETRIES - 1:
                    print(f"Aguardando {RETRY_DELAY}s para a próxima tentativa...")
                    time.sleep(RETRY_DELAY)
                else:
                    raise Exception("Não foi possível conectar ao MongoDB após várias tentativas. Abortando.")

        # === ETAPA 2: PROCESSAMENTO DAS TABELAS ===
        for tarefa in TABELAS_PARA_EXPORTAR:
            print(f"\n" + "="*50)
            print(f"[{datetime.now()}] TAREFA: {tarefa['nome_tarefa']}")
            
            # Usaremos um arquivo temporário para garantir que o BI nunca leia um arquivo "pela metade"
            csv_final = tarefa["csv_final"]
            csv_temp = csv_final + ".tmp"
            
            # Limpa lixo de execuções anteriores abortadas
            if os.path.exists(csv_temp): os.remove(csv_temp)

            try:
                print(f"[{datetime.now()}] Usando Banco de Dados: {DATABASE_NAME}")
                db = client[DATABASE_NAME]
                collection = db[tarefa["collection"]]
                
                # Cursor inteligente: Não carrega tudo na RAM
                cursor = collection.find({}, tarefa["campos_desejados"])
                
                chunk = []
                total_processado = 0
                header_written = False
                
                for doc in cursor:
                    chunk.append(doc)
                    total_processado += 1
                    
                    # Quando atinge o tamanho do lote, processa e descarrega no disco
                    if len(chunk) >= BATCH_SIZE:
                        df = pd.json_normalize(chunk)
                        
                        # Limpeza de campos aninhados indesejados
                        palavras = tarefa.get("palavras_escondidas_limpeza", [])
                        if palavras:
                            colunas_remover = [c for c in df.columns if any(p in c.lower() for p in palavras)]
                            df = df.drop(columns=colunas_remover, errors='ignore')
                        
                        # Escreve no arquivo temporário (Modo Append 'a')
                        df.to_csv(csv_temp, mode='a', index=False, header=not header_written, encoding='utf-8-sig')
                        header_written = True
                        chunk = []
                        print(f"[{datetime.now()}] Processadas {total_processado} linhas...")

                # Processa o último lote remanescente
                if chunk:
                    df = pd.json_normalize(chunk)
                    palavras = tarefa.get("palavras_escondidas_limpeza", [])
                    if palavras:
                        colunas_remover = [c for c in df.columns if any(p in c.lower() for p in palavras)]
                        df = df.drop(columns=colunas_remover, errors='ignore')
                    
                    df.to_csv(csv_temp, mode='a', index=False, header=not header_written, encoding='utf-8-sig')
                    header_written = True

                if total_processado == 0:
                    print(f"AVISO: Coleção {tarefa['collection']} está vazia. Nenhum arquivo gerado.")
                else:
                    # === SUCESSO TOTAL NA TABELA: TROCA ATÔMICA ===
                    # Só renomeamos para .csv se chegamos aqui sem erros.
                    if os.path.exists(csv_final): os.remove(csv_final)
                    os.rename(csv_temp, csv_final)
                    print(f"[{datetime.now()}] SUCESSO: {total_processado} linhas exportadas para '{csv_final}'.")

            except Exception as e:
                # Se algo der erro, apagamos o lixo temporário
                if os.path.exists(csv_temp): os.remove(csv_temp)
                # LANÇAMOS O ERRO PARA CIMA: Isso fará o script INTEIRO parar (All-or-Nothing)
                raise Exception(f"ERRO FATAL na tarefa '{tarefa['nome_tarefa']}': {e}")

        print(f"\n" + "="*50)
        print(f"[{datetime.now()}] PIPELINE FINALIZADO COM 100% DE SUCESSO.")

    except Exception as e:
        print(f"\n" + "!"*50)
        print(f"[{datetime.now()}] CRÍTICO: O PROCESSO INTEIRO FOI ABORTADO!")
        print(f"MOTIVO: {e}")
        print(f"!"*50)
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    export_mongo_to_csv()

