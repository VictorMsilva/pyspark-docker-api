import pandas as pd
import os
import shutil
from glob import glob
from dotenv import load_dotenv
import time

def consolidate_chunks_and_cleanup():
    """Consolida chunks um por vez e deleta cada um após processar"""
    
    # Carrega variáveis do .env
    load_dotenv()
    PARQUET_PATH = os.getenv("PARQUET_PATH")
    
    if not PARQUET_PATH:
        print("Erro: PARQUET_PATH não encontrado no arquivo .env")
        return None
    
    print(f"Buscando chunks em: {PARQUET_PATH}")
    
    # Encontra todas as pastas de chunks
    chunk_pattern = f"{PARQUET_PATH}/chunk_*"
    chunk_folders = glob(chunk_pattern)
    
    if not chunk_folders:
        print(f"Nenhum chunk encontrado no padrão: {chunk_pattern}")
        return None
    
    print(f"Encontrados {len(chunk_folders)} chunks para consolidar")
    
    all_data = []
    total_records = 0
    
    # Processa cada chunk individualmente
    for i, chunk_folder in enumerate(sorted(chunk_folders), 1):
        print(f"[{i}/{len(chunk_folders)}] Processando: {chunk_folder}")
        
        try:
            # Encontra arquivos .parquet dentro da pasta do chunk
            parquet_files = glob(f"{chunk_folder}/*.parquet")
            
            if not parquet_files:
                continue
            
            # Lê o arquivo parquet
            parquet_file = parquet_files[0]
            df_chunk = pd.read_parquet(parquet_file)
            
            print(f"✅ {len(df_chunk):,} registros carregados")
            
            # Adiciona aos dados consolidados
            all_data.append(df_chunk)
            total_records += len(df_chunk)
            
            # DELETA O CHUNK IMEDIATAMENTE APÓS PROCESSAR
            shutil.rmtree(chunk_folder)
            print(f"🗑️ Chunk removido: {os.path.basename(chunk_folder)}")
            
        except Exception as e:
            print(f"❌ Erro ao processar {chunk_folder}: {e}")
            continue
    
    if not all_data:
        return None
    
    print(f"Consolidando {len(all_data)} chunks...")
    
    try:
        df_final = pd.concat(all_data, ignore_index=True)
        
        # Define o caminho do arquivo consolidado
        consolidated_path = f"{PARQUET_PATH}_consolidated.parquet"
        
        # Salva o arquivo consolidado
        df_final.to_parquet(
            consolidated_path, 
            compression='snappy',
            index=False
        )
        
        print(f"✅ Arquivo consolidado salvo: {consolidated_path}")
        
        # Remove a pasta principal de chunks se estiver vazia
        try:
            if os.path.exists(PARQUET_PATH) and not os.listdir(PARQUET_PATH):
                os.rmdir(PARQUET_PATH)
        except:
            pass
        
        return consolidated_path
        
    except Exception as e:
        print(f"❌ Erro durante a consolidação: {e}")
        return None

def main():
    consolidated_path = consolidate_chunks_and_cleanup()
    return consolidated_path

if __name__ == "__main__":
    main()