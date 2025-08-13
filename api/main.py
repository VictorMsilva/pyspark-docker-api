import pandas as pd
import numpy as np
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
from dotenv import load_dotenv
import os
import glob
from datetime import datetime
import pyarrow.parquet as pq

# Carrega variáveis
load_dotenv()

# Determina o caminho correto do arquivo Parquet
PARQUET_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "processed", "taxi_clean.parquet")

app = FastAPI(
    title="Taxi Data Analysis API",
    description="API para análise de dados de táxi de NYC",
    version="1.0.0"
)

# Cache global para dados
DATA_CACHE = {}

class FilterRequest(BaseModel):
    column: str
    operator: str  # eq, gt, lt, gte, lte, in, between
    value: Any
    value2: Optional[Any] = None  # Para operação between
    limit: Optional[int] = 1000

class StatsRequest(BaseModel):
    columns: List[str]
    group_by: Optional[str] = None

def load_parquet_data():
    """Carrega dados do Parquet com tratamento de erros e cache"""
    global DATA_CACHE
    
    if "main_df" in DATA_CACHE:
        return DATA_CACHE["main_df"]
    
    try:
        # Verifica diferentes possibilidades de localização dos arquivos
        possible_paths = [
            PARQUET_PATH,
            os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "processed"),
            os.path.join(os.path.dirname(__file__), "..", "data", "processed"),
            "./data/processed",
            "../data/processed"
        ]
        
        df = None
        for path in possible_paths:
            try:
                if os.path.isdir(path):
                    # Se é um diretório, procura por arquivos parquet
                    parquet_files = glob.glob(os.path.join(path, "*.parquet"))
                    if parquet_files:
                        print(f"Lendo arquivos Parquet do diretório: {path}")
                        df = pd.read_parquet(path)
                        break
                elif os.path.exists(path) and path.endswith('.parquet'):
                    print(f"Lendo arquivo Parquet: {path}")
                    df = pd.read_parquet(path)
                    break
            except Exception as e:
                print(f"Erro ao tentar ler {path}: {e}")
                continue
        
        if df is None:
            # Última tentativa: procurar recursivamente
            for root, dirs, files in os.walk(os.path.dirname(os.path.dirname(__file__))):
                for file in files:
                    if file.endswith('.parquet'):
                        file_path = os.path.join(root, file)
                        try:
                            print(f"Tentando ler arquivo encontrado: {file_path}")
                            df = pd.read_parquet(file_path)
                            break
                        except:
                            continue
                if df is not None:
                    break
        
        if df is None:
            raise FileNotFoundError("Nenhum arquivo Parquet válido encontrado")
        
        # Otimizações de tipos de dados
        for col in df.columns:
            if df[col].dtype == 'object':
                try:
                    # Tenta converter strings para datetime se aplicável
                    if 'datetime' in col.lower():
                        df[col] = pd.to_datetime(df[col], errors='ignore')
                    elif df[col].nunique() < len(df) * 0.5:  # Se tem muitos valores repetidos
                        df[col] = df[col].astype('category')
                except:
                    pass
        
        # Cache dos dados
        DATA_CACHE["main_df"] = df
        print(f"Dados carregados e cached: {len(df)} registros, {len(df.columns)} colunas")
        print(f"Colunas disponíveis: {list(df.columns)}")
        
        return df
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao carregar dados: {str(e)}")

def apply_filter(df: pd.DataFrame, filter_req: FilterRequest) -> pd.DataFrame:
    """Aplica filtros no DataFrame"""
    try:
        column = filter_req.column
        operator = filter_req.operator.lower()
        value = filter_req.value
        value2 = filter_req.value2
        
        if column not in df.columns:
            raise ValueError(f"Coluna '{column}' não existe")
        
        # Converte valores para o tipo correto da coluna
        col_dtype = df[column].dtype
        
        if operator == "eq":
            filtered_df = df[df[column] == value]
        elif operator == "gt":
            filtered_df = df[df[column] > value]
        elif operator == "lt":
            filtered_df = df[df[column] < value]
        elif operator == "gte":
            filtered_df = df[df[column] >= value]
        elif operator == "lte":
            filtered_df = df[df[column] <= value]
        elif operator == "in":
            if isinstance(value, str):
                value = [v.strip() for v in value.split(",")]
            filtered_df = df[df[column].isin(value)]
        elif operator == "between":
            if value2 is None:
                raise ValueError("Operação 'between' requer value2")
            filtered_df = df[(df[column] >= value) & (df[column] <= value2)]
        elif operator == "contains":
            filtered_df = df[df[column].astype(str).str.contains(str(value), case=False, na=False)]
        else:
            raise ValueError(f"Operador '{operator}' não suportado")
        
        return filtered_df.head(filter_req.limit) if filter_req.limit else filtered_df
    
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro no filtro: {str(e)}")

@app.get("/")
def root():
    """Endpoint raiz com informações da API"""
    return {
        "message": "Taxi Data Analysis API",
        "version": "1.0.0",
        "endpoints": {
            "/": "Informações da API",
            "/health": "Status da API e dados",
            "/columns": "Informações das colunas",
            "/preview": "Visualizar dados (limite: 100)",
            "/filter": "Filtrar dados (POST)",
            "/stats": "Estatísticas (POST)",
            "/summary": "Resumo dos dados",
            "/unique/{column}": "Valores únicos de uma coluna"
        }
    }

@app.get("/health")
def health_check():
    """Verifica se a API e os dados estão acessíveis"""
    try:
        df = load_parquet_data()
        return {
            "status": "healthy",
            "data_loaded": True,
            "records_count": len(df),
            "columns_count": len(df.columns),
            "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024 / 1024,
            "columns": list(df.columns)
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e)
        }

@app.get("/columns")
def get_columns():
    """Retorna informações sobre as colunas disponíveis"""
    try:
        df = load_parquet_data()
        columns_info = []
        
        for col in df.columns:
            col_info = {
                "name": col,
                "dtype": str(df[col].dtype),
                "non_null_count": int(df[col].count()),
                "null_count": int(df[col].isnull().sum()),
                "unique_count": int(df[col].nunique())
            }
            
            # Adiciona estatísticas específicas por tipo
            if pd.api.types.is_numeric_dtype(df[col]):
                col_info.update({
                    "min": float(df[col].min()) if not pd.isna(df[col].min()) else None,
                    "max": float(df[col].max()) if not pd.isna(df[col].max()) else None,
                    "mean": float(df[col].mean()) if not pd.isna(df[col].mean()) else None
                })
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                col_info.update({
                    "min_date": str(df[col].min()) if not pd.isna(df[col].min()) else None,
                    "max_date": str(df[col].max()) if not pd.isna(df[col].max()) else None
                })
            
            columns_info.append(col_info)
        
        return {
            "columns": columns_info,
            "total_columns": len(columns_info)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao obter informações das colunas: {str(e)}")

@app.get("/preview")
def preview(limit: int = Query(10, ge=1, le=100)):
    """Visualiza os primeiros registros dos dados"""
    try:
        df = load_parquet_data()
        preview_df = df.head(limit)
        
        # Converte DataFrame para formato JSON-serializable
        result = []
        for _, row in preview_df.iterrows():
            row_dict = {}
            for col, val in row.items():
                if pd.isna(val):
                    row_dict[col] = None
                elif isinstance(val, (np.int64, np.int32)):
                    row_dict[col] = int(val)
                elif isinstance(val, (np.float64, np.float32)):
                    row_dict[col] = float(val)
                elif isinstance(val, pd.Timestamp):
                    row_dict[col] = val.isoformat()
                else:
                    row_dict[col] = str(val)
            result.append(row_dict)
        
        return {
            "data": result,
            "total_records": len(df),
            "preview_count": len(result),
            "columns": list(df.columns)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro no preview: {str(e)}")

@app.post("/filter")
def filter_data(filter_req: FilterRequest):
    """Filtra dados baseado nos critérios fornecidos"""
    try:
        df = load_parquet_data()
        filtered_df = apply_filter(df, filter_req)
        
        # Converte para formato JSON-serializable
        result = []
        for _, row in filtered_df.iterrows():
            row_dict = {}
            for col, val in row.items():
                if pd.isna(val):
                    row_dict[col] = None
                elif isinstance(val, (np.int64, np.int32)):
                    row_dict[col] = int(val)
                elif isinstance(val, (np.float64, np.float32)):
                    row_dict[col] = float(val)
                elif isinstance(val, pd.Timestamp):
                    row_dict[col] = val.isoformat()
                else:
                    row_dict[col] = str(val)
            result.append(row_dict)
        
        return {
            "data": result,
            "filtered_count": len(filtered_df),
            "total_records": len(df),
            "filter_applied": {
                "column": filter_req.column,
                "operator": filter_req.operator,
                "value": filter_req.value,
                "value2": filter_req.value2
            }
        }
    
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro no filtro: {str(e)}")

@app.post("/stats")
def get_statistics(stats_req: StatsRequest):
    """Calcula estatísticas para colunas especificadas"""
    try:
        df = load_parquet_data()
        
        # Verifica se as colunas existem
        for col in stats_req.columns:
            if col not in df.columns:
                raise ValueError(f"Coluna '{col}' não existe")
        
        result = {}
        
        if stats_req.group_by:
            if stats_req.group_by not in df.columns:
                raise ValueError(f"Coluna de agrupamento '{stats_req.group_by}' não existe")
            
            grouped = df.groupby(stats_req.group_by)
            for col in stats_req.columns:
                if pd.api.types.is_numeric_dtype(df[col]):
                    result[col] = {
                        "by_group": grouped[col].agg(['count', 'mean', 'min', 'max', 'std']).to_dict()
                    }
        else:
            for col in stats_req.columns:
                if pd.api.types.is_numeric_dtype(df[col]):
                    result[col] = {
                        "count": int(df[col].count()),
                        "mean": float(df[col].mean()),
                        "std": float(df[col].std()),
                        "min": float(df[col].min()),
                        "max": float(df[col].max()),
                        "median": float(df[col].median()),
                        "quartiles": {
                            "25%": float(df[col].quantile(0.25)),
                            "75%": float(df[col].quantile(0.75))
                        }
                    }
                else:
                    result[col] = {
                        "count": int(df[col].count()),
                        "unique": int(df[col].nunique()),
                        "top_values": df[col].value_counts().head(10).to_dict()
                    }
        
        return {
            "statistics": result,
            "columns_analyzed": stats_req.columns,
            "grouped_by": stats_req.group_by
        }
    
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro no cálculo de estatísticas: {str(e)}")

@app.get("/summary")
def get_summary():
    """Retorna um resumo geral dos dados"""
    try:
        df = load_parquet_data()
        
        numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
        categorical_columns = df.select_dtypes(include=['object', 'category']).columns.tolist()
        datetime_columns = df.select_dtypes(include=['datetime64[ns]']).columns.tolist()
        
        summary = {
            "total_records": len(df),
            "total_columns": len(df.columns),
            "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024 / 1024,
            "column_types": {
                "numeric": len(numeric_columns),
                "categorical": len(categorical_columns),
                "datetime": len(datetime_columns)
            },
            "missing_data": {
                col: int(df[col].isnull().sum()) 
                for col in df.columns if df[col].isnull().sum() > 0
            },
            "date_range": {}
        }
        
        # Adiciona informações de intervalo de datas
        for col in datetime_columns:
            if not df[col].isnull().all():
                summary["date_range"][col] = {
                    "start": str(df[col].min()),
                    "end": str(df[col].max())
                }
        
        return summary
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro no resumo: {str(e)}")

@app.get("/unique/{column}")
def get_unique_values(column: str, limit: int = Query(100, ge=1, le=1000)):
    """Retorna valores únicos de uma coluna"""
    try:
        df = load_parquet_data()
        
        if column not in df.columns:
            raise HTTPException(status_code=404, detail=f"Coluna '{column}' não encontrada")
        
        unique_values = df[column].value_counts().head(limit)
        
        return {
            "column": column,
            "unique_values": unique_values.to_dict(),
            "total_unique": int(df[column].nunique()),
            "showing": len(unique_values)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao obter valores únicos: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("API_PORT", 8001))
    uvicorn.run(app, host="0.0.0.0", port=port)
    operator: str  # eq, gt, lt, gte, lte, in
    value: str
    limit: Optional[int] = 1000

def load_parquet_data():
    """Carrega dados do Parquet com tratamento de erros"""
    try:
        # Verifica se é um diretório de arquivos Parquet
        if os.path.isdir(PARQUET_PATH):
            print(f"Lendo diretório Parquet: {PARQUET_PATH}")
            df = pd.read_parquet(PARQUET_PATH)
        else:
            # Procura por arquivos Parquet na pasta
            parquet_files = glob.glob(os.path.join(os.path.dirname(PARQUET_PATH), "*.parquet"))
            if parquet_files:
                print(f"Lendo arquivo Parquet: {parquet_files[0]}")
                df = pd.read_parquet(parquet_files[0])
            else:
                raise FileNotFoundError(f"Nenhum arquivo Parquet encontrado em {PARQUET_PATH}")
        
        # Converte tipos de dados problemáticos
        for col in df.columns:
            if df[col].dtype == 'object':
                try:
                    # Tenta converter strings para datetime se aplicável
                    if 'datetime' in col.lower():
                        df[col] = pd.to_datetime(df[col], errors='ignore')
                except:
                    pass
        
        print(f"Dados carregados: {len(df)} registros, {len(df.columns)} colunas")
        return df
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao carregar dados: {str(e)}")

@app.get("/")
def root():
    """Endpoint raiz com informações da API"""
    return {
        "message": "Taxi Data Analysis API",
        "version": "1.0.0",
        "endpoints": ["/preview", "/filter", "/stats", "/columns", "/health"]
    }

@app.get("/health")
def health_check():
    """Verifica se a API e os dados estão acessíveis"""
    try:
        df = load_parquet_data()
        return {
            "status": "healthy",
            "data_loaded": True,
            "records_count": len(df),
            "columns_count": len(df.columns)
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e)
        }

@app.get("/columns")
def get_columns():
    """Retorna informações sobre as colunas disponíveis"""
    try:
        df = load_parquet_data()
        columns_info = []
        
        for col in df.columns:
            col_info = {
                "name": col,
                "type": str(df[col].dtype),
                "non_null_count": int(df[col].count()),
                "null_count": int(df[col].isnull().sum())
            }
            
            # Adiciona estatísticas para colunas numéricas
            if df[col].dtype in ['int64', 'float64']:
                col_info.update({
                    "min": float(df[col].min()) if not pd.isna(df[col].min()) else None,
                    "max": float(df[col].max()) if not pd.isna(df[col].max()) else None,
                    "mean": float(df[col].mean()) if not pd.isna(df[col].mean()) else None
                })
            
            columns_info.append(col_info)
        
        return {"columns": columns_info}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao obter informações das colunas: {str(e)}")

@app.get("/preview")
def preview(limit: int = Query(10, ge=1, le=100)):
    """Retorna os primeiros registros"""
    try:
        df = load_parquet_data()
        
        # Converte timestamps para string para serialização JSON
        df_preview = df.head(limit).copy()
        for col in df_preview.columns:
            if df_preview[col].dtype == 'datetime64[ns]':
                df_preview[col] = df_preview[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        return {
            "total_records": len(df),
            "preview_count": len(df_preview),
            "data": df_preview.to_dict(orient="records")
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao carregar preview: {str(e)}")

@app.get("/stats")
def get_statistics():
    """Retorna estatísticas básicas dos dados"""
    try:
        df = load_parquet_data()
        
        stats = {
            "total_records": len(df),
            "total_columns": len(df.columns),
            "memory_usage_mb": round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2)
        }
        
        # Estatísticas por tipo de coluna
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        datetime_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
        
        if numeric_cols:
            stats["numeric_columns"] = len(numeric_cols)
            stats["numeric_stats"] = df[numeric_cols].describe().round(2).to_dict()
        
        if datetime_cols:
            stats["datetime_columns"] = len(datetime_cols)
            for col in datetime_cols:
                stats[f"{col}_range"] = {
                    "min": df[col].min().strftime('%Y-%m-%d %H:%M:%S') if not pd.isna(df[col].min()) else None,
                    "max": df[col].max().strftime('%Y-%m-%d %H:%M:%S') if not pd.isna(df[col].max()) else None
                }
        
        return stats
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao calcular estatísticas: {str(e)}")

@app.get("/filter")
def filter_data(
    column: str = Query(..., description="Nome da coluna para filtrar"),
    operator: str = Query("eq", description="Operador: eq, gt, lt, gte, lte, in, contains"),
    value: str = Query(..., description="Valor para filtrar"),
    limit: int = Query(1000, ge=1, le=10000, description="Limite de registros retornados")
):
    """Filtra os dados por coluna e valor com operadores avançados"""
    try:
        df = load_parquet_data()
        
        # Verifica se a coluna existe
        if column not in df.columns:
            raise HTTPException(status_code=400, detail=f"Coluna '{column}' não encontrada")
        
        # Aplica o filtro baseado no operador
        if operator == "eq":
            # Converte o valor para o tipo correto da coluna
            if df[column].dtype in ['int64', 'float64']:
                try:
                    value = float(value)
                    if df[column].dtype == 'int64':
                        value = int(value)
                except ValueError:
                    raise HTTPException(status_code=400, detail=f"Valor '{value}' inválido para coluna numérica")
            df_filtered = df[df[column] == value]
            
        elif operator == "gt":
            value = float(value)
            df_filtered = df[df[column] > value]
            
        elif operator == "lt":
            value = float(value)
            df_filtered = df[df[column] < value]
            
        elif operator == "gte":
            value = float(value)
            df_filtered = df[df[column] >= value]
            
        elif operator == "lte":
            value = float(value)
            df_filtered = df[df[column] <= value]
            
        elif operator == "in":
            values = [v.strip() for v in value.split(",")]
            # Converte para tipo correto se numérico
            if df[column].dtype in ['int64', 'float64']:
                values = [float(v) for v in values]
                if df[column].dtype == 'int64':
                    values = [int(v) for v in values]
            df_filtered = df[df[column].isin(values)]
            
        elif operator == "contains":
            if df[column].dtype == 'object':
                df_filtered = df[df[column].str.contains(value, case=False, na=False)]
            else:
                raise HTTPException(status_code=400, detail="Operador 'contains' só funciona com colunas de texto")
                
        else:
            raise HTTPException(status_code=400, detail=f"Operador '{operator}' não suportado")
        
        # Limita os resultados
        df_result = df_filtered.head(limit)
        
        # Converte timestamps para string
        for col in df_result.columns:
            if df_result[col].dtype == 'datetime64[ns]':
                df_result[col] = df_result[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        return {
            "filter_applied": f"{column} {operator} {value}",
            "total_matches": len(df_filtered),
            "returned_count": len(df_result),
            "data": df_result.to_dict(orient="records")
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao filtrar dados: {str(e)}")

@app.post("/filter_advanced")
def filter_advanced(filters: List[FilterRequest]):
    """Aplica múltiplos filtros aos dados"""
    try:
        df = load_parquet_data()
        
        for filter_req in filters:
            column = filter_req.column
            operator = filter_req.operator
            value = filter_req.value
            
            if column not in df.columns:
                raise HTTPException(status_code=400, detail=f"Coluna '{column}' não encontrada")
            
            # Aplica filtros similares ao endpoint anterior
            if operator == "eq":
                if df[column].dtype in ['int64', 'float64']:
                    value = float(value)
                    if df[column].dtype == 'int64':
                        value = int(value)
                df = df[df[column] == value]
            # ... outros operadores similares
        
        limit = filters[0].limit if filters else 1000
        df_result = df.head(limit)
        
        # Converte timestamps
        for col in df_result.columns:
            if df_result[col].dtype == 'datetime64[ns]':
                df_result[col] = df_result[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        return {
            "filters_applied": len(filters),
            "total_matches": len(df),
            "returned_count": len(df_result),
            "data": df_result.to_dict(orient="records")
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao aplicar filtros: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("API_PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)