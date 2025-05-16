
# 🦆 DuckDB Format Benchmark

> **Benchmark** automatizado comparando formatos de dados (Parquet, CSV e DuckDB nativo) no contexto de consultas analíticas com DuckDB.

---

## 🎯 Objetivo

Avaliar o desempenho do DuckDB processando um conjunto de dados sintético de vendas, salvo em diferentes formatos, medindo:

- Tempo de **gravação** (write)
- **Tamanho** em disco
- Tempo de **execução de queries analíticas**

---

## 📦 Estrutura do Projeto

```

.
├── dados/                  # Dados gerados (Parquet, CSV, DuckDB)
├── gerar\_relatorio.py      # Script principal de benchmark + notebook
├── relatorio.ipynb         # Notebook gerado automaticamente com resultados
└── README.md               # Este arquivo

````

---

## ⚙️ Tecnologias utilizadas

- [Python 3.10+](https://www.python.org/)
- [DuckDB](https://duckdb.org/)
- [pandas](https://pandas.pydata.org/)
- [tqdm](https://github.com/tqdm/tqdm)
- [nbformat](https://nbformat.readthedocs.io/)
- [Matplotlib](https://matplotlib.org/)

---

## 🚀 Como executar o projeto

### 1. Clone o repositório

```bash
git clone https://github.com/oDuPrado/Benchmark_case,git
cd duckdb-benchmark
````

### 2. Instale as dependências

```bash
pip install duckdb pandas tqdm nbformat matplotlib pyarrow
```

### 3. Execute o benchmark

```bash
python gerar_relatorio.py
```

> Isso irá:
>
> * Gerar ou reaproveitar um dataset sintético de 1 milhão de vendas
> * Converter o dataset para Parquet, CSV e DuckDB nativo
> * Executar 3 queries de BI em cada formato
> * Gerar o notebook `relatorio.ipynb` com gráficos e conclusão

---

## 📊 Queries analisadas

1. **vendas\_por\_loja**: total vendido por loja
2. **ticket\_medio\_cliente**: média de gasto por cliente
3. **produto\_top\_qtd**: produtos mais vendidos por quantidade

---

## 📈 Resultados esperados (exemplo)

| Formato | Write (s) | Tamanho (MB) | vendas\_por\_loja (s) | ticket\_medio\_cliente (s) | produto\_top\_qtd (s) |
| ------- | --------- | ------------ | --------------------- | -------------------------- | --------------------- |
| Parquet | 1.45      | 58.5         | 0.12                  | 0.11                       | 0.09                  |
| CSV     | 4.90      | 112.4        | 0.29                  | 0.25                       | 0.23                  |
| DuckDB  | 3.11      | 44.5         | 0.08                  | 0.07                       | 0.06                  |

---

## 🧠 Conclusões

* **Parquet** entrega ótimo equilíbrio entre compactação e leitura eficiente.
* **CSV** tem alta portabilidade, mas desempenho significativamente inferior.
* **DuckDB nativo** é o mais rápido, ideal para pipelines internos e uso local.

---

## 📓 Notebook final

O script gera automaticamente um notebook `relatorio.ipynb` com:

* Tabela de resultados
* Gráficos comparativos
* Discussões técnicas
* Conclusão estruturada em formato acadêmico

---

## 🤝 Contribuição

Contribuições são bem-vindas! Abra um [Pull Request](https://github.com/) ou uma [Issue](https://github.com/) com sugestões ou melhorias.

---

## 📄 Licença

Este projeto está sob a licença MIT. Consulte o arquivo `LICENSE` para mais informações.

```
