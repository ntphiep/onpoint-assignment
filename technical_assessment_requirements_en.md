# Technical Assessment - Requirements

## Guidelines
- Consider this as a mini-hackathon, focusing on building a simple but complete pipeline.  
- Stop at the Proof-of-Concept (POC) state; don't over-engineer.  
- Pick **one public data source** you are most interested in.  
- Provide a thorough explanation of the solution design, including performance, limitations, costs, scalability, and effectiveness.  

## Requirements
- Your submission can be in any file format (e.g., `.py`, Notebook, Colab, GitHub repo). Choose wisely.  

## Problem Statement
Can we build a mini data pipeline from **crawling → ETL → storage**, and demonstrate understanding of **CDC & AWS DMS**?  

## Tasks

### 1. Crawling
- Crawl data from a **public API** and from a **web page** (e.g., news, e-commerce).  
- Save raw results as **JSON** files.  

### 2. Processing & File Formats
- Read the crawled JSON files.  
- Normalize schema into tabular format (using pandas or PySpark).  
- Export into **CSV, Parquet, Excel**.  
- Upload files to **AWS S3** (or simulate with a local folder if AWS is not available).  

### 3. ETL Pipeline
- **Extract**: read raw JSON.  
- **Transform**: clean & normalize schema.  
- **Load**: generate **INSERT/UPSERT** SQL scripts for PostgreSQL (output as `.sql` file, no need to connect to DB).  

### 4. CDC & AWS DMS (Conceptual)
Write a short document (1–2 pages) explaining:  
- How to **replicate an orders table from PostgreSQL → S3** using **CDC streaming**.  
- How to **replicate back from S3 → PostgreSQL** by batch (every hour).  
- Provide a sample **AWS DMS JSON config**.  

### 5. Orchestration & Scaling (Conceptual)
- If using **Airflow**, how would you design a DAG for this pipeline?  
- When scaling up to **100M+ records**, how would you optimize:  
  - Crawling  
  - File processing (CSV vs Parquet)  
  - CDC replication  
  - Performance & scalability  

## Timeline & Submission
- You will have 3 days from receiving this email to submit your work.  
- You can submit earlier if you finish before the deadline.  
- No further clarifications will be provided by us; you will do your best with your understanding.  

## Deliverables
- Source code (**Python, SQL, JSON configs**).  
- **README.md** with setup & execution guide.  
- Sample outputs (**CSV, Parquet, Excel**).  
- Documentation for **CDC & orchestration** (Parts 4 & 5).  

## Evaluation Criteria
- **Coding Skills (40%)** – Clean, working code with valid outputs.  
- **Data Processing (20%)** – Handling multiple file formats, clear schema.  
- **AWS & CDC Knowledge (20%)** – Understanding of DMS, S3, Postgres.  
- **System Thinking (20%)** – Pipeline design, orchestration, scalability.  
