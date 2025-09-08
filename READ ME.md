# 🗄️ SQL ETL Pipeline Simulation  

## 🔎 Project Overview  
The **SQL ETL Pipeline Simulation** is a hands-on project that demonstrates how raw data can be extracted from external sources (CSV files), transformed into clean and structured information, and then loaded into a data warehouse for reporting and analytics. The project is built entirely with **SQL Server Management Studio (SSMS)** using **T-SQL** scripts and stored procedures.  

It simulates a real-world data engineering workflow by staging incoming data, applying business rules (like cleaning invalid records, removing duplicates, and standardizing formats), and loading it into a star schema (`DimCustomer`, `FactOrder`). The pipeline also includes auditing and error logging to track execution details and ensure reliability.  

---

## 📝 Introduction  
In today’s data-driven world, organizations rely on **ETL pipelines** to move and transform data from raw sources into actionable insights. While modern pipelines often involve complex tools, this project shows that the **core ETL process can be fully implemented in SQL**.  

This simulation is designed for **students, beginners, and data enthusiasts** who want to understand how ETL works at its foundation. By following step-by-step SQL scripts, you’ll learn how to:  
- Load external data into SQL Server  
- Clean and transform data with T-SQL  
- Build a simple data warehouse model  
- Implement auditing and error handling  
- Run a repeatable ETL process with stored procedures  

The result is a lightweight, practical project that demonstrates the **essentials of data engineering** — without requiring advanced tools or platforms.  

---

## 🚀 What This Project Does
- **Extract** → Loads CSV files (`customers.csv`, `orders.csv`) into staging tables.  
- **Transform** → Cleans messy data (fixes bad dates, invalid amounts, duplicates).  
- **Load** → Inserts clean records into a star schema (`DimCustomer`, `FactOrder`).  
- **Audit** → Tracks each ETL run and logs errors for transparency.  
- **Schedule** → Runs daily with SQL Server Agent or Windows Task Scheduler.  

**Data Flow:**  
