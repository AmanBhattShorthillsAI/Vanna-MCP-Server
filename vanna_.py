import os
import json
from tkinter import X
import weaviate
from vanna.weaviate.weaviate_vector import WeaviateDatabase
from vanna.base import VannaBase
from dotenv import load_dotenv

from langchain_openai import AzureChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage

from flask import Flask, request, jsonify
app = Flask(__name__)


import pandas as pd  # Added for Excel writing

load_dotenv()

ddl_statements = [
    {"ddl": """CREATE TABLE `account` (
  `account_id` INT,
  `district_id` INT,
  `frequency` TEXT,
  `date` DATE
);"""},
    {"ddl": """CREATE TABLE `card` (
  `card_id` INT,
  `disp_id` INT,
  `type` TEXT,
  `issued` DATE
);"""},
    {"ddl": """CREATE TABLE `client` (
  `client_id` INT,
  `gender` TEXT,
  `birth_date` DATE,
  `district_id` INT
);"""},
    {"ddl": """CREATE TABLE `disp` (
  `disp_id` INT,
  `client_id` INT,
  `account_id` INT,
  `type` TEXT
);"""},
    {"ddl": """CREATE TABLE `district` (
  `district_id` INT,
  `A2` TEXT,
  `A3` TEXT,
  `A4` TEXT,
  `A5` TEXT,
  `A6` TEXT,
  `A7` TEXT,
  `A8` INT,
  `A9` INT,
  `A10` REAL,
  `A11` INT,
  `A12` REAL,
  `A13` REAL,
  `A14` INT,
  `A15` INT,
  `A16` INT
);"""},
    {"ddl": """CREATE TABLE `loan` (
  `loan_id` INT,
  `account_id` INT,
  `date` DATE,
  `amount` INT,
  `duration` INT,
  `payments` REAL,
  `status` TEXT
);"""},
    {"ddl": """CREATE TABLE `order` (
  `order_id` INT,
  `account_id` INT,
  `bank_to` TEXT,
  `account_to` INT,
  `amount` REAL,
  `k_symbol` TEXT
);"""},
    {"ddl": """CREATE TABLE `trans` (
  `trans_id` INT,
  `account_id` INT,
  `date` DATE,
  `type` TEXT,
  `operation` TEXT,
  `amount` INT,
  `balance` INT,
  `k_symbol` TEXT,
  `bank` TEXT,
  `account` INT
);"""}
]

documentation_entries = [
    {"documentation": """Table `account`: Contains information about each account, including its creation date, location, and frequency of statement issuance. This table holds the primary details for each bank account.
Columns:
- `account_id`: The unique ID of the account.
- `district_id`: The ID for the location of the branch, foreign key to the district table.
- `frequency`: Frequency of statement issuance. Values are: 'POPLATEK MESICNE' (monthly issuance), 'POPLATEK TYDNE' (weekly issuance), 'POPLATEK PO OBRATU' (issuance after transaction).
- `date`: The creation date of the account in YYMMDD format."""},
    {"documentation": """Table `card`: Details about credit cards issued to clients. This table lists all credit cards associated with dispositions.
Columns:
- `card_id`: The unique ID number of the credit card.
- `disp_id`: The disposition ID, foreign key to the disp table.
- `type`: The type of credit card. Values are: 'junior' (junior class), 'classic' (standard class), 'gold' (high-level class).
- `issued`: The date when the credit card was issued in YYMMDD format."""},
    {"documentation": """Table `client`: Contains personal demographic information about the bank's clients.
Columns:
- `client_id`: The unique ID number for the client.
- `gender`: The gender of the client. Values are: 'F' (female), 'M' (male).
- `birth_date`: The birth date of the client in YYMMDD format.
- `district_id`: The ID for the location of the client's branch, foreign key to the district table."""},
    {"documentation": """Table `disp`: Links clients to their accounts and specifies their rights. This table acts as a mapping between clients and accounts.
Columns:
- `disp_id`: A unique ID for the disposition record.
- `client_id`: The ID of the client, foreign key to the client table.
- `account_id`: The ID of the account, foreign key to the account table.
- `type`: The type of disposition or role of the client for that account. Values are: 'OWNER' (the account owner), 'DISPONENT' (a user with rights to the account)."""},
    {"documentation": """Table `district`: Provides demographic and economic statistics for different geographic districts.
Columns:
- `district_id`: The unique ID for the district.
- `A2`: The name of the district (district_name).
- `A3`: The region the district belongs to.
- `A4`: The number of inhabitants in the district.
- `A5`: Number of municipalities with inhabitants < 499.
- `A6`: Number of municipalities with inhabitants 500-1999.
- `A7`: Number of municipalities with inhabitants 2000-9999.
- `A8`: Number of municipalities with inhabitants > 10000.
- `A9`: Number of cities.
- `A10`: The ratio of urban inhabitants.
- `A11`: The average salary in the district.
- `A12`: The unemployment rate in 1995.
- `A13`: The unemployment rate in 1996.
- `A14`: The number of entrepreneurs per 1000 inhabitants.
- `A15`: The number of committed crimes in 1995.
- `A16`: The number of committed crimes in 1996."""},
    {"documentation": """Table `loan`: Contains information about approved loans for accounts.
Columns:
- `loan_id`: The unique ID for the loan.
- `account_id`: The ID of the account the loan is associated with, foreign key to the account table.
- `date`: The date the loan was approved in YYMMDD format.
- `amount`: The approved loan amount in USD.
- `duration`: The loan duration in months.
- `payments`: The monthly payment amount in USD.
- `status`: The repayment status of the loan. Values are: 'A' (contract finished, no problems), 'B' (contract finished, loan not paid), 'C' (running contract, OK so far), 'D' (running contract, client in debt)."""},
    {"documentation": """Table `order`: Contains information about permanent (standing) orders from accounts.
Columns:
- `order_id`: The unique ID for the standing order.
- `account_id`: The ID of the account the order is from, foreign key to the account table.
- `bank_to`: The bank of the recipient.
- `account_to`: The account number of the recipient.
- `amount`: The debited amount.
- `k_symbol`: A characterization of the payment's purpose. Values are: 'POJISTNE' (insurance payment), 'SIPO' (household payment), 'LEASING' (leasing payment), 'UVER' (loan payment)."""},
    {"documentation": """Table `trans`: Records all transactions for accounts, providing a detailed log.
Columns:
- `trans_id`: The unique ID for the transaction.
- `account_id`: The ID of the account for the transaction, foreign key to the account table.
- `date`: The date of the transaction in YYMMDD format.
- `type`: The type of transaction. Values are: 'PRIJEM' (credit/income), 'VYDAJ' (withdrawal/expenditure).
- `operation`: The mode of the transaction. Values include: 'VYBER KARTOU' (credit card withdrawal), 'VKLAD' (credit in cash), 'PREVOD Z UCTU' (collection from another bank), 'VYBER' (withdrawal in cash), 'PREVOD NA UCET' (remittance to another bank).
- `amount`: The amount of money in USD for the transaction.
- `balance`: The account balance after the transaction in USD.
- `k_symbol`: A characterization of the transaction's purpose. Values include: 'POJISTNE' (insurance payment), 'SLUZBY' (payment for a service), 'UROK' (interest credited), 'SANKC. UROK' (sanction interest for negative balance), 'SIPO' (household payment), 'DUCHOD' (pension), 'UVER' (loan payment).
- `bank`: The bank of the transaction partner.
- `account`: The account of the transaction partner."""}
]

training_data = [
    # =================================================================
    # Simple Queries
    # =================================================================
    {
        "question": "How many total clients does the bank have?",
        "sql": "SELECT COUNT(client_id) FROM client;"
    },
    {
        "question": "What are the different types of credit cards the bank offers?",
        "sql": "SELECT DISTINCT type FROM card;"
    },
    {
        "question": "Show me the total number of accounts for each statement frequency.",
        "sql": "SELECT frequency, COUNT(account_id) as num_accounts FROM account GROUP BY frequency;"
    },
    {
        "question": "Which 10 districts have the highest average salary?",
        "sql": "SELECT A2 as district_name, A11 as average_salary FROM district ORDER BY A11 DESC LIMIT 10;"
    },
    {
        "question": "What is the total loan amount for each loan status?",
        "sql": "SELECT status, SUM(amount) as total_loan_amount FROM loan GROUP BY status;"
    },
    {
        "question": "How many gold cards have been issued?",
        "sql": "SELECT COUNT(card_id) FROM card WHERE type = 'gold';"
    },
    {
        "question": "What is the total number of transactions recorded?",
        "sql": "SELECT COUNT(trans_id) FROM trans;"
    },
    {
        "question": "What is the average loan amount?",
        "sql": "SELECT AVG(amount) FROM loan;"
    },
    {
        "question": "List all accounts created in 1997.",
        "sql": "SELECT account_id, date FROM account WHERE SUBSTR(date, 1, 2) = '97';"
    },
    {
        "question": "What are the different transaction operations available?",
        "sql": "SELECT DISTINCT operation FROM trans;"
    },
    {
        "question": "How many male vs female clients are there?",
        "sql": "SELECT gender, COUNT(client_id) FROM client GROUP BY gender;"
    },
    {
        "question": "What are the different payment characterizations (k_symbol) for standing orders?",
        "sql": "SELECT DISTINCT k_symbol FROM `order`;"
    },
    {
        "question": "Find the 5 largest loans by amount.",
        "sql": "SELECT loan_id, amount FROM loan ORDER BY amount DESC LIMIT 5;"
    },
    {
        "question": "How many accounts are in district with ID 1?",
        "sql": "SELECT COUNT(account_id) FROM account WHERE district_id = 1;"
    },
    {
        "question": "What are the different regions listed in the district table?",
        "sql": "SELECT DISTINCT A3 FROM district;"
    },

    # =================================================================
    # Moderate Queries
    # =================================================================
    {
        "question": "How many 'gold' credit cards are held by female clients?",
        "sql": """
SELECT COUNT(ca.card_id) 
FROM client cl
JOIN disp d ON cl.client_id = d.client_id
JOIN card ca ON d.disp_id = ca.disp_id
WHERE cl.gender = 'F' AND ca.type = 'gold';
"""
    },
    {
        "question": "What is the total transaction amount for accounts located in the 'Prague' district?",
        "sql": """
SELECT SUM(t.amount) 
FROM trans t
JOIN account a ON t.account_id = a.account_id
JOIN district d ON a.district_id = d.district_id
WHERE d.A2 = 'Prague';
"""
    },
    {
        "question": "List the client IDs and birth dates for clients who own an account with monthly statement issuance.",
        "sql": """
SELECT c.client_id, c.birth_date
FROM client c
JOIN disp d ON c.client_id = d.client_id
JOIN account a ON d.account_id = a.account_id
WHERE a.frequency = 'POPLATEK MESICNE' AND d.type = 'OWNER';
"""
    },
    {
        "question": "Find all clients who have a loan but do not have a credit card.",
        "sql": """
SELECT DISTINCT c.client_id
FROM client c
JOIN disp d ON c.client_id = d.client_id
WHERE d.account_id IN (SELECT account_id FROM loan)
  AND d.disp_id NOT IN (SELECT disp_id FROM card);
"""
    },
    {
        "question": "What is the average loan amount for male clients living in a region with an unemployment rate in 1996 (A13) higher than 5%?",
        "sql": """
SELECT AVG(l.amount)
FROM loan l
JOIN account a ON l.account_id = a.account_id
JOIN disp d ON a.account_id = d.account_id
JOIN client c ON d.client_id = c.client_id
JOIN district di ON c.district_id = di.district_id
WHERE c.gender = 'M' AND di.A13 > 5.0;
"""
    },
    {
        "question": "Which district has the highest number of bank accounts?",
        "sql": """
SELECT d.A2 as district_name, COUNT(a.account_id) as num_accounts
FROM district d
JOIN account a ON d.district_id = a.district_id
GROUP BY d.A2
ORDER BY num_accounts DESC
LIMIT 1;
"""
    },
    {
        "question": "List clients who are owners of more than one account.",
        "sql": """
SELECT c.client_id
FROM client c
JOIN disp d ON c.client_id = d.client_id
WHERE d.type = 'OWNER'
GROUP BY c.client_id
HAVING COUNT(d.account_id) > 1;
"""
    },
    {
        "question": "What is the total amount of loans given to clients in each region?",
        "sql": """
SELECT di.A3 as region, SUM(l.amount) as total_loan_amount
FROM loan l
JOIN account a ON l.account_id = a.account_id
JOIN district di ON a.district_id = di.district_id
GROUP BY di.A3;
"""
    },
    {
        "question": "Find the number of transactions for each type of credit card.",
        "sql": """
SELECT ca.type, COUNT(t.trans_id) as num_transactions
FROM trans t
JOIN account a ON t.account_id = a.account_id
JOIN disp d ON a.account_id = d.account_id
JOIN card ca ON d.disp_id = ca.disp_id
GROUP BY ca.type;
"""
    },
    {
        "question": "List all standing orders for household payments ('SIPO') that are greater than the average household payment amount.",
        "sql": """
SELECT
  d.A2,
  SUM(t.amount)
FROM district AS d
JOIN account AS a ON d.district_id = a.district_id
JOIN trans AS t ON a.account_id = t.account_id
GROUP BY
  d.A2
"""
    },
    {
        "question": "Show me the total transaction amount for each district.",
        "sql": """
SELECT * FROM `order`
WHERE k_symbol = 'SIPO'
  AND amount > (SELECT AVG(amount) FROM `order` WHERE k_symbol = 'SIPO');
"""
    },

    # =================================================================
    # Hard Queries
    # =================================================================
    {
        "question": "For each district, find the client who made the single largest transaction and show that transaction amount.",
        "sql": """
WITH RankedTransactions AS (
    SELECT
        d.A2 as district_name,
        c.client_id,
        t.amount,
        RANK() OVER(PARTITION BY d.A2 ORDER BY t.amount DESC) as rn
    FROM trans t
    JOIN account a ON t.account_id = a.account_id
    JOIN disp di ON a.account_id = di.account_id
    JOIN client c ON di.client_id = c.client_id
    JOIN district d ON a.district_id = d.district_id
)
SELECT district_name, client_id, amount
FROM RankedTransactions
WHERE rn = 1;
"""
    },
    {
        "question": "Calculate the month-over-month growth rate of the total withdrawal ('VYDAJ') transaction volume.",
        "sql": """
WITH MonthlyVolume AS (
    SELECT
        STRFTIME('%Y-%m', date) as transaction_month,
        SUM(amount) as total_volume
    FROM trans
    WHERE type = 'VYDAJ'
    GROUP BY transaction_month
)
SELECT
    transaction_month,
    total_volume,
    (total_volume - LAG(total_volume, 1, 0) OVER (ORDER BY transaction_month)) * 100.0 / LAG(total_volume, 1, 0) OVER (ORDER BY transaction_month) as growth_percentage
FROM MonthlyVolume
WHERE LAG(total_volume, 1, 0) OVER (ORDER BY transaction_month) > 0;
"""
    },
    {
        "question": "Find the average number of days between a client's account creation and them taking out their first loan.",
        "sql": """
WITH FirstLoan AS (
    SELECT
        account_id,
        MIN(date) as first_loan_date
    FROM loan
    GROUP BY account_id
)
SELECT
    AVG(JULIANDAY(fl.first_loan_date) - JULIANDAY(a.date)) as avg_days_to_first_loan
FROM account a
JOIN FirstLoan fl ON a.account_id = fl.account_id;
"""
    },
    {
        "question": "List the top 3 districts by the ratio of total loan amount to the number of inhabitants.",
        "sql": """
WITH DistrictLoanSummary AS (
    SELECT
        d.district_id,
        d.A2 as district_name,
        CAST(d.A4 AS INTEGER) as inhabitants,
        SUM(l.amount) as total_loan_amount
    FROM district d
    JOIN account a ON d.district_id = a.district_id
    JOIN loan l ON a.account_id = l.account_id
    GROUP BY d.district_id, d.A2, d.A4
)
SELECT
    district_name,
    total_loan_amount,
    inhabitants,
    (total_loan_amount * 1.0 / inhabitants) as loan_per_capita
FROM DistrictLoanSummary
ORDER BY loan_per_capita DESC
LIMIT 3;
"""
    },
    {
        "question": "Identify clients who have a 'gold' card and have an average transaction balance greater than the overall average transaction balance for all gold card holders.",
        "sql": """
WITH GoldCardHolders AS (
    SELECT d.client_id
    FROM card c
    JOIN disp d ON c.disp_id = d.disp_id
    WHERE c.type = 'gold'
),
OverallGoldAvgBalance AS (
    SELECT AVG(t.balance) as avg_balance
    FROM trans t
    JOIN disp d ON t.account_id = d.account_id
    WHERE d.client_id IN (SELECT client_id FROM GoldCardHolders)
),
ClientAvgBalance AS (
    SELECT
        d.client_id,
        AVG(t.balance) as avg_client_balance
    FROM trans t
    JOIN disp d ON t.account_id = d.account_id
    WHERE d.client_id IN (SELECT client_id FROM GoldCardHolders)
    GROUP BY d.client_id
)
SELECT cab.client_id
FROM ClientAvgBalance cab
CROSS JOIN OverallGoldAvgBalance oab
WHERE cab.avg_client_balance > oab.avg_balance;
"""
    },
    {
        "question": "For each region, what is the percentage of accounts that have taken out a loan?",
        "sql": """
SELECT
    d.A3 as region,
    COUNT(DISTINCT l.account_id) * 100.0 / COUNT(DISTINCT a.account_id) as percentage_with_loan
FROM district d
LEFT JOIN account a ON d.district_id = a.district_id
LEFT JOIN loan l ON a.account_id = l.account_id
GROUP BY d.A3;
"""
    },
    {
        "question": "Find the running total of transaction amounts for each account, ordered by date.",
        "sql": """
SELECT
    account_id,
    date,
    amount,
    SUM(amount) OVER (PARTITION BY account_id ORDER BY date) as running_total
FROM trans
ORDER BY account_id, date;
"""
    },
    {
        "question": "Which clients have had a transaction every single month of 1997?",
        "sql": """
WITH ClientMonthlyTransactions AS (
    SELECT
        d.client_id,
        STRFTIME('%Y-%m', t.date) as transaction_month
    FROM trans t
    JOIN disp d ON t.account_id = d.account_id
    WHERE STRFTIME('%Y', t.date) = '1997'
    GROUP BY d.client_id, transaction_month
)
SELECT client_id
FROM ClientMonthlyTransactions
GROUP BY client_id
HAVING COUNT(transaction_month) = 12;
"""
    },
    {
        "question": "Who is the owner of the account with the largest loan amount?",
        "sql": """
SELECT
  c.client_id
FROM client AS c
JOIN disp AS d ON c.client_id = d.client_id
JOIN loan AS l ON d.account_id = l.account_id
WHERE
  d.type = 'OWNER'
ORDER BY
  l.amount DESC
LIMIT 1;
"""
    },
    {
        "question": "What is the gender of the oldest client who opened his/her account in the 'Prague' district?",
        "sql": """
SELECT
  c.gender
FROM client AS c
JOIN disp AS d ON c.client_id = d.client_id
JOIN account AS a ON d.account_id = a.account_id
JOIN district AS dist ON a.district_id = dist.district_id
WHERE
  dist.A2 = 'Prague'
ORDER BY
  c.birth_date ASC
LIMIT 1;
"""
    },
    {
        "question": "List the account numbers of clients from 'East Bohemia' who have a running loan contract.",
        "sql": """
SELECT
  a.account_id
FROM account AS a
JOIN district AS dist ON a.district_id = dist.district_id
JOIN loan AS l ON a.account_id = l.account_id
WHERE
  dist.A3 = 'East Bohemia' AND l.status IN ('C', 'D');
"""
    },
    {
        "question": "How many female clients opened their accounts in the 'Jesenik' district?",
        "sql": """
SELECT
  COUNT(c.client_id)
FROM client AS c
JOIN disp AS d ON c.client_id = d.client_id
JOIN account AS a ON d.account_id = a.account_id
JOIN district AS dist ON a.district_id = dist.district_id
WHERE
  c.gender = 'F' AND dist.A2 = 'Jesenik';
"""
    },
    {
        "question": "Who placed the order with the id 32423?",
        "sql": """
SELECT
  c.client_id
FROM client AS c
JOIN disp AS d ON c.client_id = d.client_id
JOIN `order` AS o ON d.account_id = o.account_id
WHERE
  o.order_id = 32423 AND d.type = 'OWNER';
"""
    },
    {
        "question": "What is the region of the client with the id 3541 from?",
        "sql": """
SELECT
  d.A3
FROM district AS d
JOIN client AS c ON d.district_id = c.district_id
WHERE
  c.client_id = 3541;
"""
    },
    {
        "question": "How much is the average amount in credit card transactions made by account holders in the year 2021?",
        "sql": """
SELECT
  AVG(t.amount)
FROM trans AS t
JOIN disp AS d ON t.account_id = d.account_id
JOIN card AS c ON d.disp_id = c.disp_id
WHERE
  STRFTIME('%Y', t.date) = '2021' AND t.operation = 'VYBER KARTOU';
"""
    },
    {
        "question": "List the account numbers of female clients who are oldest and have the lowest average salary in their district.",
        "sql": """
SELECT
  a.account_id
FROM account AS a
JOIN client AS c ON a.district_id = c.district_id
JOIN district AS d ON a.district_id = d.district_id
JOIN disp ON c.client_id = disp.client_id AND a.account_id = disp.account_id
WHERE
  c.gender = 'F' AND disp.type = 'OWNER'
ORDER BY
  c.birth_date ASC, d.A11 ASC
LIMIT 1;
"""
    },
    {
        "question": "How many accounts in 'North Bohemia' have made a transaction with the partner's bank being 'AB'?",
        "sql": """
SELECT
  COUNT(DISTINCT a.account_id)
FROM account AS a
JOIN district AS d ON a.district_id = d.district_id
JOIN trans AS t ON a.account_id = t.account_id
WHERE
  d.A3 = 'North Bohemia' AND t.bank = 'AB';
"""
    }
]

def train_vanna(vn):
    """
        Trains the Vanna instance with DDL, documentation, and question-SQL pairs.
    """
    print("\n--- Training on DDL statements ---")
    for item in ddl_statements:
        vn.train(ddl=item["ddl"])
    print("DDL training complete.")

    print("\n--- Training on Documentation ---")
    for item in documentation_entries:
        vn.train(documentation=item["documentation"])
    print("Documentation training complete.")

    print("\n--- Training on Question-SQL pairs ---")
    for pair in training_data:
        ingested_id = vn.train(question=pair["question"], sql=pair["sql"])
        print(f"  - Ingested Q: '{pair['question']}' | Received ID: {ingested_id}")
    print("Question-SQL training complete.")

class LangChainAzureChat(VannaBase):
    def __init__(self, config=None):
        super().__init__(config=config)
        self.llm = AzureChatOpenAI(
            azure_deployment="gpt-4.1",
            api_version="2024-02-15-preview",
            temperature=0.0,
            max_tokens=1000,
            api_key=os.getenv("OPENAI_API_KEY"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        )

    def system_message(self, message: str) -> SystemMessage:
        return SystemMessage(content=message)

    def user_message(self, message: str) -> HumanMessage:
        return HumanMessage(content=message)

    def assistant_message(self, message: str) -> AIMessage:
        return AIMessage(content=message)

    def submit_prompt(self, prompt, **kwargs) -> str:
        response = self.llm.invoke(prompt)
        return response.content

class MyVanna(WeaviateDatabase, LangChainAzureChat):
    def __init__(self, config=None):
        self.config = config or {}
        WeaviateDatabase.__init__(self, config=config)
        LangChainAzureChat.__init__(self, config=config)

    def _initialize_weaviate_client(self):
        if self.config.get("weaviate_api_key"):
            return weaviate.connect_to_weaviate_cloud(
                cluster_url=self.config["weaviate_url"],
                auth_credentials=weaviate.auth.AuthApiKey(self.config["weaviate_api_key"]),
                skip_init_checks=True
            )
        else:
            raise ValueError("Weaviate API key is required for online Weaviate.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.weaviate_client.close()
        print("\nWeaviate connection closed successfully.")

if __name__ == '__main__':

    config = {
        "weaviate_url": os.getenv("WEAVIATE_URL"),
        "weaviate_api_key": os.getenv("WEAVIATE_API_KEY"),
    }

    with MyVanna(config=config) as vn:
        vn.connect_to_sqlite('financial.sqlite')

        train_vanna(vn)

        # with open('dev.json', 'r') as f:
        #     dev_data = json.load(f)

        # INSERT_YOUR_CODE
        import pandas as pd
        import json

        # Try to load ground truth SQLs from ground.xlsx using openpyxl engine
        try:
            ground_df = pd.read_excel('ground.xlsx', engine='openpyxl')
        except Exception as e:
            raise RuntimeError(
                "Failed to read 'ground.xlsx'. Please ensure the file exists and is a valid Excel file. "
                "You may need to install 'openpyxl' (pip install openpyxl). Original error: " + str(e)
            )

        # Accept both 'Ground Truth SQL' and 'ground truth sql' as column names
        col_candidates = [col for col in ground_df.columns if col.strip().lower() in ['ground truth sql', 'ground_truth_sql']]
        if col_candidates:
            ground_sqls = ground_df[col_candidates[0]].astype(str).tolist()
        else:
            raise ValueError("Column 'Ground Truth SQL' or 'ground truth sql' not found in ground.xlsx")

        # Load dev.json
        with open('dev.json', 'r') as f:
            dev_data = json.load(f)

        # Build a mapping from normalized SQL to question in dev.json
        def normalize_sql(sql):
            return ' '.join(sql.lower().split())

        sql_to_question = {}
        for item in dev_data:
            sql = str(item.get('SQL', '')).strip()
            question = item.get('question', '')
            if sql:
                sql_to_question[normalize_sql(sql)] = question

        # For each ground truth SQL, get the corresponding question
        questions = []
        for sql in ground_sqls:
            sql_stripped = str(sql).strip()
            question = sql_to_question.get(normalize_sql(sql_stripped), None)
            questions.append({'question': question})

        # Save the questions to questions.xlsx
        questions_df = pd.DataFrame(questions)
        questions_df.to_excel('questions.xlsx', index=False)

        predictions = {}
        debug_rows = []  # For storing prompt and SQL for Excel

        for idx, item in enumerate(dev_data):
            question = item.get('question', '')
            db_id = item.get('db_id', '')
            try:
                # Generate the SQL prompt
                prompt = vn.get_sql_prompt(
                    initial_prompt=vn.config.get("initial_prompt", None) if hasattr(vn, "config") else None,
                    question=question,
                    question_sql_list=vn.get_similar_question_sql(question),
                    ddl_list=vn.get_related_ddl(question),
                    doc_list=vn.get_related_documentation(question),
                )
                # print(f"\n[SQL PROMPT for idx {idx}]:\n{prompt}\n")
                sql_query = vn.generate_sql(question=question, allow_llm_to_see_data=True)
            except Exception as e:
                prompt = f"Error: {str(e)}"
                sql_query = f"Error: {str(e)}"

            predictions[str(idx)] = f"{sql_query}\t----- bird -----\t{db_id}"
            # print(f"[{idx}] {question}\nSQL: {sql_query}\n")

            debug_rows.append({
                "idx": idx,
                "question": question,
                "db_id": db_id,
                "sql_prompt": prompt,
                "sql_query": sql_query
            })

        with open('predict_dev.json', 'w') as f:
            json.dump(predictions, f, indent=2)

        # Save the debug info to Excel
        debug_df = pd.DataFrame(debug_rows)
        debug_df.to_excel("vanna_sql_prompt_debug.xlsx", index=False)

        print("\nAll predictions saved to predict_dev.json\n")
        print("SQL prompts and queries saved to vanna_sql_prompt_debug.xlsx\n")
