import azure.functions as func
import logging
import pyodbc
import json
import os
from decimal import Decimal

# Stringa di connessione al database SQL Server
"""CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=DATA-BUONO\\SQLSERVERITS;"
    "DATABASE=Star_TrekProva;"
    "Trusted_Connection=yes;"
)"""

# Funzione per connettersi al database
"""def get_db_connection():
    try:
        return pyodbc.connect(CONNECTION_STRING)
    except Exception as e:
        logging.error(f"Errore connessione DB: {e}")
        return None"""
        
def get_db_connection():
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        f'SERVER={os.getenv("DB_SERVER")};'
        f'DATABASE={os.getenv("DB_NAME")};'
        f'Trusted_Connection={os.getenv("Trusted_Connection")};'
    )
    return conn
    
# Serializzazione dei tipi Decimal per JSON
def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Tipo non serializzabile: {type(obj)}")

# Log operazioni
def log_operation(account_id, operation, status):
    try:
        with get_db_connection() as conn, conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO AccountLogs (AccountID, Operation, Status, Timestamp) VALUES (?, ?, ?, GETDATE())",
                (account_id, operation, status)
            )
            conn.commit()
    except Exception as e:
        logging.error(f"Errore log operazione {operation} per conto {account_id}: {e}")

# Controllo esistenza conto corrente
def if_account(account_id):
    try:
        with get_db_connection() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM Accounts WHERE AccountID = ?", (account_id,))
            return cursor.fetchone() is not None
    except Exception as e:
        logging.error(f"Errore controllo esistenza conto {account_id}: {e}")
        return False

# Definizione FunctionApp
app = func.FunctionApp()

# API: Recupero tutti i conti
@app.route(route="get_accounts")
def get_accounts(req: func.HttpRequest) -> func.HttpResponse:
    try:
        with get_db_connection() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT AccountID, Name, Balance FROM Accounts")
            accounts = [{"AccountID": r[0], "Name": r[1], "Balance": float(r[2])} for r in cursor.fetchall()]
        return func.HttpResponse(json.dumps(accounts, default=decimal_default), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.error(f"Errore recupero conti: {e}")
        return func.HttpResponse(f"Errore: {e}", status_code=500)

# API: Recupero conto specifico con if_account
@app.route(route="get_account/{account_id}")
def get_account(req: func.HttpRequest) -> func.HttpResponse:
    account_id = req.route_params.get("account_id")
    if not if_account(account_id):
        return func.HttpResponse("Conto non trovato.", status_code=404)
    try:
        with get_db_connection() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT AccountID, Name, Balance FROM Accounts WHERE AccountID = ?", (account_id,))
            row = cursor.fetchone()
            account = {"AccountID": row[0], "Name": row[1], "Balance": float(row[2])}
            log_operation(account_id, "GET_ACCOUNT", "SUCCESS")
            return func.HttpResponse(json.dumps(account, default=decimal_default), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.error(f"Errore recupero conto {account_id}: {e}")
        return func.HttpResponse(f"Errore: {e}", status_code=500)

# API: Eliminazione conto con if_account
@app.route(route="delete_account/{account_id}", methods=["DELETE"])
def delete_account(req: func.HttpRequest) -> func.HttpResponse:
    account_id = req.route_params.get("account_id")
    if not if_account(account_id):
        return func.HttpResponse("Conto non trovato.", status_code=404)
    try:
        with get_db_connection() as conn, conn.cursor() as cursor:
            cursor.execute("DELETE FROM Accounts WHERE AccountID = ?", (account_id,))
            conn.commit()
        log_operation(account_id, "DELETE_ACCOUNT", "SUCCESS")
        return func.HttpResponse("Conto eliminato con successo.", status_code=200)
    except Exception as e:
        logging.error(f"Errore eliminazione conto {account_id}: {e}")
        return func.HttpResponse(f"Errore: {e}", status_code=500)
    
    # API per creare un nuovo conto corrente
@app.route(route="create_account", methods=["POST"])
def create_account(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        name = req_body.get("name")
        balance = req_body.get("balance")

        if not name or balance is None:
            return func.HttpResponse("Dati mancanti.", status_code=400)

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("INSERT INTO Accounts (Name, Balance) OUTPUT INSERTED.AccountID VALUES (?, ?)", (name, balance))
                new_account_id = cursor.fetchone()[0]
                conn.commit()

        log_operation(new_account_id, "CREATE_ACCOUNT", "SUCCESS")
        return func.HttpResponse(f"Conto creato con ID {new_account_id}.", status_code=201)
    except Exception as e:
        logging.error(f"Errore creazione conto: {e}")
        return func.HttpResponse(f"Errore: {e}", status_code=500)

# API per aggiornare un conto corrente
@app.route(route="update_account/{account_id}", methods=["PUT"])
def update_account(req: func.HttpRequest) -> func.HttpResponse:
    account_id = req.route_params.get("account_id")
    try:
        req_body = req.get_json()
        name = req_body.get("name")
        balance = req_body.get("balance")

        if not name and balance is None:
            return func.HttpResponse("Dati mancanti.", status_code=400)

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("UPDATE Accounts SET Name = ?, Balance = ? WHERE AccountID = ?", (name, balance, account_id))
                conn.commit()

        log_operation(account_id, "UPDATE_ACCOUNT", "SUCCESS")
        return func.HttpResponse("Conto aggiornato con successo.", status_code=200)
    except Exception as e:
        logging.error(f"Errore aggiornamento conto {account_id}: {e}")
        return func.HttpResponse(f"Errore: {e}", status_code=500)
