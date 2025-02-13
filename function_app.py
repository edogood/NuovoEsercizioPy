import azure.functions as func
import logging
import pyodbc
import json

# Stringa di connessione al database SQL Server
CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=DATA-BUONO\\SQLSERVERITS;"
    "DATABASE=Star_TrekProva;"
    "Trusted_Connection=yes;"
)

# Funzione per connettersi al database
def get_db_connection():
    try:
        conn = pyodbc.connect(CONNECTION_STRING)
        return conn
    except Exception as e:
        logging.error(f"Errore connessione DB: {e}")
        return None

# Funzione per registrare i log delle operazioni nel database
def log_operation(account_id, operation, status):
    try:
        conn = get_db_connection()
        if not conn:
            return
        
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO AccountLogs (AccountID, Operation, Status, Timestamp)
            VALUES (?, ?, ?, GETDATE())
        """, (account_id, operation, status))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Errore registrazione log: {e}")

# Definizione della FunctionApp
app = func.FunctionApp()

# API per ottenere tutti i conti correnti
@app.route(route="get_accounts")
def get_accounts(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Recupero di tutti i conti correnti.")
    try:
        conn = get_db_connection()
        if not conn:
            return func.HttpResponse("Errore di connessione al database.", status_code=500)

        cursor = conn.cursor()
        cursor.execute("SELECT AccountID, Name, Balance FROM Accounts")
        rows = cursor.fetchall()

        accounts = [{"AccountID": row[0], "Name": row[1], "Balance": row[2]} for row in rows]

        cursor.close()
        conn.close()
        return func.HttpResponse(json.dumps(accounts), mimetype="application/json", status_code=200)

    except Exception as e:
        logging.error(f"Errore recupero conti: {e}")
        return func.HttpResponse(f"Errore: {e}", status_code=500)

#  API per ottenere i dettagli di un conto corrente specifico
@app.route(route="get_account/{account_id}")
def get_account(req: func.HttpRequest) -> func.HttpResponse:
    account_id = req.route_params.get("account_id")
    logging.info(f"Recupero dettagli conto {account_id}.")
    try:
        conn = get_db_connection()
        if not conn:
            return func.HttpResponse("Errore di connessione al database.", status_code=500)

        cursor = conn.cursor()
        cursor.execute("SELECT AccountID, Name, Balance FROM Accounts WHERE AccountID = ?", (account_id,))
        row = cursor.fetchone()

        if row:
            account = {"AccountID": row[0], "Name": row[1], "Balance": row[2]}
            log_operation(account_id, "GET_ACCOUNT", "SUCCESS")
            response = func.HttpResponse(json.dumps(account), mimetype="application/json", status_code=200)
        else:
            log_operation(account_id, "GET_ACCOUNT", "NOT_FOUND")
            response = func.HttpResponse("Conto non trovato.", status_code=404)

        cursor.close()
        conn.close()
        return response

    except Exception as e:
        logging.error(f"Errore recupero conto {account_id}: {e}")
        log_operation(account_id, "GET_ACCOUNT", "ERROR")
        return func.HttpResponse(f"Errore: {e}", status_code=500)

#  API per creare un nuovo conto corrente
@app.route(route="create_account", methods=["POST"])
def create_account(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        name = req_body.get("name")
        balance = req_body.get("balance")

        if not name or balance is None:
            return func.HttpResponse("Dati mancanti.", status_code=400)

        conn = get_db_connection()
        if not conn:
            return func.HttpResponse("Errore di connessione al database.", status_code=500)

        cursor = conn.cursor()
        cursor.execute("INSERT INTO Accounts (Name, Balance) OUTPUT INSERTED.AccountID VALUES (?, ?)", (name, balance))
        new_account_id = cursor.fetchone()[0]  # Ottiene l'ID generato
        conn.commit()

        log_operation(new_account_id, "CREATE_ACCOUNT", "SUCCESS")

        cursor.close()
        conn.close()
        return func.HttpResponse(f"Conto creato con ID {new_account_id}.", status_code=201)

    except Exception as e:
        logging.error(f"Errore creazione conto: {e}")
        return func.HttpResponse(f"Errore: {e}", status_code=500)


    except Exception as e:
        logging.error(f"Errore creazione conto: {e}")
        return func.HttpResponse(f"Errore: {e}", status_code=500)

#  API per aggiornare un conto corrente
@app.route(route="update_account/{account_id}", methods=["PUT"])
def update_account(req: func.HttpRequest) -> func.HttpResponse:
    account_id = req.route_params.get("account_id")
    try:
        req_body = req.get_json()
        name = req_body.get("name")
        balance = req_body.get("balance")

        if not name and balance is None:
            return func.HttpResponse("Dati mancanti.", status_code=400)

        conn = get_db_connection()
        if not conn:
            return func.HttpResponse("Errore di connessione al database.", status_code=500)

        cursor = conn.cursor()
        cursor.execute("UPDATE Accounts SET Name = ?, Balance = ? WHERE AccountID = ?", (name, balance, account_id))
        conn.commit()

        log_operation(account_id, "UPDATE_ACCOUNT", "SUCCESS")

        cursor.close()
        conn.close()
        return func.HttpResponse("Conto aggiornato con successo.", status_code=200)

    except Exception as e:
        logging.error(f"Errore aggiornamento conto {account_id}: {e}")
        return func.HttpResponse(f"Errore: {e}", status_code=500)

# API per eliminare un conto corrente
@app.route(route="delete_account/{account_id}", methods=["DELETE"])
def delete_account(req: func.HttpRequest) -> func.HttpResponse:
    account_id = req.route_params.get("account_id")
    try:
        conn = get_db_connection()
        if not conn:
            return func.HttpResponse("Errore di connessione al database.", status_code=500)

        cursor = conn.cursor()
        cursor.execute("DELETE FROM Accounts WHERE AccountID = ?", (account_id,))
        conn.commit()

        log_operation(account_id, "DELETE_ACCOUNT", "SUCCESS")

        cursor.close()
        conn.close()
        return func.HttpResponse("Conto eliminato con successo.", status_code=200)

    except Exception as e:
        logging.error(f"Errore eliminazione conto {account_id}: {e}")
        return func.HttpResponse(f"Errore: {e}", status_code=500)
