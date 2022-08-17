import os
import time
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.snowpark import Session
import udfhandler


''''
Globals
    startTime: Timer variable to store the time at which the code began execution.
'''
startTime = 0


'''
Gets the byte representation of the RSA key.

Returns
    The byte representation of the {@Args pathToRSAKey}.

Args
    pathToRSAKey: The string representation of the path to the RSA key.
'''
def getPrivateKey(pathToRSAKey: str) -> bytes:
    with open(pathToRSAKey, "rb") as key:
        p_key= serialization.load_pem_private_key(
            key.read(),
            password=os.environ['SNOWFLAKE_KEY_PASS'].encode(),
            backend=default_backend())
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())
    return pkb


'''
Sets up and returns a snowflake.connector.connection.

Returns
    A snowflake.connector.connection with the {@Args}.

Args
    acc: String representation of the account to be used.
    user: String representation of the user to be connected.
    prv_key: String representation of the key to be used. (Hint: this can be found via 
             snowpark_connector.getPrivateKey()).
    role: String representation of the role to be used.
    warehouse: String representation of the warehouse to be used.
    database: String representation of the database to be used.
    schema: String representation of the schema to be used.
'''
def connectionSetup(acc: str, user: str, prv_key: str, role: str, warehouse: str, database: str, schema: str) -> dict:
    return {"account": acc,
            "user": user,
            "private_key": prv_key,
            "role": role,
            "warehouse": warehouse,
            "database": database,
            "schema": schema}


'''
Begins a connection to snowpark via a Session.

Returns
    The Session that the is running on.

Args
    connection_params: The snowflake.connector.connection necessary to connect to snowpark.
                       (Hint: this can be found via snowpark_connector.connectionSetup()).
'''
def beginConnection(connection_params) -> Session:
    print(f"\n\n\t---------< STARTING SESSION >---------\n\n")
    global startTime
    startTime = time.time()
    return Session.builder.configs(connection_params).create()


'''
Prints out {@Prints} diagnostics regarding {@Args session}.

Prints
    Packages in the current {@Args session}.
    Imports in the current {@Args session}.
    Immediate run-time of the current {@Args session}.
    Total number of UDFs analyzed in the current {@Args session}.

Args
    session: The current Session.
'''
def _getDiagnostics(session: Session):
    print(f"\n\t---------< SHOW DIAGNOSTICS >---------\n")
    packages = str(udfhandler._list_packages(session))
    modules = str(udfhandler._list_imports(session))
    path = udfhandler.getPath().replace("/", " ").replace("\\", "/")
    print(F"\t   > Current path: {path}")
    print(f"\t   > {len(udfhandler._list_packages(session))} Packages in the current session: {packages[1:len(packages) - 1]}")
    print(f"\t   > {len(udfhandler._list_imports(session))} Modules in the current session: {modules[1:len(modules) - 1]}")
    print(f"\t   > Run-time: {time.time()-startTime} seconds")
    print(f"\t   > Analyzed: [{udfhandler.userUDFCounter}] UDFs")
    udfhandler.userUDFCounter = 0


'''
Exits the current {@Args session}.

Prints | If {@Args printDiagnostics} is set to True
    Prints the diagnostics of the current {@Args session} as described in _getDiagnostics(session).

Args
    session: The current Session.
    printDiagnostics: Optional boolean that determines whether or not diagnostics are printed. If
                      unset, this initializes to True automatically and prints the diagnostics as
                      descirbed in _getDiagnostics(session).
'''
def exitSession(session: Session, /, printDiagnostics = True):
    if printDiagnostics:
        _getDiagnostics(session)
    print(f"\n\t---------< FINISHED SESSION >---------\n")
    session.__exit__(1, 2, 3)