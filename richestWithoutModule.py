import os
import pandas as pd

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from snowflake.snowpark import Session
from snowflake.snowpark.functions import udf

'''CONNECTION'''
with open("C:\\PATH\\TO\\RSA\\KEY", "rb") as key:
        p_key= serialization.load_pem_private_key(
            key.read(),
            password=os.environ['SNOWFLAKE_KEY_PASS'].encode(),
            backend=default_backend())

pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption())

connection_params = {
    "user": "user",
    "private_key": pkb, 
    "account": "account",
    "warehouse": "warehouse",
    "database": "database", 
    "role": "role",
    "schema": "schema"
}

'''SESSION'''
test_session = Session.builder.configs(connection_params).create()

'''DEPENDENCIES'''
test_session.add_import("C:\\PATH\\TO\\LOCAL\\ENV\\LIBRARY\\random.py")
test_session.add_packages(["pandas"])

'''USER'S FUNCTION'''
def wealth(billCount: int, billType: str) -> int:
    import random
    temp = pd.DataFrame([1, 2])
    money = 0
    if billType == "ones":
        money += billCount
    elif billType == "fives":
        money += 5 * billCount
    elif billType == "tens":
        money += 10 * billCount
    elif billType == "twenties":
        money += 20 * billCount
    elif billType == "fifties":
        money += 50 * billCount
    elif billType == "hundreds":
        money += 100 * billCount
    return money

'''UDF'''
user_udf = udf(wealth, name="user_udf", replace=True)
returnedRows = test_session.sql("select name, bills, type, USER_UDF(BILLS, TYPE) from BILLS")
dataframe = pd.DataFrame(returnedRows.toPandas())
returnedRows.show()

'''ANALYZE UDF'''
def findWealthiest() -> tuple:
    df = dataframe
    id = df["USER_UDF(BILLS, TYPE)"].idxmax()
    person = df.loc[id].at["NAME"]
    money = df.loc[id].at["USER_UDF(BILLS, TYPE)"]
    return (person, money)

richest = findWealthiest()
print(f"\nWealthiest person is {richest[0]} with ${richest[1]}.")

'''EXIT SESSION'''
test_session.__exit__(1, 2, 3)