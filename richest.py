import snowpark_connector
import udfhandler
import pandas

'''CONNECTION'''
keyPath = "C:\\PATH\\TO\\RSA\\KEY"
test_session = snowpark_connector.beginConnection(
    snowpark_connector.connectionSetup("account",
                    "user",
                    snowpark_connector.getPrivateKey(keyPath),
                    "role",
                    "warehouse",
                    "database",
                    "schema"
    )
)

'''USER'S FUNCTION'''
def wealth(billCount: int, billType: str) -> int:
    import random
    temp = pandas.DataFrame([1,2])
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

'''HANDLER CALL'''
def findWealthiest() -> tuple:
    df = udfhandler.analyzeUserUDF(test_session, wealth, "bills", selections = "name, bills, type", columns = "bills, type", printResult=True)
    id = df["USER_UDF(BILLS, TYPE)"].idxmax()
    person = df.loc[id].at["NAME"]
    money = df.loc[id].at["USER_UDF(BILLS, TYPE)"]
    return (person, money)

richest = findWealthiest()
print(f"\nWealthiest person is {richest[0]} with ${richest[1]}.")

'''EXIT SESSION'''
snowpark_connector.exitSession(test_session)