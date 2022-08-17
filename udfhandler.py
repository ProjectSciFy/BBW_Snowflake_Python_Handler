import sys
import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.functions import udf

'''
Globals
    path: String representation of the local path to the environment library
          containing all imports.
    userUDFCounter: An integer counter to store the number of UDFs analyzed.
'''
path = "C:/"
userUDFCounter = 0


'''
Sets the {@Globals path} to the local library folder containing all imports.

Args
    newPath: String representation of the local library folder containing all imports.
'''
def setPath(newPath: str):
    global path
    path = newPath


'''
Returns
    The string representation of the path to the library containing all imports.
'''
def getPath() -> str:
    return path


'''
Creates a List of all packages in the current Session.

Returns
    List containing all packages in the {@Args session}.

Args
    session: The current Session.
'''
def _list_packages(session: Session) -> list:
    try:
        packages = list()
        for package in session.get_packages():
            packages.append(package)
        return packages
    except:
        print("<EXCEPTION>  Invalid session.  Function 'list_packages(session)' was passed an invalid session as a parameter.\n")


'''
Creates a List of all imports in the current Session.

Returns
    List containing all imports in the {@Args session}.

Args
    session: The current Session.
'''
def _list_imports(session: Session) -> list:
    try:
        imports = list()
        for imprt in session.get_imports():
            # Append just the name of the import, not the path
            imports.append(imprt[len(getPath()):])
        return imports
    except:
        print("<EXCEPTION>  Invalid session.  Function 'list_imports(session)' was passed an invalid session as a parameter.\n") 


'''
Sets the udfhandler's {@Globals path} to the path in {@Args paths} that allows for {@Args session} imports.

Args
    session: The current Session.
    paths: The list of all paths in {@Code sys.path}.
'''
def findPath(session: Session, paths):
    foundPath = False
    if len(getPath()) <= 3:
        for path in paths:
            if not foundPath:
                try:
                    session.add_import(path + "/random.py")
                    session.remove_import(path + "/random.py")
                    setPath(path + "/")
                    foundPath = True
                except:
                    continue
            else:
                break
                


'''
Creates a Snowpark UDF from the {@Args function} and adds {@Globals allModules} to the {@Args session}.

Returns
    A pandas.DataFrame containing the resulting execution.

Args
    session: The current Session.
    function: The python function the user wishes to execute in Snowpark.
    tableNameAndSuffix: A string containing the name of the table to execute the function on, as well as any
                        additional SQL suffixes.

    selections: A string containing the selections, separated by commas, that the {@Returns dataframe} should 
                have as columns. If left empty, this {@Returns dataframe} will contain a single column with the 
                resulting execution.
    columns: A string containing the column names, separated by commas, that the user's {@Args function} operates 
             on. Note this implies the number of elements in this string must be equal to the number of arguments
             the user's {@Args function} has. If left empty, all columns from the {@Args tableNameAndSuffix} will 
             be selected, and must match the number of arguments in the user's {@Args function}.
    printResult: A boolean that allows the user to pretty print the {@Returns dataframe}. If left empty, this is
                 set to False, and will not print.
'''
def analyzeUserUDF(session: Session, function, tableNameAndSuffix: str, /, selections = "", columns = "*", printResult = False) -> pd.DataFrame:
    # Iterate through each module imported or used by {@Args function}- 
    # try to add_import it, then try to add_packages it, and if that doesn't work continue past it.
            # USEFUL CODE, LONG RUNTIME:
            # for name in function.__code__.co_names:
            #     for path in sys.path:
            #         try:
            #             session.add_import(path + "\\" + name + ".py")
            #             setPath(path)
            #         except:
            #             try:
            #                 session.add_packages(name)
            #             except:
            #                 continue
    findPath(session, sys.path)
    for name in function.__code__.co_names:
        try:
            session.add_import(getPath() + name + ".py")
        except:
            try:
                session.add_packages(name)
            except:
                continue
    # Create user UDF from {@Args function}
    global userUDFCounter
    userUDFCounter += 1
    user_udf = udf(function, name="user_udf", replace=True)
    # Parse {@Args selections} with comma separators if nonempty
    select = selections
    if select != "":
        select += ", "
    # Parse SQL call necessary for execution
    returnedRows = session.sql("select " + select + "user_udf(" + columns + ") from " + tableNameAndSuffix)
    ret = returnedRows.toPandas()
    if printResult:
        returnedRows.show()
    return pd.DataFrame(ret)