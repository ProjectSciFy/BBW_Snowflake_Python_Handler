# BBW_Snowflake_Python_Handler
Snowflake connector and Snowpark Python UDF handler.

+-----------------------------+
|Author: Gal Pinhasi          |
|Assisted By: Daniel Lubuguin |
|Sponsor: Bath and Body Works |
|Date Created: 7/26/2022      |
+-----------------------------+


+---------------------------------------------------------------------------------------------------------------------------------------+
|                                   General Information:                                                                                |
|                                                                                                                                       |
|  This README document has been created for the purpose of precise definitions and explanations                                        |
|  for environment setup, module setup, and module usage. All code belongs to Bath & Body Works.                                        |
|                                                                                                                                       |
|  The required version of Python necessary to run this code is 3.8 and above.                                                          |
|                                                                                                                                       |
|  There are two files associated with this project- snowpark_connector.py and udfhandler.py. Each                                      |
|  file contains the code necessary for the setup and usage of their respective modules. Note that                                      |
|  the snowpark_connector module and the udfhandler module were designed to be jointly used. This                                       |
|  code uses a private key connection, which means a system environment variable must be set. This                                      |
|  step is described and explained in the snowpark_connector module under setup requirements.                                           |
|                                                                                                                                       |
|  Furthermore, snowpark_connector.py makes use of udfhandler.py. This means that in order to                                           |
|  successfully use the snowpark_connector module, the user must also have set up the udfhandler                                        |
|  module. The reverse of this is not true.                                                                                             |
+---------------------------------------------------------------------------------------------------------------------------------------+


+---------------------------------------------------------------------------------------------------------------------------------------------------+
|                                   udfhandler module:                                                                                              |
+---------------------------------------------------------------------------------------------------------------------------------------------------+
|                                   Purpose:                                                                                                        |
|                                                                                                                                                   |
|  To shorten and simplify the code necessary to create and call a snowpark UDF inside python.                                                      |
+---------------------------------------------------------------------------------------------------------------------------------------------------+
|                                   Setup Requirements:                                                                                             |
|                                                                                                                                                   |
|> The udfhandler.py file must be located in the working directory. In the case that the                                                            |
|  user is working within an environment (such as Anaconda), the udfhandler.py file must be                                                         |
|  located in the host environment import library directory.                                                                                        |
|> Sys, pandas, cryptography, snowflake, snowflake-python-connector, and snowflake-snowpark-python                                                  |
|  must all be installed in the working directory or environment library. (Hint: pip install).                                                      |
|                                                                                                                                                   |
|NOTE: Local imports of modules are not supported. If an error is thrown due to path, use udfhandler.setPath("C:\\Path\\") .                        |
+---------------------------------------------------------------------------------------------------------------------------------------------------+
|                                   Description:                                                                                                    |
|                                                                                                                                                   |
|> Imports: pandas, Session from snowflake.snowpark, udf from snowflake.snowpark.functions                                                          |
|                                                                                                                                                   |
|> Globals:                                                                                                                                         |
|       path: String representation of the local path to the environment library containing all imports.                                            |
|       userUDFCounter: An integer counter to store the number of UDFs analyzed.                                                                    |
|                                                                                                                                                   |
|> Functions:                                                                                                                                       |
|setPath(newPath: str)                                                                                                                              |
|       Sets the {@Globals path} to the local library folder containing all imports {@Args newPath}.                                                |
|       Args                                                                                                                                        |
|           newPath: String representation of the local library folder containing all imports.                                                      |
|                                                                                                                                                   |
|getPath() -> str                                                                                                                                   |
|       Returns                                                                                                                                     |                                            
|           The string representation of the path to the library containing all imports.                                                            |
|                                                                                                                                                   |
|_list_packages(session: Session) -> list                                                                                                           |
|       Creates a List of all packages in the current Session.                                                                                      |
|       Returns                                                                                                                                     |
|           List containing all packages in the {@Args session}.                                                                                    |
|       Args                                                                                                                                        |
|           session: The current Session.                                                                                                           |
|                                                                                                                                                   |
|_list_imports(session: Session) -> list                                                                                                            |
|       Creates a List of all imports in the current Session.                                                                                       |
|       Returns                                                                                                                                     |
|           List containing all imports in the {@Args session}.                                                                                     |
|       Args                                                                                                                                        |
|           session: The current Session.                                                                                                           |
|                                                                                                                                                   |
|def findPath(session: Session, paths)                                                                                                              |
|       Sets the udfhandler's {@Globals path} to the path in {@Args paths} that allows for {@Args session} imports.                                 |
|                                                                                                                                                   |
|       Args                                                                                                                                        |
|           session: The current Session.                                                                                                           |
|           paths: The list of all paths in {@Code sys.path}.                                                                                       |
|                                                                                                                                                   |
|def analyzeUserUDF(session: Session, function, tableNameAndSuffix: str, /, selections = "", columns = "*", printResult = False) -> pd.DataFrame    |
|       Creates a Snowpark UDF from the {@Args function} and adds {@Globals allModules} to the {@Args session}.                                     |
|       Returns                                                                                                                                     |
|           A pandas.DataFrame containing the resulting execution.                                                                                  |
|       Args                                                                                                                                        |
|           session: The current Session.                                                                                                           |
|           function: The python function the user wishes to execute in Snowpark.                                                                   |
|           tableNameAndSuffix: A string containing the name of the table to execute the function on, as well as any                                |
|                               additional SQL suffixes.                                                                                            |
|       Optional Args (If set, must be explicitly)                                                                                                  |
|           selections: A string containing the selections, separated by commas, that the {@Returns dataframe} should                               |
|                       have as columns. If left empty, this {@Returns dataframe} will contain a single column with the                             |
|                       resulting execution.                                                                                                        |
|           columns: A string containing the column names, separated by commas, that the user's {@Args function} operates                           |
|                    on. Note this implies the number of elements in this string must be equal to the number of arguments                           |
|                    the user's {@Args function} has. If left empty, all columns from the {@Args tableNameAndSuffix} will                           |
|                    be selected, and must match the number of arguments in the user's {@Args function}.                                            |
|           printResult: A boolean that allows the user to pretty print the {@Returns dataframe}. If left empty, this is                            |
|                        set to False, and will not print.                                                                                          |
+---------------------------------------------------------------------------------------------------------------------------------------------------+
|                                   Usage:                                                                                                          |
|> import udfhandler                                                                                                                                |
|> import snowflake_connector                                                                                                                       |
|> create a snowflake.snowpark.Session as shown in the Usage portion of snowpark_connector                                                          |
|> udfhandler.analyzeUserUDF(session, function, "tableName", selections="columns, to, show", columns="pass, these, as, args", printResult=True)     |
|                                                                                                                                                   |
| NOTE: All UDFs create a temporary SQL stored procedure. The user may create a permanent, named stored procedure by setting the argument           |
|       is_permanent to True (is_permanent=True), as shown:                                                                                         |
|> user_udf = udf(wealth, name="user_udf", is_permanent=True, replace=True)                                                                         |
|       This can be found inside the "richestWithoutModule.py" file, on line 59.                                                                    |
+---------------------------------------------------------------------------------------------------------------------------------------------------+


+-------------------------------------------------------------------------------------------------------------------+
|                                   snowpark_connector module:                                                      |
+-------------------------------------------------------------------------------------------------------------------+
|                                   Purpose:                                                                        |
|                                                                                                                   |
|  To shorten and simplify the code necessary to connect and create a snowpark session.                             |
+-------------------------------------------------------------------------------------------------------------------+
|                                   Setup Requirements:                                                             |
|                                                                                                                   |
|> Must have udfhandler.py set up. This includes the setup requirements.                                            |
|> Must have an RSA key file.                                                                                       |
|> Must set up system environment variable "SNOWFLAKE_KEY_PASS" to be your snowflake password.                      |
|  On Windows, search "advanced system settings" in your Windows search box. Click on the                           |
|  "Advanced" tab. Click on "Environment Variables". Under "System Variables", click "New", and                     |
|  put "SNOWFLAKE_KEY_PASS" (without the apostrophes) as the variable name and your actual                          |
|  snowflake password as the variable value. Press "Okay". The variable is set up.                                  |
|> os, time, udfhandler, snowflake-snowpark-python, and cryptography must be installed in the                       |
|  working directory or environment library. (Hint: pip install).                                                   |                       
+-------------------------------------------------------------------------------------------------------------------+
|                                   Description:                                                                    |
|                                                                                                                   |
|> Imports: os, time, Session from snowflake.snowpark, udfhandler,                                                  |
|           default_backend from cryptography.hazmat.backends, serialization from cryptography.hazmat.primitives    |
|                                                                                                                   |
|> Globals:                                                                                                         |
|       startTime: Timer variable to store the time at which the code began execution.                              |
|                                                                                                                   |
|> Functions:                                                                                                       |
|getPrivateKey(pathToRSAKey: str) -> bytes                                                                          |
|       Gets the byte representation of the RSA key.                                                                |
|       Returns                                                                                                     |
|           The byte representation of the {@Args pathToRSAKey}.                                                    |
|       Args                                                                                                        |
|           pathToRSAKey: The string representation of the path to the RSA key.                                     |
|                                                                                                                   |
|connectionSetup(acc: str, user: str, prv_key: str, role: str, warehouse: str, database: str, schema: str) -> dict  |
|       Sets up and returns a snowflake.connector.connection.                                                       |
|       Returns                                                                                                     |
|           A snowflake.connector.connection with the {@Args}.                                                      |
|       Args                                                                                                        |
|           acc: String representation of the account to be used.                                                   |
|           user: String representation of the user to be connected.                                                |
|           prv_key: String representation of the key to be used. (Hint: this can be found via                      |
|                    snowpark_connector.getPrivateKey()).                                                           |
|           role: String representation of the role to be used.                                                     |
|           warehouse: String representation of the warehouse to be used.                                           |
|           database: String representation of the database to be used.                                             |
|           schema: String representation of the schema to be used.                                                 |
|                                                                                                                   |
|beginConnection(connection_params) -> Session                                                                      |
|       Begins a connection to snowpark via a Session.                                                              |
|       Returns                                                                                                     |
|           The Session that the is running on.                                                                     |
|       Args                                                                                                        |
|           connection_params: The snowflake.connector.connection necessary to connect to snowpark.                 |
|                              (Hint: this can be found via snowpark_connector.connectionSetup()).                  |
|                                                                                                                   |
|_getDiagnostics(session: Session)                                                                                  |
|       Prints out {@Prints} diagnostics regarding {@Args session}.                                                 |
|       Prints                                                                                                      |
|           Packages in the current {@Args session}.                                                                |
|           Imports in the current {@Args session}.                                                                 |
|           Immediate run-time of the current {@Args session}.                                                      |
|           Total number of UDFs analyzed in the current {@Args session}.                                           |
|       Args                                                                                                        |
|           session: The current Session.                                                                           |
|                                                                                                                   |
|exitSession(session: Session, /, printDiagnostics = True)                                                          |
|       Exits the current {@Args session}.                                                                          |
|       Prints | If {@Args printDiagnostics} is set to True                                                         |
|           Prints the diagnostics of the current {@Args session} as described in _getDiagnostics(session).         |
|       Args                                                                                                        |
|           session: The current Session.                                                                           |
|           printDiagnostics: Optional boolean that determines whether or not diagnostics are printed. If           |
|                             unset, this initializes to True automatically and prints the diagnostics as           |
|                             described in _getDiagnostics(session).                                                |
+-------------------------------------------------------------------------------------------------------------------+
|                                   Usage:                                                                          |
|> import snowpark_connector as spc                                                                                 |
|> import udfhandler                                                                                                |
|> keyPath = "C:\\PATH\\TO\\RSA\\KEY"                                                                               |
|> session = spc.beginConnection(spc.connectionSetup("account", "user", spc.getPrivateKey(keyPath), "role",         |
|                                                   "warehouse", "database", "schema"))                             |
|> Do things with udfhandler and session                                                                            |
|> spc.exitSession(session)                                                                                         |
+-------------------------------------------------------------------------------------------------------------------+
