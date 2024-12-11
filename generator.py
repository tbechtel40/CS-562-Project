import subprocess
import struct
import pandas as pd

# Load .env variables, with error checking
def loadEnvVariables():
    load_dotenv() 
    user = os.getenv('USER') 
    password = os.getenv('PASSWORD') 
    dbname = os.getenv('DBNAME') 
    if not user or not password or not dbname: 
        raise ValueError("Missing required environment variables.") 
    return user, password, dbname

# Validate that the database connects properly for early failure detection
def validate_database_connection(user, password, dbname): 
    try: 
        conn = psycopg2.connect(f"dbname={dbname} user={user} password={password}") 
        conn.close() 
        print("Database connection successful.") 
    except Exception as e: 
        raise ValueError(f"Database connection failed: {e}")

def validSQLChar(c): 
    return ( ('0' <= c <= '9') or # Digits 0-9 
            ('A' <= c <= 'Z') or # Uppercase letters A-Z 
            ('a' <= c <= 'z') or # Lowercase letters a-z 
            c in " '()*," or # Whitespace, single quote, parentheses, asterisk, comma c in "=<>"
            

# Validation for phi
def validatePhi(S,n,V,F,sigma,G):
    S = S.strip()
    n = n.strip()
    V = V.strip()
    F = F.strip()
    sigma = sigma.strip()
    G = G.strip()

    # All parameters must have a value
    if not S or not n or not V or not F or not sigma or not G: 
        raise ValueError("All Phi operator arguments must be provided.")
    
    # n must be an integer
    if not n.is_integer():
        raise ValueError("N must be a valid integer")

    # check if n matches # of conditions in sigma 
    if int(n) != len(sigma.split(',')): 
        raise ValueError("Number of grouping variables (n) does not match the number of conditions in SELECT ([sigma]).")

    # Check if S has valid input (no spaces within var names)
    for (s in S):
        s = s.strip() # trim input
        for (sletter in s):
            if sletter == ' ':
                raise ValueError("S attribute variable name cannot contain spaces")
            if not validSQLCharacter(sletter):
                raise ValueError("S attribute variable name contains invalid SQL character")
    

    # do the same for V, F, and sigma
    
    # Check if V has valid input (no spaces within var names)
    for (v in V):
        v = v.strip() # trim input
        for (vletter in v):
            if vletter == ' ':
                raise ValueError("V attribute variable name cannot contain spaces")
            if not validSQLCharacter(vletter):
                raise ValueError("V attribute variable name contains invalid SQL character")
     
    # Check if F has valid input (no spaces within var names)
    for (f in F):
        f = f.strip() # trim input
        for (fletter in f):
            if fletter == ' ':
                raise ValueError("F attribute variable name cannot contain spaces")
            if not validSQLCharacter(fletter):
                raise ValueError("F attribute variable name contains invalid SQL character")

    # Check if sigma has valid input (no spaces within var names)
    for (sigmaAttr in sigma):
        sigmaAttr = sigmaAttr.strip() # trim input
        for (sigletter in sigmaAttr):
            if sigletter == ' ':
                raise ValueError("Sigma attribute variable name cannot contain spaces")
            if not validSQLCharacter(sigletter):
                raise ValueError("Sigma attribute variable name contains invalid SQL character")

def main():
    """
    This is the generator code. It should take in the MF structure and generate the code
    needed to run the query. That generated code should be saved to a 
    file (e.g. _generated.py) and then run.
    """

    file_check = input("Press 1 if you'd like to read the arguments for the Phi operator from a file\nPress 2 if you'd like to input the arguments for the Phi operator")

    # NOTE: Don't forget to do other file check for 1.

    if(file_check == 2):
        S = input("Input Select Attribute (S)") # cust, 1_sum_quant, 2_sum_quant, 3_sum_quant
        n = input("Input Number of Grouping Variables (n)") # 3
        V = input("Input Grouping Attribute (V)") # cust
        F = input("Input F-Vect ([F])") # 1_sum_quant, 1_avg_quant, 2_sum_quant, 3_sum_quant, 3_avg_quant
        sigma = input("Input Select Condition-Vect ([sigma])") # 1.state=’NY’, 2.state=’NJ’, 3.state=’CT’ /// must be equal to n
        G = input("Input Having Condition (G)") # 1_sum_quant > 2 * 2_sum_quant or 1_avg_quant > 3_avg_quant /// correspond to F, S
    else: # reading from file
        file_name = input("Please type in the file name")
        # read from file

    
    # Process input and put into dictionary
    user, password, dbname = loadEnvVariables()
    validateDatabaseConnection(user, password, dbname)
    
    # turn into arrays for most of them

    # Phi operator dictionary
    Phi = {
        "S": S,
        "n": n,
        "V": V,
        "F": F,
        "sigma": sigma,
        "G": G
    }

    # Example of how to call variable in dictionary: Phi["G"]

    schema = [("cust", "varchar(20)"), ("cust", "varchar(20)"), ("day", "integer"), ("month", "integer"), ("year", "integer"), ("state", "char(2)"), ("quant", "int"), ("date", "date")]

    print("struct {\n")

    """printf (“ %s %s[%d];\n”, V[0].type, V[0].attrib, V[0].size); # cust
    printf (“ %s %s[%d];\n”, V[1].type, V[1].attrib, V[1].size); # prod (if for 2nd g.v.)
    printf (“ %s %s;\n”, F_VECT[0].type, F_VECT[0].agg);
    printf (“ %s %s;\n”, F_VECT[1].type, F_VECT[1].agg);
    printf (“ %s %s;\n”, F_VECT[2].type, F_VECT[2].agg);
    printf (“} mf_struct[500];\n”); """

    mf_struct = []

    # Algorithm

        # emf = df[mf_struct["V"]].drop_duplicates()

        # converting dataframe to dictionary
        # emf = [{V: row[i] for i, V in enum(mf_struct["V"])} for row in emf.values]

        # normal aggregates
        # iterate through f: if aggregate not group by aggregate, store in normal aggregate dictionary and compute
        # 0_avg_prod --> 0 = groub by it belongs to (0 = normal), avg = aggregate, prod = column
        # use regex

        # emf algorithm
        # for sigma 
            # for row in df.iterrows()
                # for group by in emf
                    # if eval(condition is true)
                        # update aggregate functions for current group by variable in F

    # Having

        # identical to 5 BUT its only on the mf_struct
        # same if eval, all that

    # Select

        # go through rows of table, make sure it's in select table

    # 


    """
    for scan sc=0 to n {
        for each tuple t on scan {
           for all entries of H,
                check if the defining condition of grouping var
                Xsc is satisfied. If yes, update Xsc’s aggregates of the entry
                appropriately.
                X0 denotes the group (the defining condition of X0 is X0.S = S,
                where S denotes the grouping attributes.)
        }
    }

    for row in cur:
        #scan table
    """
    body = """
    
    """

    # Note: The f allows formatting with variables.
    #       Also, note the indentation is preserved.
    tmp = f"""
import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv

# DO NOT EDIT THIS FILE, IT IS GENERATED BY generator.py

def query():
    load_dotenv()

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
                            cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()
    cur.execute("SELECT * FROM sales")
    
    _global = []
    {body}
    
    return tabulate.tabulate(_global,
                        headers="keys", tablefmt="psql")

def main():
    print(query())
    
if "__main__" == __name__:
    main()
    """

    # Write the generated code to a file
    open("_generated.py", "w").write(tmp)
    # Execute the generated code
    subprocess.run(["python", "_generated.py"])


if "__main__" == __name__:
    main()
