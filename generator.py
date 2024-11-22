import subprocess


def main():
    """
    This is the generator code. It should take in the MF structure and generate the code
    needed to run the query. That generated code should be saved to a 
    file (e.g. _generated.py) and then run.
    """

    file_check = input("Press 1 if you'd like to read the arguments for the Phi operator from a file\nPress 2 if you'd like to input the arguments for the Phi operator")

    # NOTE: Don't forget to do other file check for 1.

    if(file_check == 2):
        S = input("Select Attribute (S)")
        n = input("Number of Grouping Variables (n)")
        V = input("Grouping Attributes (V)")
        F = input("F-Vect ([F])")
        sigma = input("Select Condition-Vect ([sigma])")
        G = input("Having Condition (G)")
    else: # reading from file
        file_name = input("Please type in the file name")

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

    """
    for scan sc=0 to n {
        for each tuple t on scan {
            for the entries of H with matching grouping attributes (for MF queries)
                check if the defining condition of grouping var
                Xsc is satisfied. If yes, update Xsc’s aggregates of the entry
                appropriately.
                X0 denotes the group (the defining condition of X0 is X0.S = S,
                where S denotes the grouping attributes.)
        }
    }
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
