import subprocess

def main():
    """
    This is the generator code. It should take in the MF structure and generate the code
    needed to run the query. That generated code should be saved to a 
    file (e.g. _generated.py) and then run.
    """

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
    # Asks user if they'd like to manually input Phi variables or read variables from a file
    file_check = input("Press 1 to read from a file, press 2 to input the arguments")

    if(int(file_check) == 2): # manual input
        S = input("Input Select Attribute (S): ") # cust, 1_sum_quant, 2_sum_quant, 3_sum_quant
        n = input("Input Number of Grouping Variables (n): ") # 3
        V = input("Input Grouping Attribute (V): ") # cust
        F = input("Input F-Vect ([F]): ") # 1_sum_quant, 1_avg_quant, 2_sum_quant, 3_sum_quant, 3_avg_quant
        sigma = input("Input Select Condition-Vect ([sigma]): ") # 1.state=’NY’, 2.state=’NJ’, 3.state=’CT’ /// must be equal to n
        G = input("Input Having Condition (G): ") # 1_sum_quant > 2 * 2_sum_quant or 1_avg_quant > 3_avg_quant /// correspond to F, S
    else: # read from file
        file_path = input("Please type in the file path from this directory: ")
        with open(file_path, "r") as file:
            file_info = file.read().split("\\n")

        # sets variables based on current line
        for idx, line in enumerate(file_info):
            if idx == 0:
                S = line.split(",")
            elif idx == 1:
                n = int(line)
            elif idx == 2:
                V = line.split(",")
            elif idx == 3:
                F = line.split(",")
            elif idx == 4:
                sigma = line.split(",")
            elif idx == 5:
                G = line.split(",")

        file.close()
        
    print(S)
    print(n)
    print(V)
    print(F)
    print(sigma)
    print(G)
    
    # Phi operator dictionary
    Phi = {
        "S": S, # string -> array
        "n": n, # string -> integer
        "V": V, # string -> array
        "F": F, # string -> array
        "sigma": sigma, #string -> array
        "G": G # string -> array
    }

    # table column names
    schema = ["cust", "prod", "day", "month", "year", "state", "quant", "date"]

    df = pd.DataFrame(data=cur, columns=schema) # creates dataframe

    # Algorithm

    emf = df[Phi["V"]].drop_duplicates() # gets rid of duplicate rows

    # converting dataframe to dictionary
    emf = [{V: row[i] for i, V in enum(Phi["V"])} for row in emf.values]

    # normal aggregates
    # iterate through f: if aggregate not group by aggregate, store in normal aggregate dictionary and compute
    # 0_avg_prod --> 0 = group by it belongs to (0 = normal), avg = aggregate, prod = column

    # emf algorithm
    # for sigma 
        # for row in df.iterrows()
            # for group by in emf
                # if eval(condition is true)
                    # update aggregate functions for current group by variable in F

    # main algorithm for EMF queries
    for predicate in Phi["sigma"]: # for each grouping variable predicate
        for row in df.iterrows(): # for each tuple/row in the table
            for gb_var in emf: # for each group by variable
                if eval(predicate): # checks if the condition is true for the row
                    # ex: predicate = 1.state="NY"
                    aggregate = predicate.split(".")[1]
                    col = aggregate.split("=")[0] # gets the affected column (state)
                    condition = predicate.split("=")[1] # gets whatever is after "="
                    index = schema.index(col) # gets the index for the value in the row
                    if (aggregate == "sum" | aggregate == "avg"):
                        gb_var += row[index]
                    if (aggregate == "min" | aggregate == "max"):
                        gb_var = row[index]
                    if (aggregate == "count"):
                        gb_var += 1

    # need to divide by length for average

    # Having
        # identical to algorithm BUT its only on the Phi
        # same if eval, all that

    for predicate in Phi["G"]:
        for row in df.iterrows():
           if eval(predicate): # checks if the condition is true for the row
                # ex: predicate = 1.state="NY"
                aggregate = predicate.split(".")[1]
                col = aggregate.split("=")[0] # gets the affected column (state)
                condition = predicate.split("=")[1] # gets whatever is after "="
                index = schema.index(col) # gets the index for the value in the row
                if (aggregate == "sum" | aggregate == "avg"):
                    gb_var += row[index]
                if (aggregate == "min" | aggregate == "max"):
                    gb_var = row[index]
                if (aggregate == "count"):
                    gb_var += 1

    # Select
        # go through rows of table, make sure it's in select table

    for column_name in df.columns[0]:
        if column_name not in S:
            df.drop(column_name, axis=1)

    _global = df"""

    # Note: The f allows formatting with variables.
    #       Also, note the indentation is preserved.
    tmp = f"""
import os
import psycopg2
import psycopg2.extras
import tabulate
import pandas as pd # added package
import enum # added package
from dotenv import load_dotenv

# DO NOT EDIT THIS FILE, IT IS GENERATED BY generator.py

def query():
    #load_dotenv()

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    conn = psycopg2.connect(host='localhost', dbname='postgres', user='postgres', password='password', port=5432)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
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