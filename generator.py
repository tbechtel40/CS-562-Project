import subprocess

def main():
    """
    This is the generator code. It should take in the MF structure and generate the code
    needed to run the query. That generated code should be saved to a 
    file (e.g. _generated.py) and then run.
    """

    body = """
    # Asks user if they'd like to manually input Phi variables or read variables from a file
    file_check = input("Press 1 to read from a file, press 2 to input the arguments: ")

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
            file_info = file.read().split("\n")

        # sets variables based on current line
        for idx, line in enumerate(file_info):
            if idx == 0:
                S = line.split(",")
                for i, l in enumerate(S):
                    S[i] = l.strip()
            elif idx == 1:
                n = int(line)
            elif idx == 2:
                V = line.split(",")
                for i, l in enumerate(V):
                    V[i] = l.strip()
            elif idx == 3:
                F = line.split(",")
                for i, l in enumerate(F):
                    F[i] = l.strip()
            elif idx == 4:
                sigma = line.split(",")
                for i, l in enumerate(sigma):
                    sigma[i] = l.strip()
            elif idx == 5:
                G = line.split(",")
                for i, l in enumerate(G):
                    G[i] = l.strip()

        file.close()
    
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

    emf = df[Phi["V"]].drop_duplicates() # gets rid of duplicate rows

    # converting dataframe to dictionary
    emf = [{V: row[i] for i, V in enumerate(Phi["V"])} for row in emf.values]

    # Dictionary for aggregates

    aggDict = {}
    for i in range(n):
        aggDict[i+1] = {}

    for aggregate in Phi["F"]:
        gb_num = int(aggregate[0]) # gets the group by number
        agg = aggregate.split("_")[1] # gets the aggregate function
        col = aggregate.split("_")[2] # gets the affected column (i.e. quant)
        aggDict[gb_num].update({agg: col})

    # emf algorithm
    # for sigma 
        # for row in df.iterrows()
            # for group by in emf
                # if eval(condition is true)
                    # update aggregate functions for current group by variable in F

    # main algorithm for EMF queries
    for predicate in Phi["sigma"]: # for each grouping variable predicate
        gb_num = int(predicate[0])
        for row in df.iterrows(): # for each tuple/row in the table
            for gb_var in emf: # for each group by variable
                col = predicate.split(".")[1].split(" ")[0]
                operator = predicate.split(".")[1].split(" ")[1]
                value = predicate.split(".")[1].split(" ")[2]
                cond1 = row[1][col]
                if operator == "=":
                    operator = "=="
                if eval(f'cond1 {operator} value'): # checks if the condition is true for the row
                    # ex: predicate = 1.state="NY"
                    updateAggregates = aggDict[gb_num]
                    for aggregate in updateAggregates:
                        ind = f"{gb_num}_{aggregate}_{aggDict[gb_num][aggregate]}"
                        for gb_att in V:
                            if emf[emf.index(gb_var)][gb_att] == row[1][gb_att]:
                                if (aggregate == "sum" or aggregate == "avg"):
                                    try:
                                        avg_count = f"{ind}_count"
                                        emf[emf.index(gb_var)][ind] += row[1][aggDict[gb_num][aggregate]]
                                        emf[emf.index(gb_var)][avg_count] += 1
                                    except:
                                        emf[emf.index(gb_var)][ind] = row[1][aggDict[gb_num][aggregate]]
                                        emf[emf.index(gb_var)][avg_count] = 1
                                if (aggregate == "min" or aggregate == "max"):
                                    try:
                                        if (aggregate == "min" and emf[emf.index(gb_var)][ind] > row[1][aggDict[gb_num][aggregate]]):
                                            emf[emf.index(gb_var)][ind] = row[1][aggDict[gb_num][aggregate]]
                                        elif (aggregate == "max" and emf[emf.index(gb_var)][ind] < row[1][aggDict[gb_num][aggregate]]):
                                            emf[emf.index(gb_var)][ind] = row[1][aggDict[gb_num][aggregate]]
                                    except:
                                        emf[emf.index(gb_var)][ind] = row[1][aggDict[gb_num][aggregate]]
                                if (aggregate == "count"):
                                    try:
                                        emf[emf.index(gb_var)][ind] += 1
                                    except:
                                        emf[emf.index(gb_var)][ind] = 1
        for gb_var in emf:
            aggregate = "avg"
            try:
                ind = f"{gb_num}_{aggregate}_{aggDict[gb_num][aggregate]}"
                emf[emf.index(gb_var)][ind] = emf[emf.index(gb_var)][ind] / emf[emf.index(gb_var)][avg_count]
            except:
                pass

    # Select
        # go through rows of table, make sure it's in select table

    for column_name in df.columns:
        if column_name not in S:
            df = df.drop(column_name, axis=1)
            df = df.drop_duplicates()

    for col in S:
        if col not in df.columns:
            data = []
            for item in emf:
                data.append(item.get(col))
            df[col] = data

    # Having

    for predicate in Phi["G"]:
        for gb_var in emf:
            eval_string = ""
            pred_values = predicate.split(" ")
            for item in pred_values:
                if item == "=":
                    eval_string += "== "
                elif "_" in item:
                    eval_string += str(gb_var[item]) + " "
                else:
                    eval_string += str(item) + " "
            if not eval(eval_string):
                attribute_list = []
                for gb_att in V:
                    attribute_list.append(gb_var[gb_att])
                de_delete = pd.DataFrame(tuple(attribute_list), columns=V)
                df_filter = df.merge(de_delete, on=V, how="left", indicator=True)
                df = df_filter[df_filter["_merge"] == "left_only"].drop(columns=["_merge"])

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