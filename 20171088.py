# -*- coding: utf-8 -*-

import sqlparse
import csv
import sys 
import statistics
# Loop which calls parse, and whatever parse returns goes into execute(and update). Read database in every loop to
# ensure you are running on live database
class Tables: 
    def __init__(self,tablename,column_names):
        self.tablename = tablename
        self.column_names = []
        self.data = []
        
        for i in column_names:
            temp = [] 
            for j in i:
                if j != ';' :
                    temp.append(j)
             
            temp = ''.join(temp)
            self.column_names.append(temp)
    
    def read_table(self,filename):
        """Function to read table in csv format"""
        with open(filename, 'r') as csvFile:
            reader = csv.reader(csvFile)
            for row in reader:
                row_to_append = [] 
                for j in row:
                    if len(j.split("\"")) > 1 :
                        j = j.split("\"")[1]
                    row_to_append.append(j)
                    
                self.data.append(row_to_append)
        csvFile.close()
        
    def return_table_name(self):
        return self.tablename
    
    def return_table_columns(self):
        return self.column_names
    
    def return_table_data(self):
        return self.data
    
    def assign_data(self, data):
        self.data = data 
    
#    def read_metadata(self,lines_from_file) :
#        for i in range(2,len(lines_from_file)-1):
#            if i == 2:
#                self.tablename = lines_from_file[i]
#            else:
#                self.column_names.append(lines_from_file[i])


class Databases:
    def __init__(self):
        self.table_column_list = [] 
        self.table_name_list = [] 
     
    def get_tables(self,metadata_filename):
        meta_file = open(metadata_filename,"r")
        lines = meta_file.readlines()
        name_next_flag = False 
        single_table = [] 
        for i in range(len(lines)):
            if lines[i] == "<begin_table>\n":
                name_next_flag = True 
            elif lines[i] == "<end_table>\n" or lines[i] == "<end_table>" :
                self.table_column_list.append(single_table)
                single_table = [] 
            elif name_next_flag == True : 
                self.table_name_list.append(lines[i])
                name_next_flag = False 
            else : 
                single_table.append(lines[i])    
        
    def add_table(self, new_table, new_table_name):
        self.table_column_list.append(new_table)
        self.table_name_list.append(new_table_name)
        
    def return_table_names(self):
        return self.table_name_list 
    
    def return_table_columns(self):
        return self.table_column_list
        
        
def populate_tables(D):
    """ Get values of tables and add to a table_list. Identify all tables in database through database passed. """
    table_list = [] 
    for i in range(len(D.return_table_names())):
        T = Tables(D.return_table_names()[i][:-1], D.return_table_columns()[i])
        T.read_table(T.return_table_name()+'.csv')
        table_list.append(T)
        
    return table_list 
        
def get_tables_involved(str_tokens):
    """ Gets all tables from where data is taken in query parsed """
    tables_involved = [] 
    is_table = False
    for i in str_tokens:
        if i == 'where':
            is_table = False 
        
        if is_table == True : 
            tables_involved.append(i)
        
        if i == 'from':
            is_table = True 
            
    return tables_involved

def combine_two_tables(table1, table2):
    result_table = [] 
    for row1 in table1:
        for row2 in table2:
            temp = row1 + row2
            result_table.append(temp)
            
    return result_table

def join_tables(table_list,tables_involved):
    """ Cross product of tables involved """
    data_total = [] 
    column_name_list = [] 
    for i in tables_involved:
        involved = False 
        for j in table_list: 
            if j.return_table_name().lower() == i :
                data_total.append(j.return_table_data())
                involved = True 
                for k in j.return_table_columns():
                    temp = str(j.return_table_name() + '.' + k) 
                    column_name_list.append(temp)
        
        if involved == False : 
            print("Input table not in database")
            sys.exit()
                
    new_data = data_total[0] 
    for i in range(len(tables_involved)-1):
        new_data = combine_two_tables(new_data, data_total[i+1])
        
    T = Tables("JointTable",column_name_list)
    T.assign_data(new_data)
    return T
        
def check_distinct_column(a,joint_table):
    temp_list = [] 
    valid = True 
    new_a = a
    for i in joint_table.return_table_columns():
        temp = i.split('.')
        temp_list.append(temp)
    
    count = 0  
    for i in temp_list:
        if i[1].lower() == str(a+'\n').lower():
            count += 1 
            new_a = str(i[0] + '.' + i[1])
            
    if count != 1:
        valid = False 
        
    return valid, new_a
                
    
          
def single_condition(cond, joint_table):
    """ Work on joint_table to get required rows fitting given conditions """
    req_rows_list = [] 
    if len(cond.split('<=')) == 2:
        a = cond.split('<=')[0]
        b = cond.split('<=')[1]
        rel = '<='
    elif len(cond.split('>=')) == 2:
        a = cond.split('>=')[0]
        b = cond.split('>=')[1]
        rel = '>='
    elif len(cond.split('>')) == 2:
        a = cond.split('>')[0]
        b = cond.split('>')[1]
        rel = '>'    
    elif len(cond.split('<')) == 2:
        a = cond.split('<')[0]
        b = cond.split('<')[1]
        rel = '<'    
    elif len(cond.split('=')) == 2:
        a = cond.split('=')[0]
        b = cond.split('=')[1]
        rel = '=='
    else :
        print("Input Error")
        sys.exit()
        
            
    col_number_a = -1 
    col_number_b = -1 
    
    if a.isnumeric() == False : 
        for j in range(len(joint_table.return_table_columns())):
            if joint_table.return_table_columns()[j].split('\n')[0].lower() == a.lower():
                col_number_a = j 
        if col_number_a == -1 :  
            #check if distinct column, if it is, append tablex. and 
            valid, a = check_distinct_column(a,joint_table)
            if valid == False : 
                print("Condition syntax not valid")
                sys.exit()
            else : 
                for j in range(len(joint_table.return_table_columns())):
                    if joint_table.return_table_columns()[j].split('\n')[0].lower() == a.split('\n')[0].lower():
                        col_number_a = j 
                
                
    if b.isnumeric() == False : 
        for j in range(len(joint_table.return_table_columns())):           
            if joint_table.return_table_columns()[j].split('\n')[0].lower() == b.lower():
                col_number_b = j   
        if col_number_b == -1 :  
            #check if distinct column, if it is, append tablex. and 
            valid, b = check_distinct_column(b,joint_table)
            if valid == False : 
                print("Condition syntax not valid")
                sys.exit()
            else : 
                for j in range(len(joint_table.return_table_columns())):
                    if joint_table.return_table_columns()[j].split('\n')[0].lower() == b.split('\n')[0].lower():
                        col_number_b = j 
                        

    
    if col_number_a == -1 and col_number_b == -1 :   
        eval_type = 1 
    elif col_number_a == -1 and col_number_b != -1 :
        eval_type = 2
    elif col_number_a != -1 and col_number_b == -1 : 
        eval_type = 3
    else : 
        eval_type = 4 
          

    for row in joint_table.return_table_data():  
        if eval_type == 1 : 
            eval_cond = str(a) + rel + str(b)
        elif eval_type == 2 :
            eval_cond = str(a) + rel + str(row[col_number_b])
        elif eval_type == 3 : 
            eval_cond = str(row[col_number_a]) + rel + str(b) 
        else : 
            eval_cond = str(row[col_number_a]) + rel + str(row[col_number_b])   
        
        if eval(eval_cond):
            req_rows_list.append(row)
            
    return req_rows_list

def row_intersection(rows1, rows2):
    """ AND operation """
    all_rows = [] 
    for i in rows1:
        for j in rows2:
            if i == j : 
                all_rows.append(i) 
                
    for i in rows2:
        for j in rows1:
            if i == j :
                all_rows.append(i) 
                
    # All rows has repeated rows. req_rows will have all rows with no repeatation 
    req_rows = [] 
    for i in all_rows:
        already_in = False
        for j in req_rows:
            if i == j:
                already_in = True 
                
        if already_in == False : 
            req_rows.append(i)
            
    return req_rows

def row_union(rows1, rows2):
    """ OR operation """ 
    all_rows = [] 
    for i in rows1:
        all_rows.append(i)
    for i in rows2:
        already_exists = False
        for j in rows1:
            if i == j :
                already_exists = True 
                
        if already_exists == False :     
            all_rows.append(i)
    
    return all_rows
    
def get_required_rows(str_tokens, joint_table):
    """ Get required rows after filtering through all conditions """
    flag = False
    if str_tokens[1].lower() == 'distinct' : 
        if len(str_tokens) == 6:
            condition = str_tokens[5] 
        else :
            condition = False 
            req_rows = joint_table.return_table_data()
            flag = False
    else :
        if len(str_tokens) == 5:
            condition = str_tokens[4] 
        else: 
            condition = False
            req_rows = joint_table.return_table_data()
            flag = False
        
#    condition = ''.join(condition.split(' ')[1:])
    if condition:
        if len(condition.split(' and ')) == 2 or len(condition.split(' or ')) == 2:
            if len(condition.split(' and ')) == 2 :
                req_rows1 = single_condition(''.join(condition.split(' and ')[0].split(' ')[1:]),joint_table)
                req_rows2 = single_condition(''.join(condition.split(' and ')[1].split(' ')[0:]),joint_table)
                req_rows = row_intersection(req_rows1, req_rows2)
                flag = False
            if len(condition.split(' or ')) == 2 :
                req_rows1 = single_condition(''.join(condition.split(' or ')[0].split(' ')[1:]),joint_table)
                req_rows2 = single_condition(''.join(condition.split(' or ')[1].split(' ')[0:]),joint_table)
                req_rows = row_union(req_rows1, req_rows2)
                flag = False
            
        else : 
            req_rows = single_condition(''.join(condition.split(' ')[1:]), joint_table)
            # check if join 
            cond = ''.join(condition.split(' ')[1:])
            if len(cond.split('<=')) == 2:
                a = cond.split('<=')[0]
                b = cond.split('<=')[1]
                rel = '<='
            elif len(cond.split('>=')) == 2:
                a = cond.split('>=')[0]
                b = cond.split('>=')[1]
                rel = '>='
            elif len(cond.split('>')) == 2:
                a = cond.split('>')[0]
                b = cond.split('>')[1]
                rel = '>'    
            elif len(cond.split('<')) == 2:
                a = cond.split('<')[0]
                b = cond.split('<')[1]
                rel = '<'    
            elif len(cond.split('=')) == 2:
                a = cond.split('=')[0]
                b = cond.split('=')[1]
                rel = '=='
            else :
                print("Input Error")
                sys.exit()
                
            if a.isnumeric() == False and b.isnumeric() == False and rel == '==':
                flag = True 
            
    return req_rows, flag


def get_required_columns(str_tokens, req_rows, joint_table, flag):
    """ Project only the required columns of the rows given based on select statement """
    if str_tokens[1] == 'distinct' : 
        req_col_names = str_tokens[2]
    else :
        req_col_names = str_tokens[1]
 
    req_col_names = req_col_names.split(',')   
    final_output = [] 
    col_nums_output = []
    operation = [] 
    
    if '*' in req_col_names:
        if len(req_col_names) > 1 :
            print("Can't  project any other columns along with wildcard")
            sys.exit()
        else:
            req_col_names = []
            for i in range(len(joint_table.return_table_columns())):
                req_col_names.append(joint_table.return_table_columns()[i].split('\n')[0].lower())
    
    if flag == True : 
        # remove second column that is being projected and in join
        if str_tokens[1].lower() == 'distinct' : 
            if len(str_tokens) == 6:
                condition = str_tokens[5] 
        else :
            if len(str_tokens) == 5:
                condition = str_tokens[4] 
                
        cond = ''.join(condition.split(' ')[1:])
        if len(cond.split('<=')) == 2:
#            a = cond.split('<=')[0]
            b = cond.split('<=')[1]
        elif len(cond.split('>=')) == 2:
#            a = cond.split('>=')[0]
            b = cond.split('>=')[1]
        elif len(cond.split('>')) == 2:
#            a = cond.split('>')[0]
            b = cond.split('>')[1]
        elif len(cond.split('<')) == 2:
#            a = cond.split('<')[0]
            b = cond.split('<')[1]
        elif len(cond.split('=')) == 2:
#            a = cond.split('=')[0]
            b = cond.split('=')[1]
        else :
            print("Input Error")
            sys.exit()
        
#        col_number_a = -1 
        col_number_b = -1 
        col_name_b = None 
        
#        for j in range(len(joint_table.return_table_columns())):
#            if joint_table.return_table_columns()[j].split('\n')[0].lower() == a.lower():
#                col_number_a = j 
#        if col_number_a == -1 :  
#            #check if distinct column, if it is, append tablex. and 
#            valid, a = check_distinct_column(a,joint_table)
#            if valid == False : 
#                print("Condition syntax not valid")
#                sys.exit()
#            else : 
#                for j in range(len(joint_table.return_table_columns())):
#                    if joint_table.return_table_columns()[j].split('\n')[0].lower() == a.split('\n')[0].lower():
#                        col_number_a = j 
#                
                    
        for j in range(len(joint_table.return_table_columns())):           
            if joint_table.return_table_columns()[j].split('\n')[0].lower() == b.lower():
                col_number_b = j   
                col_name_b = joint_table.return_table_columns()[j].split('\n')[0].lower()
        if col_number_b == -1 :  
            #check if distinct column, if it is, append tablex. and 
            valid, b = check_distinct_column(b,joint_table)
            if valid == False : 
                print("Condition syntax not valid")
                sys.exit()
            else : 
                for j in range(len(joint_table.return_table_columns())):
                    if joint_table.return_table_columns()[j].split('\n')[0].lower() == b.split('\n')[0].lower():
                        col_number_b = j 
                        col_name_b = joint_table.return_table_columns()[j].split('\n')[0].lower()
        
        # remove name of col b from req col names 
#        if col_name_b != None:
#            try:
#                req_col_names.remove(col_name_b)
#            except:
#                pass
#            try:
#                col_name_b = col_name_b.split('.')[1]
#        
    for col_name in req_col_names:
        temp_operation = 0 
        if len(col_name.split('(')) == 2 : 
            temp_operation = col_name.split('(')[0].strip()
            col_name = col_name.split('(')[1][:-1]
        
        # else colname remains the same
        added = False 
        for i in range(len(joint_table.return_table_columns())):
            if flag == True and col_name_b.strip() == col_name.lower().strip() and temp_operation == 0:
                added = True 
                pass
            else:
                if col_name.lower().strip() == joint_table.return_table_columns()[i].split('\n')[0].lower().strip() :
                    col_nums_output.append([col_name.lower().strip(),i])
                    if temp_operation != 0:
                        operation.append([col_name.lower().strip(), temp_operation])
                    added = True 
        
        if added == False :     
            valid, col_name = check_distinct_column(col_name.strip(),joint_table) 
            if valid == False : 
                print("Projection column name invalid\n")
                sys.exit()
                
            for i in range(len(joint_table.return_table_columns())):
                if flag == True and col_name_b.strip() == col_name.split('\n')[0].lower().strip() and temp_operation == 0:
                      pass
                else:
                    if col_name.split('\n')[0].lower().strip() == joint_table.return_table_columns()[i].split('\n')[0].lower().strip() :
                        col_nums_output.append([col_name.split('\n')[0].strip(),i])
                        if temp_operation != 0:
                            operation.append([col_name.split('\n')[0].strip(), temp_operation])
                
    req_rows_temp = list(zip(*req_rows))
    for i in col_nums_output:
        final_output.append(req_rows_temp[i[1]])
    
    final_output = list(zip(*final_output))
    for i in range(len(final_output)):
        final_output[i] = list(final_output[i])
        
    if str_tokens[1] == 'distinct' : 
        temp_final_output = final_output
        final_output = [] 
        for i in temp_final_output:
            if i not in final_output:
                final_output.append(i)
        
    if not operation:
        column_names_final = [ i[0] for i in col_nums_output ]
        return column_names_final, final_output
        
    operated_final_output = [] 
    ofor = [] 
    operation_index_pair = []
    for i in operation:
        for j in col_nums_output:
            if i[0] == j[0]:
                operation_index_pair.append([i[1],j[1],i[0]])
    
    if len(req_col_names) != len(operation_index_pair):
        print("Output dimensions don't agree")
        sys.exit()
    
    column_names_final = [] 
    for i in operation_index_pair:
        column_names_final.append(str(i[0]+'('+i[2])+')')
    
    print(operation_index_pair)
    for i in operation_index_pair:
        if i[0] == 'max':
            if len(req_rows) != 0:    
                ofor.append(max([int(row[i[1]]) for row in req_rows]))
        elif i[0] == 'min':
            if len(req_rows) != 0:  
                ofor.append(min([int(row[i[1]]) for row in req_rows]))
        elif i[0] == 'sum':
            if len(req_rows) != 0:  
                ofor.append(sum([int(row[i[1]]) for row in req_rows]))
        elif i[0] == 'mean':
            if len(req_rows) != 0:  
                ofor.append(statistics.mean([int(row[i[1]]) for row in req_rows]))
        else : 
            print("Aggregator invalid")
            sys.exit()
            
    operated_final_output.append(ofor)
    
    return column_names_final,operated_final_output
    # Agg functions being executed on columns 
    # if *, all cols
    # if max, min, sum, avg, only take whats within for next step and process seperately at last step 
    #check for each req_col_name if the column name associated with tablename is there. Then check if distinct col name is there
    #(ie without associated tablename directly).

def print_output(attr, rows):
    final_list = ''
    for i in range(len(attr)):
        final_list += str(attr[i])
        if i != len(attr) - 1 : 
            final_list += ','
    final_list += '\n'
    
    for row in rows :
        for i in row : 
            final_list += str(i) + ','
        final_list = final_list[:-1]
        final_list += '\n'
    
    print(final_list)
        
    
def check_input_validity(str_tokens):
        
    if str_tokens[0] != 'select':
        print("Query not valid")
        sys.exit()
        
    if len(str_tokens) < 4:
        print("Query not valid")
        sys.exit()
    
    if str_tokens[1] == 'distinct':
        if len(str_tokens) < 5:
            print("Query not valid")
            sys.exit()
        if str_tokens[3] != 'from':
            print("Query not valid")
            sys.exit()
        if len(str_tokens) > 5  and len(str_tokens) < 6:
            print("Query not valid")
            sys.exit()
        if len(str_tokens) > 5 and str_tokens[5].split(' ')[0] != 'where':
            print('Query not valid')
            sys.exit()
    else :
        if str_tokens[2] != 'from':
            print("Query not valid")
            sys.exit()
        if len(str_tokens) > 4  and len(str_tokens) < 5:
            print("Query not valid")
            sys.exit()
        if len(str_tokens) > 4 and str_tokens[4].split(' ')[0] != 'where':
            print('Query not valid')
            sys.exit()

# Main funtion 

D = Databases() 
D.get_tables('metadatatest.txt')   

table_list = populate_tables(D)   
#sql_stat = input()
sql_stat = str(sys.argv[1])
number_of_statements = len(sql_stat.split(';'))
if number_of_statements == 0 : 
    print("Error : No input given")
    sys.exit()
for i in range(number_of_statements):
    if len(sql_stat.split(';')[i]) > 0 : 
        stmt = sqlparse.parse(sql_stat.split(';')[i])
        toks = stmt[0]
        toks = toks.tokens
        
        token_list = sqlparse.sql.IdentifierList(toks).get_identifiers()
        str_tokens = []
        for token in token_list:
            t = token.value.lower()
            if t[-1] == ';':
                t = t[:-1]
            str_tokens.append(t)
        
        check_input_validity(str_tokens)
        
        tables_involved = [x.strip() for x in get_tables_involved(str_tokens)[0].split(',')]
        
        joint_table = join_tables(table_list,tables_involved)
        
        req_rows, flag = get_required_rows(str_tokens, joint_table)
        zero_len = False
        if len(req_rows) == 0 :
            req_rows =  [ [ '-' for i in row ]  for row in joint_table.return_table_data() ]
            zero_len = True 
            
        col_names, req_cols = get_required_columns(str_tokens, req_rows, joint_table, flag)
        
        if zero_len == True :
            req_cols = [[]] 
            
        print_output(col_names,req_cols)


#Select * from table1,table2 where table1.A = 100 OR table2.D > 10000;