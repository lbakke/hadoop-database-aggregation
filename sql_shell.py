#!/usr/bin/env python3

import os
import subprocess
import sys

columns = ['Event', 'White', 'Black', 'Result', 'UTCDate', 'UTCTime', 'WhiteElo', 'BlackElo', 'WhiteRatingDiff', 'BlackRatingDiff', 'ECO', 'Opening', 'TimeControl', 'Termination', 'AN']
columns = [x.lower() for x in columns]
EXIT_FAILURE = 1

def parse_command(cmd):
    words = cmd.split(' ')
    if len(words) < 1: 
        print('Error parsing command: command must contain "SELECT"')
        return EXIT_FAILURE
    if words[-1][-1] == ';':        # if last word has semicolon at end (like regular SQL), drop it
        words[-1] = words[-1][:-1]

    ops = {'sum': False, 'count': False, 'min': False, 'max': False, 'avg': False, 'stdev': False}
    my_columns = set()

    ''' SELECT '''
    if words[0].lower() != 'select':
        print('Error parsing command: command must begin with "SELECT" keyword.')
        return EXIT_FAILURE

    word_index = 1
    select_set = set()
    while (word_index < len(words)):
        if words[word_index][-1] != ',':    # keep grabbing select keywords until no , 
            break
        select_set.add(words[word_index][:-1])
        word_index += 1
    if word_index >= len(words): 
        print('Error parsing command: command missing field after SELECT.')
        return EXIT_FAILURE
    select_set.add(words[word_index])
    word_index += 1

    ''' APPROVE SELECT FIELDS '''
    for item in select_set: 
        if 'sum' in item.lower(): 
            ops['sum'] = True
        elif 'count' in item.lower(): 
            ops['count'] = True
        elif 'min' in item.lower(): 
            ops['min'] = True
        elif 'max' in item.lower(): 
            ops['max'] = True
        elif 'avg' in item.lower(): 
            ops['avg'] = True
        elif 'stdev' in item.lower(): 
            ops['stdev'] = True
        else:
            item = item.lower()
            if '(' in item or ')' in item:
                print(f'Invalid operation "{item}". Operations are: SUM, COUNT, MIN, MAX, AVG and STDEV.')
                return EXIT_FAILURE
            elif item not in columns: 
                print(f'Invalid column "{item}"". Column options are: {columns}.')
                return EXIT_FAILURE
            my_columns.add(item)

    if word_index >= len(words): 
        if len(my_columns) != len(select_set):     # if len(cols) != len(selected words), must be some operations, which need "GROUP BY"
            print('Cannot have operations without GROUP BY to indicate grouping.')
            return EXIT_FAILURE
        else:
            return my_columns, ops, None, None
    
    ''' GROUP BY '''
    if word_index < len(words): 
        next1 = words[word_index]
        word_index += 1
    else: 
        print('Syntax error: Missing GROUP BY command.')
        return EXIT_FAILURE
    if word_index < len(words): 
        next2 = words[word_index]
        word_index += 1
    else: 
        print('Syntax error: Missing GROUP BY command.')
        return EXIT_FAILURE

    if next1.lower() != 'group' or next2.lower() != 'by': 
        print('SELECT command must be followed by GROUP BY in our parser.')
        return EXIT_FAILURE

    ''' APPROVE GROUP BY FIELD '''
    if word_index >= len(words): 
        print(f'GROUP BY must be followed by valid column name. Column options are: {columns}.')
        return EXIT_FAILURE
    groupby_field = words[word_index].lower()
    word_index += 1
    if groupby_field not in columns: 
        print(f'Invalid GROUP BY column {groupby_field}. Column options are: {columns}.')
        return EXIT_FAILURE

    orderby_field = 0
    if word_index < len(words): 
        if words[word_index].lower == 'desc': 
            orderby_field = 1
        word_index += 1

    if word_index < len(words):
        print(f'Unknown words at end of SQL command. Ignoring after {groupby_field}.')

    ''' RETURN PARAMETERS '''
    return my_columns, ops, groupby_field, orderby_field

def verify_input_data(columns, opers, groupby): 
    if groupby == None:        # if no aggregation/group by, must be ok
        return True 
    else: 
        if columns != None and len(columns) > 0: 
            if len(columns) > 1: 
                return False        # if grouping, only allowed to use one column 
            if columns[0] != groupby: 
                return False        # if grouping, column must be equal to groupby field 
        return True

def call_map_reduce(syspath, columns, opers, groupby):

    ''' CALL M/R WITH PARAMETERS '''
    for key in opers: 
        if opers[key]: 
            print(f'calling this M/R: {key}, group by {groupby}')
            # f = open("sql_shell.py", "r")
            # text = f.read()
            # f.close()
            # map_result = subprocess.run(["./testmap.py"], stdout=subprocess.PIPE, text=True, input=text)
            # reduce_result = subprocess.run(["./testreduce.py"], stdout=subprocess.PIPE, text=True, input=map_result.stdout)

            runstr = 'hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -files countMap.py,countReduce.py -input ' + syspath + '/smaller_file -output ' + syspath + '/temp2 -mapper "countMap.py 1" -reducer "countReduce.py 0 20 0"'
            subprocess.run([runstr], stdout=subprocess.PIPE)

            ''' PRINT RESULTS '''
            print('printing results')
            # print(reduce_result.stdout)
            outstr = 'hadoop fs -cat ' + syspath + '/temp2/part-00000'
            subprocess.run([outstr])

            cleardirstr = 'hadoop fs -rm -r ' + syspath + '/temp2'
            subprocess.run([cleardirstr])

if __name__ == '__main__': 

    if len(sys.argv) != 2: 
        print('To run the SQL command interface shell, run "./sql_shell [path]" with the path of where hadoop can access on disc01 (eg. /users/lbakke)')

    syspath = sys.argv[1]

    help_str = '''Welcome to our SQL command interface. Type your command as so to begin:

    SELECT [col_name, sum(*), count(*), min(*), max(*), avg(*), stdev(*)], * 
    [GROUP BY] [col_name];

    eg. SELECT event, count(*), min(*) GROUP BY event;

Recall in SQL any columns selected with aggregation commands must match the GROUP BY field.

Type 'exit' to quit or 'help' to hear the instructions again.
    '''

    print(help_str)
    command = input('SQL > ')
    while ('exit' not in command): 
        if ('help' in command): 
            print(help_str)
        else: 
            result = parse_command(command)
            if result != EXIT_FAILURE: 
                cols, ops, group_by = list(result[0]), result[1], result[2]
                if verify_input_data(cols, ops, group_by):
                    call_map_reduce(syspath, cols, ops, group_by)
                else: 
                    print(f'Invalid column selection, any columns displayed must match group by field "{group_by}".')
        command = input('SQL > ')




''' order by -> only two options are the group by column, or the count/etc column 


things to add: LIMIT, ORDER BY 

ORDER BY: 0 -> nothing or column, 1 -> count
LIMIT: -1 -> no limit, 0-? -> limit # 
ORDER BY: 0 -> asc or nothing   , 1 -> desc 

'''