#!/usr/bin/env python3

columns = ['Event', 'White', 'Black', 'Result', 'UTCDate', 'UTCTime', 'WhiteElo', 'BlackElo', 'WhiteRatingDiff', 'BlackRatingDiff', 'ECO', 'Opening', 'TimeControl', 'Termination', 'AN']
columns = [x.lower() for x in columns]

def parse_command(cmd):
    words = cmd.split(' ')
    if words[0].lower() != 'select':
        print('Error parsing command: command must begin with "SELECT" keyword.')
        return

    word_index = 1
    select_set = set()
    while (words[word_index][-1] == ','):
        select_set.add(words[word_index][:-1])
        word_index += 1
    select_set.add(words[word_index])
    word_index += 1

    ops = {'sum': False, 'count': False, 'min': False, 'max': False, 'avg': False, 'stdev': False}

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
                return 
            elif item not in columns: 
                print(f'Invalid column "{item}"". Column options are: {columns}.')
                return

    

if __name__ == '__main__': 

    help_str = '''Welcome to our SQL command interface. Type your command as so to begin:

    SELECT [col_name] [col_name] ... [sum(*), count(*), min(*), max(*), avg(*), stdev(*)]
    [GROUP BY] [col_name]

    Type 'exit' to quit or 'help' to hear the instructions again.
    '''

    print(help_str)
    command = input('SQL > ')
    while ('exit' not in command): 
        if ('help' in command): 
            print(help_str)
        else: 
            parse_command(command)
        command = input('SQL > ')