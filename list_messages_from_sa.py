from MF4Reader import MF4Reader
from pathlib import Path
import argparse


aparser = argparse.ArgumentParser(description='List all messages from a given source.')
aparser.add_argument('-dbc', nargs='?', help='Folder with dbc files')
aparser.add_argument('-l', '--logs', nargs='?', help='Folder with MF4 log files')
args = aparser.parse_args()

if args.logs is None:
    args.logs = input('Log files folder: ')
args.logs = Path(args.logs)

if args.dbc is None:
    args.dbc = input('DBC folder: ')
args.dbc = Path(args.dbc)


mReader = MF4Reader(args.logs, args.dbc)

while True:
    sa = input('SA: ')
    if len(sa) == 0:
        break

    if sa.startswith('0x') or sa.startswith('0X'):
        sa = int(sa, 16)
    else:
        sa = int(sa)
    msgs = mReader.get_messages_from_source(sa)
    for x in msgs:
        print(x.name)
    print('\n')
