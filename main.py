import sys
from TransactionManager import TransactionManager


if __name__ == "__main__":

    fileName = sys.argv[1] if len(sys.argv)>=2 else None
    tm = TransactionManager()
    if fileName:
        try:
            with open(fileName, 'r') as f:
                for command in f:
                    tm.process_line(command)
        except IOError:
            print('Error while opening file {}'.format(fileName))
    else:
        print('filename not found for reading, reading input from command line. Enter exit to terminate')
        while True:
            command = input()
            # print(command)
            if command.strip() == 'exit':
                break
            tm.process_line(command)


            

