import sys
from TransactionManager import TransactionManager


if __name__ == "__main__":

    fileName = sys.argv[1] if len(sys.argv)>=2 else None
    tm = TransactionManager()
    if fileName:
        try:
            print('\nfile: \n',fileName)
            with open(fileName, 'r') as f:
                for command in f:
                    tm.processLine(command)
            print('\n')
        except IOError:
            print('Error while opening file {}'.format(fileName))
    else:
        print('Filename not found for reading')
        print('reading input from command line')
        print('Enter exit to terminate')
        while True:
            command = input()
            # print(command)
            if command.strip() == 'exit':
                break
            tm.processLine(command)


            

