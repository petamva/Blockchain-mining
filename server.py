import socket
import time
import random

PORT = 9999
flag = 0  # this will serve later as the prefix of the payloads

with open('1661-0.txt', 'r') as f:
    strings = []
    for line in f:
        if line != '\n':  # get rid of empty lines
            strings.append(line[:-1])  # get rid of end of the line '\n' 

ssocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
ssocket.bind(('', PORT))
ssocket.listen()
print(f'Server ready: listening to port {PORT} for connections.\n')
(c, addr) = ssocket.accept()

while True:
    for string in strings:
        if flag:  # append the prefix 1, 2, 3... to each line for each iteration
            msg = f'{{"string" : "{flag}{string} "}}'
        else:  # except for the first time where flag is 0
            msg = f'{{"string" : "{string} "}}'
        print(msg)
        c.send((msg + '\n').encode())
        time.sleep(random.randint(2, 10))
    flag += 1  # after the file has been read, increment flag
