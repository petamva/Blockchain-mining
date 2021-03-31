import socket
import time
import random

PORT = 9999
flag = 0  # this will serve later as the prefix of the payloads

ssocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
ssocket.bind(('', PORT))
ssocket.listen()
print(f'Server ready: listening to port {PORT} for connections.\n')
(c, addr) = ssocket.accept()

while True:
    with open('1661-0.txt', 'r') as f:
        for line in f:
            if line != '\n':
                if flag:  # append the prefix 1, 2, 3... to each line for each iteration
                    msg = f'{{"string" : "{flag}{line[:-1]} "}}'
                else:  # except for the first time where flag is 0
                    msg = f'{{"string" : "{line[:-1]} "}}'
                print(msg)
                c.send((msg + '\n').encode())
                time.sleep(random.randint(2, 5))
    flag += 1  # after the file has been read, increment flag
