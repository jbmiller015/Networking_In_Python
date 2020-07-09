import os
import sys
import socket


class Client:
    """
    Client forms and sends get and put requests to a Distributed Hash Table.
    """

    def __init__(self):
        self.SERVER_ADDRESS = serv_add
        self.VERB = verb
        self.KEY = key
        self.VALUE = value
        self.MAX_PACKET = 65507
        my_hostname = socket.gethostname()
        my_ip = socket.gethostbyname(my_hostname)
        self.CLIENT_ADD = (HOST, PORT) = my_ip, 11190
        Client.action(self)

    def action(self):
        """
        Creates socket and sends request to node.
        :return:
        """

        # Create socket and bind to default port
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client_socket.bind(self.CLIENT_ADD)

        # Set timeout
        client_socket.settimeout(3.0)

        # Construct request message
        message = self.form_request()

        # Send request
        client_socket.sendto(message.encode(), self.SERVER_ADDRESS)

        # Listen for response
        try:
            data, server = client_socket.recvfrom(self.MAX_PACKET)
            data = self.split_lines(data.decode())
            print(f'{data}')
        except socket.timeout:
            print('REQUEST TIMED OUT')

    def form_request(self):
        """
        Construct Request message from given command line arguments
        :return:
        """
        request = "Destination: " + self.SERVER_ADDRESS[0] + " " + str(self.SERVER_ADDRESS[1]) + "\r\n"
        request += "Nodes Visited: 0\r\n"
        if self.VALUE is None:
            request += "Data: " + self.VERB + " " + self.KEY
        else:
            request += "Data: " + self.VERB + " " + self.KEY + " " + self.VALUE
        return request

    @staticmethod
    def split_lines(response):
        """
        Split request string with newline characters for later processing.
        :param response: Received request from client.
        :return: Request string object broken up by newline characters.
        """
        try:
            return ''.join((line + '\n') for line in response.splitlines())
        except Exception as g:
            print(g)


if __name__ == '__main__':
    if len(sys.argv) >= 5:
        try:
            NODE = sys.argv[1]
            NODE_PORT = sys.argv[2]
            verb = sys.argv[3]
            key = sys.argv[4]
            if len(sys.argv) == 6:
                value = sys.argv[5]
            else:
                value = None
        except ValueError as z:
            print("Please provide a valid line number.")
            os._exit(0)
        finally:
            serv_add = (HOST, PORT) = NODE, int(NODE_PORT)
    else:
        print("Please provide a valid file and/or line number.")
        os._exit(0)


    Client()
