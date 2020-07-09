import hashlib
import os
import socket
import sys


class Table:
    """
    Table holds the local Hash Table and performs all related functions
    as well as handles connections, requests, and responses.
    """
    MAX_PACKET = 65507
    KV_STORE = {}
    lines = {}

    def __init__(self):
        self.n = Node()
        self.LINE_COUNT = line_count
        self.SERVER_ADDRESS = server_add
        Table.serve(self)

    def serve(self):
        """
        Listens for client connections until stopped.
        """

        # Set up socket
        try:
            node_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            node_socket.bind(self.SERVER_ADDRESS)
        except PermissionError as x:
            print(x)
            os._exit(0)
        finally:
            print('Serving on port {port} ...'.format(port=PORT))

        # Core Functionality
        # Accept connections and serve
        while True:
            request = node_socket.recvfrom(self.MAX_PACKET)
            self.split_lines(request)
            message = self.handle()
            host, port = self.lines.get('Destination').split()
            addr = (host, int(port))
            node_socket.sendto(message, addr)
            print("Sent\n")

    def split_lines(self, req):
        """
        Split request string with newline characters for later processing.
        :param req:
        :param request: Received request from client.
        :return: Request string object broken up by newline characters.
        """
        request = req[0].decode()
        self.lines = {"Source": req[1][0] + " " + str(req[1][1])}
        print("\nReceived Request: ")
        for line in request.splitlines():
            print(line)
            key, val = line.split(': ')
            self.lines[key] = val

    def handle(self):
        """
        Handle received request from client.
        :return: Appropriate response for condition.
        """
        try:
            # Return error if resource is not identified after visiting every node
            if int(self.lines.get("Nodes Visited")) == line_count:
                return self.err_response(404)

            # If get request
            if "get" in self.lines.get("Data"):
                return self.handle_get()

            # If put request
            elif "put" in self.lines.get("Data"):
                return self.handle_put()

            # If request is something other than get or put
            else:
                return self.err_response(501)

        except Exception as f:
            print(f)
            return self.err_response(500)

    def handle_get(self):
        """
        Check get request and form appropriate response.
        :return: Appropriate response for condition.
        """

        try:
            # Split get and key from data value
            verb, key = self.lines.get("Data").split(" ", 1)

            # Find location and hash value of key
            location, key_hash = self.n.key_loc(key)

            # If key is supposed to be stored locally
            if location is None:

                # Find key in Table and return success response if found
                if key in self.KV_STORE:
                    return self.gen_response(key, self.KV_STORE.get(key), verb, key_hash)

                # If not found in local table
                else:
                    return self.err_response(404)

            # If key is supposed to be stored in a different node
            else:
                return self.go_next(location)

        except Exception as e:
            print(e)
            return self.err_response(500)

    def handle_put(self):
        """
        Check put request and form appropriate response.
        :return: Appropriate response for condition.
        """

        try:
            # Split put and key/value from data value
            verb, key_val = self.lines.get("Data").split(" ", 1)
            key_val = key_val.strip()

            # If placing/replacing value
            if ' ' in key_val:
                key, val = key_val.split()

            # If "removing" key from local table
            else:
                key = key_val
                val = None

            # Find location and hash value of key
            location, key_hash = self.n.key_loc(key)

            # If key is supposed to be stored locally
            if location is None:

                # Find key in Table and return success response if found
                self.KV_STORE[key] = val
                return self.gen_response(key, val, verb, key_hash)

            # If key is supposed to be stored in a different node
            else:
                return self.go_next(location)
        except ValueError as e:
            print(e)
            return self.err_response(400)
        except Exception as e:
            print(e)
            return self.err_response(500)

    def gen_response(self, key, val, verb, key_hash):
        """
        Constructs complete Success Response.
        :param key: String representation of key in hashtable.
        :param val: String representation of value in hashtable.
        :param verb: Action to be taken by hashtable (get or put).
        :param key_hash: String representation of hashed key.
        :return: Complete Success Response.
        """
        try:

            # If key's value has been "removed" return not found
            if verb == "get" and val is None:
                return self.err_response(404)

            # Build response
            message = "Success!\r\n"
            message += "Source: " + server_add[0] + " " + str(server_add[1]) + "\r\n"
            message += "Node Hash: " + str(self.n.SUCC_ID) + "\r\n"
            message += "Destination: " + self.lines.get("Source") + "\r\n"
            message += "Nodes Visited: " + str((int(self.lines.get("Nodes Visited")) + 1)) + "\r\n"
            if val is None:
                message += "Key: " + key + "\nValue: <EMPTY>\nKey Hash: " + str(key_hash)
            else:
                message += "Key: " + key + "\nValue: " + val + "\nKey Hash: " + str(key_hash)
            message += "\r\n\r\n"

            # Set class-level dictionary 'lines' with return information
            self.lines["Destination"] = self.lines.get("Source")
            self.lines["Source"] = server_add[0] + " " + str(server_add[1])
            print("Sending Success Response to: " + self.lines["Destination"])

            return message.encode()

        except Exception as b:
            print(b)
            return self.err_response(500)

    def err_response(self, response_code):
        """
        Constructs complete error response.
        :param response_code: Integer representation of HTTP error code.
        :return: Complete Error Response.
        """

        try:
            # Retrieve appropriate error statement
            response = self.generate_response_message(response_code)

            # Build response
            message = "Error\r\n"
            message += "Source: " + server_add[0] + " " + str(server_add[1]) + "\r\n"
            message += "Node Hash: " + str(self.n.SUCC_ID) + "\r\n"
            message += "Destination: " + self.lines.get("Source") + "\r\n"
            message += "Nodes Visited: " + str(int(self.lines.get("Nodes Visited")) + 1) + "\r\n"
            message += "Error Message: " + response
            message += "\r\n\r\n"

            # Set class-level dictionary 'lines' lines with return information
            self.lines["Destination"] = self.lines.get("Source")
            self.lines["Source"] = server_add[0] + " " + str(server_add[1])
            print("Sending Error Response: " + response)

            return message.encode()

        except Exception as a:
            print(a)
            return self.err_response(500)

    @staticmethod
    def generate_response_message(response_code):
        """
        Retrieve proper error response message.
        :param response_code: Integer representation of HTTP error code.
        :return: Error response message.
        """
        return {
            400: "Bad Request - The request line contained invalid characters following the protocol string.",
            404: "Not Found - The requested resource was not found.",
            500: "Internal Server - Sorry, something went wrong.",
            501: "Not Implemented - Server does not support the functionality required to fulfill the request."
        }.get(response_code)

    def go_next(self, location):
        """
        Find next node in path and construct request to send to that node.
        :param location: Hashed value of next node in path.
        :return: Request for next node.
        """

        # Get string value of hashed node id
        destination = self.n.get_loc_key(location)
        print("Forwarding to Node: " + str(location))

        # Build request
        request = "Source: " + self.lines.get("Source") + "\r\n"
        request += "Destination: " + destination.strip('\n') + "\r\n"
        request += "Nodes Visited: " + str((int(self.lines.get("Nodes Visited")) + 1)) + "\r\n"
        request += "Data: " + self.lines.get("Data")

        # Set class-level dictionary 'lines' with return information
        self.lines["Destination"] = destination

        return request.encode()


class Node:
    """
    Node implements the the various local components of A Scalable Peer-to-peer Lookup Service (Chord)
    """
    MAX_ENT = 160
    finger_table = {}
    NODE_ID = 0
    SUCC_ID = 0
    PRED_ID = 0
    nodes_key = {}

    def __init__(self):
        self.SERVER_LINES = server_lines
        self.LINE_COUNT = line_count
        self.NODES = Node.discover(self)
        print("Discovering...")
        print("Building Table...")
        Node.build_table(self)

    def discover(self):
        """
        Sets Successor and Predecessor Node ID.
        :return:
        """

        # Create list of hashed nodes
        hashed_nodes = self.gen_hash()

        # Loop through sorted list and assign appropriate members
        for i in range(len(hashed_nodes)):
            if self.NODE_ID == hashed_nodes[i]:
                try:
                    self.SUCC_ID = hashed_nodes[i + 1]
                except IndexError:
                    self.SUCC_ID = hashed_nodes[0]
                try:
                    self.PRED_ID = hashed_nodes[i - 1]
                except IndexError:
                    self.PRED_ID = hashed_nodes[len(hashed_nodes) - 1]
        return hashed_nodes

    def gen_hash(self):
        """
        Construct list of sorted, hashed nodes.
        :return: List of sorted, hashed nodes.
        """

        line_num = 0
        hashed_nodes = [None] * self.LINE_COUNT

        # Loop through given list of Hostnames and Port numbers
        for line in self.SERVER_LINES:

            # Determine host and port from each line
            line.strip('\n')
            host, port = line.split()

            # Get IPv4 representation of host
            ip = socket.gethostbyname(host)

            # Convert port to int then to bytes
            port_int = int(port)
            port_bytes = port_int.to_bytes(2, byteorder='big')

            # Convert IPv4 to bytes then append port bytes
            node_id = socket.inet_pton(socket.AF_INET, ip)
            node_id += port_bytes

            # Hash byte representation
            hashed_val = int(hashlib.sha1(node_id).hexdigest(), 16)

            # Store in local array
            hashed_nodes[line_num] = hashed_val

            # Store hashed node as key and original host and port strings as value in class-level dictionary
            self.nodes_key[hashed_val] = line

            # Set Current Node ID based off command line argument
            if line_num == int(sys.argv[2]):
                self.NODE_ID = hashed_nodes[line_num]
            line_num += 1

        # Sort list
        hashed_nodes.sort()

        return hashed_nodes

    def build_table(self):
        """
        Build finger table from sorted list of hashed nodes
        """

        # Set modulo value
        mod_val = pow(2, self.MAX_ENT)

        # Number of rows in table = k where 1 < k < m
        if self.LINE_COUNT < self.MAX_ENT:
            k = self.LINE_COUNT
        else:
            k = self.MAX_ENT

        # Create finger table entries
        # Entries are represented by ~ log2(Given Host List) nodes
        # Visual Representation of table: <index> \\ <N+2^i mod 2^m> // <N+2^i+1 mod 2^m> // <Successor of range>
        for i in range(k):
            x = ((self.NODE_ID + pow(2, i)) % mod_val)
            y = ((self.NODE_ID + pow(2, i + 1)) % mod_val)
            successor_node = self.set_successor(x)
            self.finger_table[i] = {"start": x, "end": y, "successor": successor_node}

    def set_successor(self, x):
        """
        Determine successor of finger.
        :param x: Finger.
        :return: Finger's Successor
        """

        # Set default to Node's Successor
        successor = self.SUCC_ID

        # If finger is between current node and its successor
        if self.check_successor(x):
            return self.SUCC_ID

        # If finger is not between current node and successor, find next closest node
        else:
            for node in self.NODES:
                if (node ^ x) < (successor ^ x) and x < node:
                    successor = node
            return successor

    def closest_successor(self, x):
        """
        Search finger table for closest successor node to key.
        :param x: Key.
        :return: Closest successor to key's location.
        """

        # Set default to Node's Successor
        successor = self.SUCC_ID

        # Loop through finger table and compare successors
        for i in range(len(self.finger_table)):
            node = self.finger_table.get(i)
            finger = node.get("start")
            if (finger ^ x) < (successor ^ x) and x < node.get("successor"):
                successor = node.get("successor")

        return successor

    def check_successor(self, x):
        """
        Compare the location of x to the range between the current node and its successor.
        :param x: Key or finger.
        :return: True if x is in range, false if not.
        """
        if self.NODE_ID > self.SUCC_ID and (
                x in range(self.NODE_ID, pow(2, self.MAX_ENT)) or x in range(0, self.SUCC_ID)):
            return True
        elif self.SUCC_ID > self.NODE_ID and x in range(self.NODE_ID, self.SUCC_ID):
            return True
        else:
            return False

    def check_self(self, x):
        """
        Compare the location of x to the range between the current node and its predecessor.
        :param x: Key.
        :return: True if x is in range, false if not.
        """
        if (self.NODE_ID > self.PRED_ID) and x in range(self.PRED_ID, self.NODE_ID):
            return True
        if (self.PRED_ID > self.NODE_ID) and (
                x in range(self.PRED_ID, pow(2, self.MAX_ENT)) or x in range(0, self.NODE_ID)):
            return True
        else:
            return False

    def check_table(self, x):
        """
        Search table for successor.
        :param x: Key.
        :return: Location of successor if found.
        """
        for i in range(len(self.finger_table)):
            finger = self.finger_table.get(i)
            start = finger.get("start")
            end = finger.get("end")
            if start > end and (x in range(start, pow(2, self.MAX_ENT)) or x in range(0, end)):
                return finger.get("successor")
            elif end > start and x in range(start, end):
                return finger.get("successor")
        return None

    def key_loc(self, key):
        """
        Determine the location of given key.
        :param key: String representation of key.
        :return: Location of key's successor and key's hashed representation.
        """

        # Hash key
        x = int(hashlib.sha1(key.encode()).hexdigest(), 16)

        # If only one node in system or the key can be found locally
        if len(self.finger_table) == 1 or self.check_self(x):
            return None, x

        # If key's successor is current node's successor
        if self.check_successor(x):
            return self.SUCC_ID, x

        # Search Table
        else:
            loc = self.check_table(x)
            if loc is None:
                loc = self.closest_successor(x)
        return loc, x

    def get_loc_key(self, location):
        """
        Returns associated Hostname and Port number of given Node ID
        :param location: Node ID
        :return: String representation of Hostname and Port number
        """
        return self.nodes_key.get(location)


if __name__ == '__main__':

    # Check Command Line args
    if len(sys.argv) > 1:
        try:
            FILE = open(sys.argv[1], "r")
            isinstance(int(sys.argv[2]), int)
        except IOError as y:
            print("Please provide a valid host file.")
            os._exit(0)
        except ValueError as z:
            print("Please provide a valid line number.")
            os._exit(0)
        finally:
            server_lines = FILE.readlines()
            line_count = len(server_lines)
            if line_count < int(sys.argv[2]) + 1:
                print("Please provide a valid line number.")
                os._exit(0)
            host_name, port_num = server_lines[int(sys.argv[2])].split()
            server_add = (HOST, PORT) = host_name, int(port_num)
            FILE.close()

            Table()
    else:
        print("Please provide a valid file and/or line number.")
        os._exit(0)
