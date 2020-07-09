import errno
import mimetypes
import os
import signal
import socket
import sys
from datetime import datetime
from os import path

# Check Command Line args
if len(sys.argv) > 1:
    try:
        isinstance(int(sys.argv[1]), int)
    except ValueError as z:
        print("Please provide a valid port number.")
        os._exit(0)
    finally:
        SERVER_ADDRESS = (HOST, PORT) = '', int(sys.argv[1])
else:
    print("Please provide a valid port number.")
    os._exit(0)
REQUEST_QUEUE_SIZE = 5
MAX_PACKET = 131072


def end_service(signum, frame):
    """
    Ends and removes child process after client disconnects.
    :param signum: Signal Number.
    :param frame: Stack Frame.
    :return: Empty.
    """
    while True:
        try:
            pid, status = os.waitpid(
                -1,  # Wait for any child process
                os.WNOHANG  # Do not block and return EWOULDBLOCK error
            )
        except OSError:
            return

        if pid == 0:
            return


def split_lines(request):
    """
    Split request string with newline characters for later processing.
    :param request: Received request from client.
    :return: Request string object broken up by newline characters.
    """
    try:
        return ''.join((line + '\n') for line in request.splitlines())
    except Exception as g:
        print(g)
        return err_response(500)


def recv_all(sock):
    """
    Loop recv() function until complete communication is received.
    :param sock: Socket connection.
    :return: Data received from connection.
    """
    try:
        data = bytearray()
        while True:
            part = sock.recv(MAX_PACKET)
            data += part
            if len(part) < MAX_PACKET:
                # either 0 or end of data
                break
        return data

    except Exception as h:
        print(h)
        return err_response(500)


def generate_response_message(response_code):
    """
    Retrieve proper error response message.
    :param response_code: Integer representation of HTTP error code.
    :return: Error response message.
    """
    return {
        400: "<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\">\n<html>\n<head>\n    <title>400 Bad "
             "Request</title>\n</head>\n<body>\n    <h1>Bad Request</h1>\n   <p>Your browser sent a request that this "
             "server could not understand.</p>\n   <p>The request line contained invalid characters following the "
             "protocol string.</p>\n</body>\n</html>",
        404: "<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\">\n<html>\n<head>\n    <title>404 Fot "
             "Found</title>\n</head>\n<body>\n    <h1>Not Found</h1>\n   <p>The requested URL was not found on this "
             "server.</p>\n</body>\n</html>",
        500: "<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\">\n<html>\n<head>\n    <title>500 Internal Server "
             "Error</title>\n</head>\n<body>\n    <h1>Internal Server Error</h1>\n   <p>Sorry, something went "
             "wrong.</p>\n</body>\n</html>",
        501: "<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\">\n<html>\n<head>\n    <title>501 Not "
             "Implemented</title>\n</head>\n<body>\n    <h1>Not Implemented</h1>\n   <p>Server does not support the "
             "functionality required to fulfill the request.</p>\n</body>\n</html>"
    }.get(response_code)


def err_response(response_code):
    """
    Constructs complete error response.
    :param response_code: Integer representation of HTTP error code.
    :return: Complete Error Response.
    """
    try:
        response = generate_response_message(response_code)
        status = (response[(response.find("<title>") + 7):response.find("</title>")])
        http_header = "HTTP/1.1 " + status + "\r\n"
        utc_datetime = datetime.utcnow()
        http_header += "Date: " + utc_datetime.strftime('%a, %d %b %Y %H:%M:%S GMT') + "\r\n"
        http_header += "Content-Type: text/html\r\n"
        http_header += "Content-Length: " + str(len(response)) + "\r\n"
        http_header += "Connection: Close"
        http_header += "\r\n\r\n"
        http_body = response
        http_body += "\r\n\r\n"
        return http_header.encode(), http_body.encode()

    except Exception as a:
        print(a)
        return err_response(500)


def form_response(contents, file_size, mtime, mime_type):
    """
    Constructs complete OK response given type of content requested.
    :param contents: Contents to send to client.
    :param file_size: Size of content requested.
    :param mtime: Time content was last modified.
    :param mime_type: Type of content.
    :return: Complete OK response.
    """
    try:
        # Form Header
        http_header = "HTTP/1.1 200 OK\r\n"
        utc_datetime = datetime.utcnow()
        http_header += "Date: " + utc_datetime.strftime('%a, %d %b %Y %H:%M:%S GMT') + "\r\n"
        http_header += "Last-Modified: " + datetime.fromtimestamp(mtime).strftime(
            '%a, %d %b %Y %H:%M:%S GMT') + "\r\n"
        http_header += "Content-Type: " + str(mime_type) + "\r\n"
        http_header += "Content-Length: " + file_size + "\r\n"
        http_header += "Connection: Close"
        http_header += "\r\n\r\n"
        http_body = contents
        return http_header.encode(), http_body

    except Exception as b:
        print(b)
        return err_response(500)


def find_resource(file_path):
    """
    Get requested resource from web_root.
    :param file_path: Path to resource in web_root.
    :return: Call to form_response: Complete OK response.
    """
    try:
        # If the file is binary
        if file_path.find(".jpg") > -1 or file_path.find(".jpeg") > -1 or file_path.find(".png") > -1:
            f = open(file_path, "rb")
            contents = bytearray(f.read())
            f.close()
            contents += b'\r\n'
            return form_response(contents, str(len(contents)), os.path.getmtime(file_path),
                                 mimetypes.MimeTypes().guess_type(file_path)[0])

        # If file is text
        else:
            f = open(file_path, "r")
            contents = f.read()
            f.close()
            contents += "\r\n"
            return form_response(contents.encode(), str(len(contents)), os.path.getmtime(file_path),
                                 mimetypes.MimeTypes().guess_type(file_path)[0])

    except Exception as c:
        print(c)
        return err_response(500)


def handle_file_extension(file_path):
    """
    Extract file extension if available or add general path to index file.
    :param file_path: Path to resource in web_root.
    :return: Formatted path.
    """
    try:
        # Split path and extension
        root, ext = os.path.splitext(file_path)

        # If extension is not present, add appropriate path to general index file
        if not ext:
            if not root.endswith('/'):
                root += '/'
            ext = 'index.html'
        return root + ext

    except Exception as d:
        print(d)
        return err_response(500)


def handle_get(lines):
    """
    Check get request and form appropriate response.
    :param lines: Get Request.
    :return: Appropriate response for condition.
    """
    try:
        file_path = "web_root"

        http_index = lines.find("HTTP")
        if http_index == -1 or lines.find("/../") != -1:
            return err_response(400)

        file_path += (lines[4:http_index - 1])
        if file_path == "web_root" or file_path.endswith('.'):
            return err_response(400)

        file_path = handle_file_extension(file_path)

        if path.exists(file_path):
            return find_resource(file_path)
        else:
            return err_response(404)

    except Exception as e:
        print(e)
        return err_response(500)


def handle(lines):
    """
    Handle received request from client.
    :param lines: Request from client.
    :return: Appropriate response for condition.
    """
    try:
        if "GET" in lines:
            return handle_get(lines)
        else:
            return err_response(501)
    except Exception as f:
        print(f)
        return err_response(500)


def serve():
    """
    Listens for client connections until stopped.
    """

    # Set up socket and listen for connections on specified port
    try:
        listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listen_socket.bind(SERVER_ADDRESS)
        listen_socket.listen(REQUEST_QUEUE_SIZE)
    except PermissionError as x:
        print(x)
        os._exit(0)
    finally:
        print('Serving HTTP on port {port} ...'.format(port=PORT))

    # Set up async handler
    signal.signal(signal.SIGCHLD, end_service)

    # Core Functionality
    # Accept connections and break off child process to serve individual connections
    while True:
        try:
            client_connection, client_address = listen_socket.accept()
        except IOError as x:
            code, msg = x.args
            # restart 'accept' if it was interrupted
            if code == errno.EINTR:
                continue
            else:
                raise

        pid = os.fork()
        if pid == 0:  # child
            listen_socket.close()  # close child copy
            request = recv_all(client_connection)
            lines = split_lines(request.decode())
            print("\nReceived Request...\nRequest:\n" + lines)
            http_header, http_body = handle(lines)
            print("Outgoing Response Header:\n" + http_header.decode())
            client_connection.sendall(http_header)
            client_connection.sendall(http_body)
            client_connection.close()
            os._exit(0)
        else:  # parent
            client_connection.close()  # close parent copy and loop over


if __name__ == '__main__':
    serve()
