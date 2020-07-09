#include <cstring>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <iostream>
#include <unistd.h>
#include <stdio.h>
#include <fstream>
#include <algorithm>
#include <regex>

using namespace std;

#define PORTNO "80"
#define MAXCHAR 131072

/**
 * Takes in a commandline-provided url then verifies whether it is a valid url.
 * If the provided url is valid, the name of the server, the file path, and the port number are extracted (if found).
 * Port numbers will default to global 'PORTNO' if none is found.
 * File Path will default to '/' if none is found.
 * @param url Commandline-provided url
 * @param serverName Name of server
 * @param filepath File path to associated resources
 * @param portNum Port Number
 */
void formatUrl(char *url, string &serverName, string &filepath, string &portNum) {

    string::size_type i, j, k;
    string fUrl = url;
    regex url_regex (R"(([--:\w?@%&+~#=]*\.[a-z]{2,4}\/{0,2})((?:[?&](?:\w+)=(?:\w+))+|[--:\w?@%&+~#=]+)?)");

    //Validate URL
    if (!regex_match(url,  url_regex)) {
        fprintf(stderr,"Error: Malformed URL. Please Provide a Valid Url.");
        exit(1);
    }

    //Extract http from url string
    if (fUrl.substr(0, 7) == "http://")
        fUrl.erase(0, 7);

    //Extract https from url string
    if (fUrl.substr(0, 8) == "https://")
        fUrl.erase(0, 8);

    //Find index of ':' character
    //This typically indicates the presence of a port number
    k = fUrl.find(':');

    //Find index of '/' character
    //This typically indicates the presence of a filepath
    i = fUrl.find('/');

    //If a ':' is found
    if (k != string::npos) {

        //Extract server name
        serverName = fUrl.substr(0, k);

        //If '/' character is found
        if (i != string::npos) {

            //Determine length of port number string
            j = (i - k) - 1;

            //Extract port number string
            portNum = fUrl.substr(k + 1, j);

            //If no port number follows ':' character
            if(!std::any_of(portNum.begin(), portNum.end(), ::isdigit)) {

                //Set default port number
                portNum = PORTNO;
                fprintf(stderr,"No Port Number Given; Defaulting to Port: 80");
            }

            //Extract filepath
            filepath = fUrl.substr(i);

        }
        //If no '/' character is found
        else {

            //Extract port number string
            portNum = fUrl.substr(k + 1);

            //If no port number follows ':' character
            if(!std::any_of(portNum.begin(), portNum.end(), ::isdigit)) {

                //Set default port number
                portNum = PORTNO;
                fprintf(stderr,"No Port Number Given; Defaulting to Port: 80");
            }

            //Set Default filepath
            filepath = "/";
        }
        return;
    }
    //If '/' character is found, but no ':' character is found
    if (i != string::npos && k == string::npos) {

        //Extract server name
        serverName = fUrl.substr(0, i);

        //Extract filepath
        filepath = fUrl.substr(i);

        //Set default port number
        portNum = PORTNO;

        return;
    }
    //If no port number or file path was detected
    else {

        //Set url
        serverName = fUrl;

        //Set default filepath
        filepath = "/";

        //Set default port number
        portNum = PORTNO;
    }
}

int main(int argc, char *argv[]) {

    struct addrinfo hints, *servinfo, *partner;
    int server, sockDes, received, sent, contentLen, headerIndex, contentLengthIndex;
    int cHeaderLen = 16;
    char rBuffer[MAXCHAR];
    string serverName, filepath, portNum, requestUri, rHeader, bodyStrLen, rBody;

    //Check argument count
    if (argc < 2) {
        fprintf(stderr, "Error, no URL provided\n");
        exit(0);
    }

    //Take in Url from command line args and pick out important information.
    formatUrl(argv[1], serverName, filepath, portNum);

    //Construct Request Uri with provided information.
    requestUri = "GET " + filepath + " HTTP/1.1\r\nHost: " + serverName + "\r\nConnection: close\r\n\r\n";

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    //Determine server information
    if ((server = getaddrinfo(serverName.c_str(), portNum.c_str(), &hints, &servinfo)) != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(server));
        return 1;
    }

    //Establish connection
    for (partner = servinfo; partner != NULL; partner = partner->ai_next) {
        //If socket info is incorrect
        if ((sockDes = socket(partner->ai_family, partner->ai_socktype,
                              partner->ai_protocol)) == -1) {
            perror("client: socket");
            continue;
        }
        //If connection cannot be established
        if (connect(sockDes, partner->ai_addr, partner->ai_addrlen) == -1) {
            close(sockDes);
            perror("client: connect");
            continue;
        }
        break;
    }

    sent = 0;   //Bytes sent
    int bytesToSend = strlen(requestUri.c_str());   //Bytes yet to send

    //Loop until complete message has been sent
    while ((sent = send(sockDes, requestUri.c_str(), strlen(requestUri.c_str()), 0)) < 0)
        bytesToSend -= sent;
    if (sent < 0)
        perror("Send Error");

    //Loop until complete Response has been received
    while((received = recv(sockDes, rBuffer, MAXCHAR, 0))>0){
        int i = 0;
        while(i < received){

            //Find break b/w header and body.
            headerIndex = rHeader.find("\r\n\r\n");

            //If end of header is found
            if((size_t)headerIndex != string::npos){

                //Find header field 'content-length'
                contentLengthIndex = rHeader.find("Content-Length:");

                //If header field 'content-length' not found
                if((size_t)contentLengthIndex == string::npos) {
                    fprintf(stderr, "Header Field 'Content-Length' Not Found");
                    exit(1);
                }

                //Extract header-length string
                bodyStrLen = rHeader.substr(contentLengthIndex + cHeaderLen,
                        headerIndex - (contentLengthIndex + cHeaderLen));

                //Convert header-length string to integer
                contentLen = stoi(bodyStrLen);

                //Append to body string
                rBody += rBuffer[i];
            }
            //If end of header hasn't been found
            else{
                //Append to header string
                rHeader += rBuffer[i];
            }
            i++;
        }
    }
    if (received < 0)
        perror("Receive Error");

    //Close socket and free memory
    close(sockDes);
    freeaddrinfo(servinfo);

    //Print results to stderr and to stdout
    fprintf(stderr, requestUri.c_str());
    fprintf(stderr, rHeader.c_str());
    fwrite(rBody.c_str(), sizeof(char), contentLen, stdout);

    return 0;
}


