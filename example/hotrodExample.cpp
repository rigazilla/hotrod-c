#include <iostream>

#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <netdb.h>
#include <fcntl.h>
#include <poll.h>

#include <hotrod-c.h>

using namespace std;

typedef struct {
    int socket;
    int hasError;
} streamCtx;

void reader(void *ctx, uint8_t *val, int len) {
    streamCtx *sc = (streamCtx*)ctx;
    int count = read( sc->socket , val, len);
    if (!sc->hasError) {
        sc->hasError=errno;
    }
}

void writer(void *ctx, uint8_t *val, int len) {
    streamCtx *sc = (streamCtx*)ctx;
    int count = send(sc->socket, val, len, 0);
    if (!sc->hasError) {
        sc->hasError=errno;
    }
}

void cleaner(void *ctx) {
    streamCtx *sc = (streamCtx*)ctx;
    close(sc->socket);
}

int getSocket(string addr, uint16_t port);

int main() {
    int sock = getSocket("127.0.0.1",11222);
    streamCtx ctx = {sock, 0};
    requestHeader rqh, rqPutH;
    responseHeader rsh;
    byteArray keyArr, valArr, res;
    char* key="key";
    char* value="value";
    keyArr.buff= (uint8_t*)key;
    keyArr.len=4;
    valArr.buff=(uint8_t*)value;
    valArr.len=6;

    rqh.magic=0xA0;
    rqh.messageId=1;
    rqh.clientIntelligence=CLIENT_INTELLIGENCE_BASIC;
    rqh.cacheName.len=0;
    rqh.version=28;
    rqh.flags=0;
    rqh.topologyId=0x09;

    rqPutH.magic=0xA0;
    rqPutH.messageId=1;
    rqPutH.clientIntelligence=CLIENT_INTELLIGENCE_BASIC;
    rqPutH.cacheName.len=0;
    rqPutH.version=28;
    rqPutH.flags=0;
    rqPutH.topologyId=0x09;

    writePut(&ctx, writer,&rqPutH, &keyArr, &valArr);
    readPut(&ctx, reader, &rsh, &res);
    writeGet(&ctx, writer, &rqh, &keyArr);
    readGet(&ctx, reader, &rsh, &res);

    cleaner(&ctx);
  return 0;
}

int getSocket(string addr, uint16_t port) {
    int sock = 0, valread;
    struct sockaddr_in serv_addr;
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) 
    { 
        printf("\n Socket creation error \n"); 
        return -1; 
    } 
   	int flags = fcntl(sock, F_GETFL, 0);
	fcntl(sock, F_SETFL, flags | O_NONBLOCK);
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
       
    // Convert IPv4 and IPv6 addresses from text to binary form 
    if(inet_pton(AF_INET, addr.data(), &serv_addr.sin_addr)<=0)  
    { 
        printf("\nInvalid address/ Address not supported \n"); 
        return -1; 
    }

    connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr));
    printf("errno=%x\n",errno);
    if (sock < 0 && errno == EINPROGRESS) {
		pollfd fds[1];
		fds[0].fd = sock;
		fds[0].events = POLLOUT;
		auto evCount = poll(fds, 1, 0);
		if (evCount > 0) {
			if ((POLLOUT ^ fds[0].revents) != 0) {
				printf("Failed to connect to %s:%d", addr, port);
			} else {
				int opt;
				socklen_t optlen = sizeof(opt);
				sock = getsockopt(sock, SOL_SOCKET, SO_ERROR, (void*) ((&opt)),
						&optlen);
			}
		} else if (evCount == 0) {
			printf("Timed out connecting to %s:%d", addr, port);
		}
	}
    if (errno!=0 && errno!=0x73)
    { 
        printf("\nConnection Failed \n"); 
        return -1; 
    }
    return sock;
}
