#include <iostream>
#include <errno.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <netdb.h>
#include <fcntl.h>
#include <poll.h>

#include "hotrod-c.h"
#include "murmurHash3.h"

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
    int count = send(sc->socket, val, len, MSG_NOSIGNAL);
    printf("Oh dear, %d %s\n", errno, strerror(errno));
    if (!sc->hasError) {
        sc->hasError=errno;
    }
}

void cleaner(void *ctx) {
    streamCtx *sc = (streamCtx*)ctx;
    close(sc->socket);
}

int getSocket(const char * addr, uint16_t port);


uint32_t getNormalizedHash32(uint32_t objectId) {
    return hash32(objectId) & 0x7fffffff;
}

uint32_t getNormalizedHashVoidPtr(const void *key, int size) {
     return hashVoidPtr(key,size) & 0x7fffffff;
}

uint32_t getSegment32(uint32_t objectId, unsigned int numSegments) {
    uint32_t segmentSize = (uint32_t)(0x7FFFFFFFUL/numSegments)+1;
    uint32_t hash = getNormalizedHash32(objectId);
    return hash/segmentSize;
}

uint32_t getSegmentVoidPtr(void *key, int size, unsigned int numSegments) {
    uint32_t segmentSize = (uint32_t)(0x7FFFFFFFUL/numSegments)+1;
    uint32_t hash = getNormalizedHashVoidPtr(key, size);
    return hash/segmentSize;
}

uint32_t* getServerListVoidPtr(topologyInfo *t, void *key, int size) {
    uint32_t seg = getSegmentVoidPtr(key, size, t->segmentsNum);
    return t->ownersPerSegment[seg];
}

uint8_t getServerListSizeVoidPtr(topologyInfo *t, void *key, int size) {
    uint32_t seg = getSegmentVoidPtr(key, size, t->segmentsNum);
    return t->ownersNumPerSegment[seg];
}

uint32_t* getServerList32(topologyInfo *t, uint32_t objectId) {
    uint32_t seg = getSegment32(objectId, t->segmentsNum);
    return t->ownersPerSegment[seg];
}

uint8_t getServerListSize32(topologyInfo *t, uint32_t objectId) {
    uint32_t seg = getSegment32(objectId, t->segmentsNum);
    return t->ownersNumPerSegment[seg];
}


int main() {
    int sock = getSocket("127.0.0.1",11222);
    streamCtx ctx = {sock, 0};
    requestHeader rqh, rqPutH;
    responseHeader rsh, rshPing;
    byteArray keyArr, valArr, res;
    const char* key="key";
    const char* value="value";
    keyArr.buff= (uint8_t*)key;
    keyArr.len=4;
    valArr.buff=(uint8_t*)value;
    valArr.len=6;

    rqh.magic=0xA0;
    rqh.messageId=1;
    rqh.clientIntelligence=CLIENT_INTELLIGENCE_HASH_DISTRIBUTION_AWARE;
    rqh.cacheName.len=0;
    rqh.version=30;
    rqh.flags=0;
    rqh.topologyId=0x02;

    mediaType mt;
    mt.infoType=0;

    rqh.keyMediaType = mt;
    rqh.valueMediaType = mt;

    rqPutH.magic=0xA0;
    rqPutH.messageId=1;
    rqPutH.clientIntelligence=CLIENT_INTELLIGENCE_HASH_DISTRIBUTION_AWARE;
    rqPutH.cacheName.len=0;
    rqPutH.version=30;
    rqPutH.flags=0;
    rqPutH.topologyId=0x02;
    rqPutH.keyMediaType = mt;
    rqPutH.valueMediaType = mt;

    topologyInfo tInfo;
    mediaType keyMt, valueMt;

    writePing(&ctx, writer, &rqPutH);
    readPing(&ctx, reader, &rshPing, &rqPutH, &tInfo, &keyMt, &valueMt);

    printf("Storing entry (%s,%s)\n",key, value);

    uint32_t* vect = getServerListVoidPtr(&tInfo, keyArr.buff, keyArr.len);

    char* str = (char *)malloc(keyArr.len+1);
    memcpy(str, tInfo.servers[vect[0]].buff,tInfo.servers[vect[0]].len);
    str[tInfo.servers[vect[0]].len]=0;
    int sock1 = getSocket(str, tInfo.ports[vect[0]]);
    streamCtx ctx1 = {sock1, 0};

    writePut(&ctx1, writer, &rqPutH, &keyArr, &valArr);
    if (ctx.hasError) {
        // Handle here transport error case
        printf("writer error! %s\n", ctx1.hasError, strerror(ctx1.hasError));
        exit(ctx1.hasError);
    }
    readPut(&ctx1, reader, &rsh, &rqPutH, &tInfo, &res);
    if (ctx1.hasError) {
        // Handle here transport error case
        printf("reader error! %d %s\n", ctx.hasError, strerror(ctx.hasError));
        exit(ctx1.hasError);
    }
    if (rsh.error.buff!=nullptr) {
        printf("hotrod error: %.*s\n", rsh.error.len, rsh.error.buff);
        // Handle here hotrod error case
        free(rsh.error.buff);
    }
    writeGet(&ctx1, writer, &rqh, &keyArr);
    if (ctx1.hasError) {
        // Handle here transport error case
        printf("writer error! %d %s\n", ctx1.hasError, strerror(ctx1.hasError));
        exit(ctx1.hasError);
    }
    readGet(&ctx1, reader, &rsh, &rqh, &tInfo, &res);
    if (ctx1.hasError) {
        // Handle here transport error case
        printf("reader error! %d %s\n", ctx1.hasError, strerror(ctx1.hasError));
        exit(ctx1.hasError);
    }
    if (rsh.error.buff!=nullptr) {
        printf("hotrod error: %.*s\n", rsh.error.len, rsh.error.buff);
        // Handle here the error case
        free(rsh.error.buff);
    } else {
        printf("Read entry (%s,%.*s)\n",key, res.len, res.buff);
    }
    cleaner(&ctx1);
    cleaner(&ctx);
  return 0;
}



int getSocket(const char* str, uint16_t port) {
    int sock = 0, valread;
    struct sockaddr_in serv_addr;
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) 
    { 
        printf("\n Socket creation error \n"); 
        return -1; 
    } 
   	int flags = fcntl(sock, F_GETFL, 0);
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    // Convert IPv4 and IPv6 addresses from text to binary form 
    if(inet_pton(AF_INET, str, &serv_addr.sin_addr)<=0)
    { 
        printf("\nInvalid address/ Address not supported \n"); 
        return -1; 
    }

    connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr));
    if (sock < 0)
    { 
        printf("\nConnection Failed \n"); 
        return -1; 
    }
    return sock;
}
