#include <stdint.h>

/*! \mainpage A Reference Implementation in plain C for Hotrod protocol 2.8+

 *
 * \section intro_sec Introduction
 *
 * This project aims to provide a C library that speeds up Infinispan Hotrod clients development and is:
 * - easy to understand and extends;
 * - easy to use and install;
 * - easy to translate in other languages.
 *
 * \section Implementing in other languages
 * 
 * C code is written as plain a possible, it should be quite easy to understand and translate.
 * 
 * User must provide two function for read/write bytes from/to the stream:
 * - a `streamReader` function which read bytes from the stream gived a preallocated buffer
 * and the number of bytes to be read.
 * - a `streamWriter' function which writes bytes to the stream gived a data buffer and
 * its length.
 * Both the functions require an opaque pointer to a context, that can be used during the execution. Context
 * typically will contain the socket identifier and the io status.
 * 
 * Implementation roadmap should go through:
 * - how read n bytes     see readBytes()
 * - how to read 1 byte   see readByte()
 * - VInt                 see readVInt()
 * - VLong                see readVLong()
 * - responseHeader       see readResponseHeader() and ::responseHeader
 * - topology       see readNewTopology()
 * - mediaType       see readMediaType()
 * 
 */

/**
 * \defgroup ClientIntelligence Client intelligence level
 * @{
 */
const uint8_t CLIENT_INTELLIGENCE_BASIC                   = 0x01;
const uint8_t CLIENT_INTELLIGENCE_TOPOLOGY_AWARE          = 0x02;
const uint8_t CLIENT_INTELLIGENCE_HASH_DISTRIBUTION_AWARE = 0x03; 
/**@}*/

/**
 * @file
 * @brief This is the C implementation of the hotrod 2.8 protocol for client.
 * Use this implementation as reference for implementing other non-Java hotrod protocol.
 */
typedef struct {
    int len;
    uint8_t *buff;
} byteArray;

typedef struct {
    uint8_t infoType;
    uint32_t predefinedMediaType;
    byteArray customMediaType;
    uint32_t paramsNum;
    byteArray *keys;
    byteArray *values;
} mediaType;

typedef struct {
    uint8_t magic;
    uint64_t messageId;
    uint8_t version;
    uint8_t opCode;
    byteArray cacheName;
    uint32_t flags;
    uint8_t clientIntelligence;
    uint32_t topologyId;
    mediaType keyMediaType;
    mediaType valueMediaType;
} requestHeader;

typedef struct {
    uint8_t magic;
    uint64_t messageId;
    uint8_t opCode;
    uint8_t status;
    byteArray error;
    uint8_t topologyChanged;
} responseHeader;

typedef struct {
    uint32_t topologyId;
    uint32_t serversNum;
    byteArray *servers;
    uint16_t *ports;
    uint8_t hashFuncNum;
    uint32_t segmentsNum;
    uint8_t *ownersNumPerSegment;
    uint32_t **ownersPerSegment;
    uint8_t status;
    byteArray error;
    uint8_t topologyChanged;
} topologyInfo;

typedef void (*streamReader)(void* ctx, uint8_t *val, int len);
typedef void (*streamWriter)(void* ctx, uint8_t *val, int len);


/**
 * writeGet sends a GET request
 * 
 * After this call a @ref readGet must be performed on the same stream to read get response
 * result.
 */
void writeGet(void *ctx, streamWriter writer, requestHeader *hdr, byteArray *keyName);

/**
 * readGet read a GET response
 * 
 * This must be call after a @ref writeGet has been executed to read a get response result.
 */
void readGet(void *ctx, streamReader reader, responseHeader *hdr, requestHeader *reqHdr, topologyInfo *tInfo, byteArray *arr);


/**
 * writePut send a request for a put operation
 * 
 * After this call a @ref readPut must be performed on the same stream to read a put response
 * result.
 * 
 */
void writePut(void *ctx, streamWriter writer, requestHeader *hdr, byteArray *keyName, byteArray *keyValue);

void readPut(void *ctx, streamReader reader, responseHeader *hdr, requestHeader *reqHdr, topologyInfo *tInfo, byteArray *arr);

void writePing(void *ctx, streamWriter writer, requestHeader *hdr);
void readPing(void *ctx, streamReader reader, responseHeader *hdr, requestHeader *reqHdr, topologyInfo *tInfo, mediaType *keyMt, mediaType *valueMt);
