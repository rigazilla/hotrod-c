#include <iostream>
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
 */

/**
 * @file
 * @brief This is the C implementation of the hotrod 3.0 protocol for client.
 * Use this implementation as reference for implementing other non-Java hotrod protocol.
 */
typedef struct {
    int len;
    int8_t *buff;
} byteArray;

typedef struct {
    u_int8_t magic;
    u_int64_t messageId;
    u_int8_t version;
    u_int8_t opCode;
    byteArray cacheName;
    u_int32_t flags;
    u_int8_t clientIntelligence;
    u_int32_t topologyId;
} requestHeader;

typedef struct {
    u_int8_t magic;
    u_int64_t messageId;
    u_int8_t opCode;
    u_int8_t status;
    char* errorMessage;
    u_int8_t topologyChanged;
} responseHeader;

typedef u_int8_t (*streamReader)(int, u_int8_t *);

/** 
 * Read an int from the stream of bytes
 * 
 * the value is represented in the stream as a sequence of bytes
 * starting from the less significant. For each byte the most significant
 * bit it's not part of the value but is used as a stop bit (0 means stop).
 * Code is reported here as reference implementation:
 * 
 *     uint8_t b = readByte(reader);
 *     reader(1, &b);
 *     int32_t i = b & 0x7F;
 *     for (int shift = 7; (b & 0x80) != 0; shift += 7) {
 *         b = readByte(reader);
 *         i |= (b & 0x7FL) << shift;
 *     }
 *     return i;
 */
uint32_t readVInt(streamReader reader) {
    uint8_t b = readByte(reader);
    reader(1, &b);
    int32_t i = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
        b = readByte(reader);
        i |= (b & 0x7FL) << shift;
    }
    return i;
}

/** 
 * Read a long from the stream of bytes
 * 
 * @see readVInt
 */
uint64_t readVLong(streamReader reader) {
    uint8_t b = readByte(reader);
    int64_t i = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
        b = readByte(reader);
        i |= (b & 0x7FL) << shift;
    }
    return i;
}

/**
 * Read 1 byte from the stream
 */
u_int8_t readByte(streamReader reader) {
    u_int8_t b;
    reader(1, &b);
    return b;
}


/**
 * Read an byte array from the stream
 * 
 * Array length is represented as vInt @see readVInt and
 * preceed the array content
 */
uint64_t readBytes(streamReader reader, u_int8_t* &str) {
  uint32_t size = readVInt(reader);
  str=(u_int8_t*)malloc(sizeof(uint8_t)*size);
  reader(size, str);
  return size;
}

/**
 * \defgroup ResponseOpcode Response opcode
 * @{
 */

/**@}*/

/**
 * \defgroup ErrorResponseCode Error response code
 * @{
 */
const u_int8_t OK                                 = 0x00; ///< No error
const u_int8_t INVALID_MAGIC_OR_MESSAGE_ID_STATUS = 0x81; ///< Invalid magic or message id
const u_int8_t UNKNOWN_COMMAND_STATUS             = 0x82; ///< Unknown command
const u_int8_t UNKNOWN_VERSION_STATUS             = 0x83; ///< Unknown version
const u_int8_t REQUEST_PARSING_ERROR_STATUS       = 0x84; ///< Request parsing error
const u_int8_t SERVER_ERROR_STATUS                = 0x85; ///< Server Error
const u_int8_t COMMAND_TIMEOUT_STATUS             = 0x86; ///< Command timed out
/**@}*/

int readResponseError(u_int8_t status, streamReader reader, u_int8_t *&error) {
    switch (status) {
        case INVALID_MAGIC_OR_MESSAGE_ID_STATUS:
        case UNKNOWN_COMMAND_STATUS:
        case UNKNOWN_VERSION_STATUS:
        case REQUEST_PARSING_ERROR_STATUS:
        case SERVER_ERROR_STATUS:
        case COMMAND_TIMEOUT_STATUS:
        return readBytes(reader, error);
    }
    error=nullptr;
    return 0;
}
/**
 *  readHeader populates and header a 3.0 hotrod response
 *  
 * Hotrod 3.0 response header description
 * 
 * Field | Size (bytes) or type | Comment | References
 * ------|----------------------|---------|------------
 * Magic | 1 | Valid value is 0xA1 | |
 * Message ID | vLong | | @ref readVLong |
 * Operation Code | 1 | response opcode | @ref ResponseOpcode |
 * Status Code | 1 | status code | @ref ErrorResponseCode |
 * Error Message | array | optional | @ref readResponseError, @ref readBytes | 
 */
int readHeader(streamReader reader, responseHeader &hdr, int &errcode, u_int8_t *&errmessage) {
    hdr.magic = readByte(reader);
    hdr.messageId = readVLong(reader);
    hdr.opCode = readByte(reader);
    hdr.status = readByte(reader);
    readResponseError(hdr.status, reader, errmessage);
    errcode = 0;
    return 0;
}

void say_hello(){
    std::cout << "Hello, from hotrod-c!\n";
}
