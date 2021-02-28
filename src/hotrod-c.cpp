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
 * \section Implementing in other languages
 * 
 * C code is written as plain a possible, it should be quite easy to understand and translate.
 * 
 * Byte stream to hotrod data struct translation requires a `streamReader` function able to read bytes from
 * the stream gived a preallocated buffer and the number of bytes to be read.
 * 
 * Implementation roadmap should go through:
 * - how read n bytes     see readBytes()
 * - how to read 1 byte   see readByte()
 * - VInt                 see readVInt()
 * - VLong                see readVLong()
 * - responseHeader       see readResponseHeader() and ::responseHeader
 * 
 */

/**
 * @file
 * @brief This is the C implementation of the hotrod 3.0 protocol for client.
 * Use this implementation as reference for implementing other non-Java hotrod protocol.
 */
typedef struct {
    int len;
    uint8_t *buff;
} byteArray;

typedef struct {
    uint8_t magic;
    uint64_t messageId;
    uint8_t version;
    uint8_t opCode;
    byteArray cacheName;
    uint32_t flags;
    uint8_t clientIntelligence;
    uint32_t topologyId;
} requestHeader;

typedef struct {
    uint8_t magic;
    uint64_t messageId;
    uint8_t opCode;
    uint8_t status;
    char* errorMessage;
    uint8_t topologyChanged;
} responseHeader;

typedef void (*streamReader)(int len, uint8_t *val);
typedef void (*streamWriter)(int len, uint8_t *val);

/**
 * Read 1 byte from the stream
 */
uint8_t readByte(streamReader reader) {
    uint8_t val;
    reader(1, &val);
    return val;
}

/**
 * write 1 byte to the stream
 */
void writeByte(streamWriter writer, uint8_t val) {
    writer(1, &val);
}

/** 
 * Read an unsigned int from the stream of bytes
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
 * Write an unsigned int from the stream of bytes
 * 
 * the value is represented in the stream as a sequence of bytes
 * starting from the less significant. For each byte the most significant
 * bit it's not part of the value but is used as a stop bit (0 means stop).
 * Code is reported here as reference implementation:
 * 
 *      while (val>0x7f) {
 *         uint8_t b = (val & 0x7f) | 0x80;
 *         writeByte(writer, b);
 *         val >>=7;
 *     }
 *     writeByte(writer, val); 
 */
void writeVInt(streamWriter writer, uint32_t val) {
    while (val>0x7f) {
        uint8_t b = (val & 0x7f) | 0x80;
        writeByte(writer, b);
        val >>=7;
    }
    writeByte(writer, val);
}

/** 
 * Read an unsigned long from the stream of bytes
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
 * Write an unsigned long from the stream of bytes
 * 
 * @see writeVInt
 */
void writeVLong(streamWriter writer, uint64_t val) {
    while (val>0x7f) {
        uint8_t b = (val & 0x7f) | 0x80;
        writeByte(writer, b);
        val >>=7;
    }
    writeByte(writer, val);
}

/**
 * Read an byte array from the stream
 * 
 * Array length is represented as vInt @see readVInt and
 * preceed the array content
 */
uint32_t readBytes(streamReader reader, uint8_t* &str) {
  uint32_t size = readVInt(reader);
  str=(uint8_t*)malloc(sizeof(uint8_t)*size);
  reader(size, str);
  return size;
}

/**
 * Write an byte array from the stream
 * 
 * Array length is represented as vInt @see writeVInt and
 * preceed the array content
 */
void writeBytes(streamWriter writer, uint8_t* &str, uint32_t len) {
  writeVInt(writer, len);
  writer(len, str);
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
const uint8_t OK_STATUS                          = 0x00; ///< No error
const uint8_t INVALID_MAGIC_OR_MESSAGE_ID_STATUS = 0x81; ///< Invalid magic or message id
const uint8_t UNKNOWN_COMMAND_STATUS             = 0x82; ///< Unknown command
const uint8_t UNKNOWN_VERSION_STATUS             = 0x83; ///< Unknown version
const uint8_t REQUEST_PARSING_ERROR_STATUS       = 0x84; ///< Request parsing error
const uint8_t SERVER_ERROR_STATUS                = 0x85; ///< Server Error
const uint8_t COMMAND_TIMEOUT_STATUS             = 0x86; ///< Command timed out
/**@}*/

/**
 * \defgroup RequestOpCode Operation code for request
 * @{
 */
const uint8_t PUT_RESPONSE                   = 0x02;
const uint8_t GET_RESPONSE                   = 0x04;
const uint8_t PUT_IF_ABSENT_RESPONSE         = 0x06;
const uint8_t REPLACE_RESPONSE               = 0x08;
const uint8_t REPLACE_IF_UNMODIFIED_RESPONSE = 0x0A;
const uint8_t REMOVE_RESPONSE                = 0x0C;
const uint8_t REMOVE_IF_UNMODIFIED_RESPONSE  = 0x0E;
const uint8_t CONTAINS_KEY_RESPONSE          = 0x10;
const uint8_t GET_WITH_VERSION_RESPONSE      = 0x12;
const uint8_t CLEAR_RESPONSE                 = 0x14;
const uint8_t STATS_RESPONSE                 = 0x16;
const uint8_t PING_RESPONSE                  = 0x18;
const uint8_t BULK_GET_RESPONSE              = 0x1A;
const uint8_t GET_WITH_METADATA_RESPONSE     = 0x1C;
const uint8_t BULK_GET_KEYS_RESPONSE         = 0x1E;
const uint8_t QUERY_RESPONSE                 = 0x20;
const uint8_t AUTH_MECH_LIST_RESPONSE        = 0x22;
const uint8_t AUTH_RESPONSE                  = 0x24;
const uint8_t ADD_CLIENT_LISTENER_RESPONSE   = 0x26;
const uint8_t REMOVE_CLIENT_LISTENER_RESPONSE = 0x28;
const uint8_t SIZE_RESPONSE                  = 0x2A;
const uint8_t EXEC_RESPONSE                  = 0x2C;
const uint8_t PUT_ALL_RESPONSE               = 0x2E;
const uint8_t GET_ALL_RESPONSE               = 0x30;
const uint8_t ITERATION_NEXT_RESPONSE        = 0x34;
const uint8_t ITERATION_END_RESPONSE         = 0x36;
const uint8_t ITERATION_START_RESPONSE       = 0x32;
const uint8_t GET_STREAM_RESPONSE            = 0x38;
const uint8_t PUT_STREAM_RESPONSE            = 0x3A;
const uint8_t PREPARE_RESPONSE               = 0x3C;
const uint8_t COMMIT_RESPONSE                = 0x3E;
const uint8_t ROLLBACK_RESPONSE              = 0x40;
const uint8_t ERROR_RESPONSE                 = 0x50;
const uint8_t CACHE_ENTRY_CREATED_EVENT_RESPONSE = 0x60;
const uint8_t CACHE_ENTRY_MODIFIED_EVENT_RESPONSE = 0x61;
const uint8_t CACHE_ENTRY_REMOVED_EVENT_RESPONSE = 0x62;
const uint8_t CACHE_ENTRY_EXPIRED_EVENT_RESPONSE = 0x63;
const uint8_t COUNTER_CREATE_RESPONSE            = 0x4C;
const uint8_t COUNTER_GET_CONFIGURATION_RESPONSE = 0x4E;
const uint8_t COUNTER_IS_DEFINED_RESPONSE        = 0x51;
const uint8_t COUNTER_ADD_AND_GET_RESPONSE       = 0x53;
const uint8_t COUNTER_RESET_RESPONSE             = 0x55;
const uint8_t COUNTER_GET_RESPONSE               = 0x57;
const uint8_t COUNTER_CAS_RESPONSE               = 0x59;
const uint8_t COUNTER_ADD_LISTENER_RESPONSE      = 0x5B;
const uint8_t COUNTER_REMOVE_LISTENER_RESPONSE   = 0x5D;
const uint8_t COUNTER_REMOVE_RESPONSE            = 0x5F;
const uint8_t COUNTER_GET_NAMES_RESPONSE         = 0x65;
const uint8_t COUNTER_EVENT_RESPONSE             = 0x66;
/**@}*/

/**
 * \defgroup ResponseOpCode Operation code for response
 * @{
 */
const uint8_t PUT_REQUEST                    = 0x01;
const uint8_t GET_REQUEST                    = 0x03;
const uint8_t PUT_IF_ABSENT_REQUEST          = 0x05;
const uint8_t REPLACE_REQUEST                = 0x07;
const uint8_t REPLACE_IF_UNMODIFIED_REQUEST  = 0x09;
const uint8_t REMOVE_REQUEST                 = 0x0B;
const uint8_t REMOVE_IF_UNMODIFIED_REQUEST   = 0x0D;
const uint8_t CONTAINS_KEY_REQUEST           = 0x0F;
const uint8_t GET_WITH_VERSION_REQUEST       = 0x11;
const uint8_t CLEAR_REQUEST                  = 0x13;
const uint8_t STATS_REQUEST                  = 0x15;
const uint8_t PING_REQUEST                   = 0x17;
const uint8_t BULK_GET_REQUEST               = 0x19;
const uint8_t GET_WITH_METADATA_REQUEST      = 0x1B;
const uint8_t BULK_GET_KEYS_REQUEST          = 0x1D;
const uint8_t QUERY_REQUEST                  = 0x1F;
const uint8_t AUTH_MECH_LIST_REQUEST         = 0x21;
const uint8_t AUTH_REQUEST                   = 0x23;
const uint8_t ADD_CLIENT_LISTENER_REQUEST    = 0x25;
const uint8_t REMOVE_CLIENT_LISTENER_REQUEST = 0x27;
const uint8_t SIZE_REQUEST                   = 0x29;
const uint8_t EXEC_REQUEST                   = 0x2B;
const uint8_t PUT_ALL_REQUEST                = 0x2D;
const uint8_t GET_ALL_REQUEST                = 0x2F;
const uint8_t ITERATION_START_REQUEST        = 0x31;
const uint8_t ITERATION_NEXT_REQUEST         = 0x33;
const uint8_t ITERATION_END_REQUEST          = 0x35;
const uint8_t GET_STREAM_REQUEST             = 0x37;
const uint8_t PUT_STREAM_REQUEST             = 0x39;
const uint8_t PREPARE_REQUEST                = 0x3B;
const uint8_t COMMIT_REQUEST                 = 0x3D;
const uint8_t ROLLBACK_REQUEST               = 0x3F;
const uint8_t COUNTER_CREATE_REQUEST         = 0x4B;
const uint8_t COUNTER_GET_CONFIGURATION_REQUEST = 0x4D;
const uint8_t COUNTER_IS_DEFINED_REQUEST     = 0x4F;
const uint8_t COUNTER_ADD_AND_GET_REQUEST    = 0x52;
const uint8_t COUNTER_RESET_REQUEST          = 0x54;
const uint8_t COUNTER_GET_REQUEST            = 0x56;
const uint8_t COUNTER_CAS_REQUEST            = 0x58;
const uint8_t COUNTER_ADD_LISTENER_REQUEST   = 0x5A;
const uint8_t COUNTER_REMOVE_LISTENER_REQUEST = 0x5C;
const uint8_t COUNTER_REMOVE_REQUEST         = 0x5E;
const uint8_t COUNTER_GET_NAMES_REQUEST      = 0x64;

/**@}*/

int readResponseError(uint8_t status, streamReader reader, uint8_t *&errorMsg) {
    switch (status) {
        case INVALID_MAGIC_OR_MESSAGE_ID_STATUS:
        case UNKNOWN_COMMAND_STATUS:
        case UNKNOWN_VERSION_STATUS:
        case REQUEST_PARSING_ERROR_STATUS:
        case SERVER_ERROR_STATUS:
        case COMMAND_TIMEOUT_STATUS:
        return readBytes(reader, errorMsg);
    }
    errorMsg=nullptr;
    return 0;
}

/**
 *  readResponseHeader populates and header a 3.0 hotrod response
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
void readResponseHeader(streamReader reader, responseHeader &hdr) {
    hdr.magic = readByte(reader);
    hdr.messageId = readVLong(reader);
    hdr.opCode = readByte(reader);
    hdr.status = readByte(reader);
    // Following cast is true when sizeof(char)==8
    uint8_t *errMsg; 
    readResponseError(hdr.status, reader, errMsg);
    hdr.errorMessage= (char*)errMsg;
}

/**
 *  writeRequestHeader populates and header a 3.0 hotrod response
 *  
 * Hotrod 3.0 response header description
 * 
 * Field | Size (bytes) or type | Comment | References
 * ------|----------------------|---------|------------
 * Magic | 1 | Valid value is 0xA0 | |
 * Message ID | vLong | | @ref readVLong |
 * Protocol Version | 1 | | |
 * Operation Code | 1 | request opcode | @ref RequestOpcode |
 * Cache name | array | mandatory | A 0 lenght name means server default cache |
 * Flags | vInt |  | @ref addRefHere TODO |
 * Client Intelligence | 1 | @ref Client Intelligence | 
 * Topology Id | vInt | id of the topology in use |
 */
void writeRequestHeader(streamWriter writer, requestHeader hdr) {
    writeByte(writer, hdr.magic);
    writeVLong(writer, hdr.messageId);
    writeByte(writer, hdr.version);
    writeByte(writer, hdr.opCode);
    writeBytes(writer, hdr.cacheName.buff, hdr.cacheName.len);
    writeVInt(writer, hdr.flags);
    writeByte(writer, hdr.clientIntelligence);
    writeVInt(writer, hdr.topologyId);
}

/**
 * writeRequestWithKey send a request for operations that has a key as parameter
 * 
 * This function should not be called directly, though it could be used as a general func
 * to request execution of operations with 1 key as parameter if the specific func is missing.
 */
void writeRequestWithKey(streamWriter writer, requestHeader &hdr, byteArray &keyName) {
    writeRequestHeader(writer, hdr);
    writeBytes(writer, keyName.buff, keyName.len);
}

/**
 * writeGetRequest sends a GET request
 * 
 * After this call a @ref readGetResponse must be performed on the same stream to read get response
 * result.
 */
void writeGetRequest(streamWriter writer, requestHeader &hdr, byteArray &keyName) {
    hdr.opCode=GET_REQUEST;
    writeRequestWithKey(writer, hdr, keyName);
}

/**
 * readGetResponse read a GET response
 * 
 * This must be call after a @ref writeGetRequest has been executed to read get response result.
 */
void readGetResponse(streamReader reader, responseHeader &hdr, byteArray &arr) {
    readResponseHeader(reader, hdr);
    if (hdr.status == OK_STATUS) {
       arr.len= readBytes(reader, arr.buff);
    }
}
