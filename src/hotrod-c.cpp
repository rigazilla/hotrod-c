#include <iostream>
#include <stdint.h>
#include <string.h>
#include <hotrod-c.h>


/**
 * Read 1 byte from the stream
 */
uint8_t readByte(void* ctx, streamReader reader) {
    uint8_t val;
    reader(ctx, &val, 1);
    return val;
}

/**
 * write 1 byte to the stream
 */
void writeByte(uint8_t **buff, uint8_t val) {
    **buff=val;
    ++*buff;
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
uint32_t readVInt(void *ctx, streamReader reader) {
    uint8_t b = readByte(ctx, reader);
    int32_t i = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
        b = readByte(ctx, reader);
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
void writeVInt(uint8_t **buff, uint32_t val) {
    while (val>0x7f) {
        uint8_t b = (val & 0x7f) | 0x80;
        writeByte(buff, b);
        val >>=7;
    }
    writeByte(buff, val);
}

/** 
 * Read an unsigned long from the stream of bytes
 * 
 * @see readVInt
 */
uint64_t readVLong(void *ctx, streamReader reader) {
    uint8_t b = readByte(ctx, reader);
    int64_t i = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
        b = readByte(ctx, reader);
        i |= (b & 0x7FL) << shift;
    }
    return i;
}

/** 
 * Write an unsigned long from the stream of bytes
 * 
 * @see writeVInt
 */
void writeVLong(uint8_t **buff, uint64_t val) {
    while (val>0x7f) {
        uint8_t b = (val & 0x7f) | 0x80;
        writeByte(buff, b);
        val >>=7;
    }
    writeByte(buff, val);
}

/**
 * Read an byte array from the stream
 * 
 * Array length is represented as vInt @see readVInt and
 * preceed the array content
 */
uint32_t readBytes(void *ctx, streamReader reader, uint8_t **str) {
  uint32_t size = readVInt(ctx, reader);
  *str=(uint8_t*)malloc(sizeof(uint8_t)*size);
  reader(ctx, *str, size);
  return size;
}

/**
 * Write an byte array from the stream
 * 
 * Array length is represented as vInt @see writeVInt and
 * preceed the array content
 */
void writeBytes(uint8_t **buff, uint8_t *str, uint32_t len) {
  writeVInt(buff, len);
  if (len>0) {
    memcpy(*buff, str, len);
    *buff+=len;
  }
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
 * \defgroup ResponseOpCode Operation code for request
 * @{
 */
const uint8_t PUT_RESPONSE                        = 0x02;
const uint8_t GET_RESPONSE                        = 0x04;
const uint8_t PUT_IF_ABSENT_RESPONSE              = 0x06;
const uint8_t REPLACE_RESPONSE                    = 0x08;
const uint8_t REPLACE_IF_UNMODIFIED_RESPONSE      = 0x0A;
const uint8_t REMOVE_RESPONSE                     = 0x0C;
const uint8_t REMOVE_IF_UNMODIFIED_RESPONSE       = 0x0E;
const uint8_t CONTAINS_KEY_RESPONSE               = 0x10;
const uint8_t GET_WITH_VERSION_RESPONSE           = 0x12;
const uint8_t CLEAR_RESPONSE                      = 0x14;
const uint8_t STATS_RESPONSE                      = 0x16;
const uint8_t PING_RESPONSE                       = 0x18;
const uint8_t BULK_GET_RESPONSE                   = 0x1A;
const uint8_t GET_WITH_METADATA_RESPONSE          = 0x1C;
const uint8_t BULK_GET_KEYS_RESPONSE              = 0x1E;
const uint8_t QUERY_RESPONSE                      = 0x20;
const uint8_t AUTH_MECH_LIST_RESPONSE             = 0x22;
const uint8_t AUTH_RESPONSE                       = 0x24;
const uint8_t ADD_CLIENT_LISTENER_RESPONSE        = 0x26;
const uint8_t REMOVE_CLIENT_LISTENER_RESPONSE     = 0x28;
const uint8_t SIZE_RESPONSE                       = 0x2A;
const uint8_t EXEC_RESPONSE                       = 0x2C;
const uint8_t PUT_ALL_RESPONSE                    = 0x2E;
const uint8_t GET_ALL_RESPONSE                    = 0x30;
const uint8_t ITERATION_NEXT_RESPONSE             = 0x34;
const uint8_t ITERATION_END_RESPONSE              = 0x36;
const uint8_t ITERATION_START_RESPONSE            = 0x32;
const uint8_t GET_STREAM_RESPONSE                 = 0x38;
const uint8_t PUT_STREAM_RESPONSE                 = 0x3A;
const uint8_t PREPARE_RESPONSE                    = 0x3C;
const uint8_t COMMIT_RESPONSE                     = 0x3E;
const uint8_t ROLLBACK_RESPONSE                   = 0x40;
const uint8_t ERROR_RESPONSE                      = 0x50;
const uint8_t CACHE_ENTRY_CREATED_EVENT_RESPONSE  = 0x60;
const uint8_t CACHE_ENTRY_MODIFIED_EVENT_RESPONSE = 0x61;
const uint8_t CACHE_ENTRY_REMOVED_EVENT_RESPONSE  = 0x62;
const uint8_t CACHE_ENTRY_EXPIRED_EVENT_RESPONSE  = 0x63;
const uint8_t COUNTER_CREATE_RESPONSE             = 0x4C;
const uint8_t COUNTER_GET_CONFIGURATION_RESPONSE  = 0x4E;
const uint8_t COUNTER_IS_DEFINED_RESPONSE         = 0x51;
const uint8_t COUNTER_ADD_AND_GET_RESPONSE        = 0x53;
const uint8_t COUNTER_RESET_RESPONSE              = 0x55;
const uint8_t COUNTER_GET_RESPONSE                = 0x57;
const uint8_t COUNTER_CAS_RESPONSE                = 0x59;
const uint8_t COUNTER_ADD_LISTENER_RESPONSE       = 0x5B;
const uint8_t COUNTER_REMOVE_LISTENER_RESPONSE    = 0x5D;
const uint8_t COUNTER_REMOVE_RESPONSE             = 0x5F;
const uint8_t COUNTER_GET_NAMES_RESPONSE          = 0x65;
const uint8_t COUNTER_EVENT_RESPONSE              = 0x66;
/**@}*/

/**
 * \defgroup RequestOpCode Operation code for response
 * @{
 */
const uint8_t PUT_REQUEST                         = 0x01;
const uint8_t GET_REQUEST                         = 0x03;
const uint8_t PUT_IF_ABSENT_REQUEST               = 0x05;
const uint8_t REPLACE_REQUEST                     = 0x07;
const uint8_t REPLACE_IF_UNMODIFIED_REQUEST       = 0x09;
const uint8_t REMOVE_REQUEST                      = 0x0B;
const uint8_t REMOVE_IF_UNMODIFIED_REQUEST        = 0x0D;
const uint8_t CONTAINS_KEY_REQUEST                = 0x0F;
const uint8_t GET_WITH_VERSION_REQUEST            = 0x11;
const uint8_t CLEAR_REQUEST                       = 0x13;
const uint8_t STATS_REQUEST                       = 0x15;
const uint8_t PING_REQUEST                        = 0x17;
const uint8_t BULK_GET_REQUEST                    = 0x19;
const uint8_t GET_WITH_METADATA_REQUEST           = 0x1B;
const uint8_t BULK_GET_KEYS_REQUEST               = 0x1D;
const uint8_t QUERY_REQUEST                       = 0x1F;
const uint8_t AUTH_MECH_LIST_REQUEST              = 0x21;
const uint8_t AUTH_REQUEST                        = 0x23;
const uint8_t ADD_CLIENT_LISTENER_REQUEST         = 0x25;
const uint8_t REMOVE_CLIENT_LISTENER_REQUEST      = 0x27;
const uint8_t SIZE_REQUEST                        = 0x29;
const uint8_t EXEC_REQUEST                        = 0x2B;
const uint8_t PUT_ALL_REQUEST                     = 0x2D;
const uint8_t GET_ALL_REQUEST                     = 0x2F;
const uint8_t ITERATION_START_REQUEST             = 0x31;
const uint8_t ITERATION_NEXT_REQUEST              = 0x33;
const uint8_t ITERATION_END_REQUEST               = 0x35;
const uint8_t GET_STREAM_REQUEST                  = 0x37;
const uint8_t PUT_STREAM_REQUEST                  = 0x39;
const uint8_t PREPARE_REQUEST                     = 0x3B;
const uint8_t COMMIT_REQUEST                      = 0x3D;
const uint8_t ROLLBACK_REQUEST                    = 0x3F;
const uint8_t COUNTER_CREATE_REQUEST              = 0x4B;
const uint8_t COUNTER_GET_CONFIGURATION_REQUEST   = 0x4D;
const uint8_t COUNTER_IS_DEFINED_REQUEST          = 0x4F;
const uint8_t COUNTER_ADD_AND_GET_REQUEST         = 0x52;
const uint8_t COUNTER_RESET_REQUEST               = 0x54;
const uint8_t COUNTER_GET_REQUEST                 = 0x56;
const uint8_t COUNTER_CAS_REQUEST                 = 0x58;
const uint8_t COUNTER_ADD_LISTENER_REQUEST        = 0x5A;
const uint8_t COUNTER_REMOVE_LISTENER_REQUEST     = 0x5C;
const uint8_t COUNTER_REMOVE_REQUEST              = 0x5E;
const uint8_t COUNTER_GET_NAMES_REQUEST           = 0x64;

/**@}*/

int readResponseError(void *ctx, streamReader reader, uint8_t status, uint8_t **errorMsg) {
    switch (status) {
        case INVALID_MAGIC_OR_MESSAGE_ID_STATUS:
        case UNKNOWN_COMMAND_STATUS:
        case UNKNOWN_VERSION_STATUS:
        case REQUEST_PARSING_ERROR_STATUS:
        case SERVER_ERROR_STATUS:
        case COMMAND_TIMEOUT_STATUS:
        return readBytes(ctx, reader, errorMsg);
    }
    *errorMsg=nullptr;
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
void readResponseHeader(void *ctx, streamReader reader, responseHeader *hdr) {
    hdr->magic = readByte(ctx, reader);
    hdr->messageId = readVLong(ctx, reader);
    hdr->opCode = readByte(ctx, reader);
    hdr->status = readByte(ctx, reader);
    hdr->topologyChanged = readByte(ctx, reader);
    uint8_t *errMsg;
    hdr->error.len= readResponseError(ctx, reader, hdr->status, &errMsg);
    // Following cast is true when sizeof(char)==8
    hdr->error.buff= errMsg;
    // TODO implement here topology changes
}

/**
 *  writeRequestHeader populates and header a 2.8 hotrod response
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
int writeRequestHeader(uint8_t *buff, requestHeader *hdr) {
    uint8_t *curs=buff;
    writeByte(&curs, hdr->magic);
    writeVLong(&curs, hdr->messageId);
    writeByte(&curs, hdr->version);
    writeByte(&curs, hdr->opCode);
    writeBytes(&curs, hdr->cacheName.buff, hdr->cacheName.len);
    writeVInt(&curs, hdr->flags);
    writeByte(&curs, hdr->clientIntelligence);
    writeVInt(&curs, hdr->topologyId);
    writeByte(&curs, 0);
    writeByte(&curs, 0);
    return curs-buff;
}

/**
 * writeRequestWithKey send a request for operations that has a key as parameter
 * 
 * This function should not be called directly, though it could be used as a general func
 * to request execution of operations with 1 key as parameter if the specific func is missing.
 */
void writeRequestWithKey(void *ctx, streamWriter writer, requestHeader *hdr, byteArray *keyName) {
    uint8_t *buff=(uint8_t *)malloc(hdr->cacheName.len+29+5+keyName->len);
    int len=writeRequestHeader(buff, hdr);
    uint8_t *buff1=buff+len;
    writeBytes(&buff1,keyName->buff,keyName->len);
    len=buff1-buff;
    writer(ctx, buff, len);
    free(buff);
}

void writeGet(void *ctx, streamWriter writer, requestHeader *hdr, byteArray *keyName) {
    hdr->opCode=GET_REQUEST;
    writeRequestWithKey(ctx, writer, hdr, keyName);
}

void readGet(void *ctx, streamReader reader, responseHeader *hdr, byteArray *arr) {
    readResponseHeader(ctx, reader, hdr);
    if (hdr->status == OK_STATUS) {
       arr->len= readBytes(ctx, reader, &arr->buff);
    }
}

    enum  TimeUnit {
        SECONDS = 0x00,
        MILLISECONDS = 0x01,
        NANOSECONDS = 0x02,
        MICROSECONDS = 0x03,
        MINUTES = 0x04,
        DAYS = 0x06,
        HOURS = 0x05,
        DEFAULT = 0x07,
        INFINITUM = 0x08
        };

/**
 * writePut send a request for a put operation
 */
void writePut(void *ctx, streamWriter writer, requestHeader *hdr, byteArray *keyName, byteArray *keyValue) {
    uint8_t *buff=(uint8_t *)malloc(hdr->cacheName.len+29+5+keyName->len+5+keyValue->len);
    hdr->opCode=PUT_REQUEST;
    int len=writeRequestHeader(buff, hdr);
    uint8_t *buff1=buff+len;
    writeBytes(&buff1,keyName->buff,keyName->len);
    writeByte(&buff1,0x88);
    writeBytes(&buff1,keyValue->buff,keyValue->len);
    len=buff1-buff;
    writer(ctx, buff, len);
    free(buff);
}

void readPut(void *ctx, streamReader reader, responseHeader *hdr, byteArray *arr) {
    readResponseHeader(ctx, reader, hdr);
}

