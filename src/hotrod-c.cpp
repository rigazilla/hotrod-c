#include <iostream>
#include <string.h>
#include <hotrod-c.h>

/** @file */ 

/**
 * Read 1 byte from the stream
 */
uint8_t readByte(void* ctx, streamReader reader) {
    uint8_t val;
    reader(ctx, &val, 1);
    return val;
}

/**
 * Read 1 short from the stream, high byte first
 */
uint16_t readShort(void* ctx, streamReader reader) {
    uint16_t val;
    uint8_t b;
    reader(ctx, &b, 1);
    val = b<<8;
    reader(ctx, &b, 1);
    val += b;
    return val;
}

/**
 * write 1 byte to the buffer
 */
void writeByte(uint8_t **buff, uint8_t val) {
    **buff=val;
    ++*buff;
}

/**
 * write 1 short from the buffer, high byte first
 */
void writeShort(uint8_t **buff, uint16_t val) {
    **buff=(uint8_t)(val>>8);
    ++*buff;
    **buff=(uint8_t)(val & 0xff);
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
 * Write an unsigned int to the buffer
 * 
 * the value is represented in the buffer as a sequence of bytes
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
 * Write an unsigned long as vlong to the bytes buffer
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
 * Read an bytes array of variable length from the stream
 * 
 * An array is composed by two fields:
 * - array length as vInt readVInt()
 * - array content as bytes
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
 * An array is composed by two fields:
 * - array length as vInt writeVInt()
 * - array content as bytes
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
 * \defgroup RequestOpCode Operation code for request
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

/**
 * \defgroup ResponseOpCode Operation code for response
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
 *  readNewTopolgy read a new topology description from the stream
 *  
 * Hotrod topology description. If the cluster topology is changed a new
 * topology description will be attached to all the response header until
 * the topology id sent in the request will match the new topology id.
 * 
 * Field | Size (bytes) or type | Comment | References
 * ------|----------------------|---------|------------
 * TopologyId | vInt | Topology id | readVInt() |
 * Servers Num | vInt | number of nodes in the cluster| |
 *  loop 1| | repeated ServerNum times| |
 * Server Addr | array | server address | readBytes() |
 * Server Port | 2 (short)| | |
 *  end loop 1| | | |
 * Hash Func Num | 1 | hash function number id (usually 0x03) | |
 * Segments Num | vInt | number of segments in the topology | |
 * loop 2 | | repeated ServerNum times| |
 * Owners Num | 1 | number of owners per segment N | |
 * loop 3 | | next field is repeated OwnersNum times| |
 * Owner | vInt | server owner for this seg | |
 * end loop 3| | | |
 * end loop 2| | | |
 */
void readNewTopology(void *ctx, streamReader reader, responseHeader *hdr, const requestHeader* const reqHdr, topologyInfo *tInfo) {
    tInfo->topologyId = readVInt(ctx, reader);
    tInfo->serversNum = readVInt(ctx, reader); // Number of servers
    tInfo->servers = (byteArray*)malloc(sizeof(byteArray)*tInfo->serversNum);
    tInfo->ports = (uint16_t*)malloc(sizeof(uint16_t)*tInfo->serversNum);
    for (int i=0; i< tInfo->serversNum; i++) { // Loop reading servers
        tInfo->servers[i].len=readBytes(ctx, reader, &tInfo->servers[i].buff);
        tInfo->ports[i]=readShort(ctx, reader);
    }
    if (reqHdr->clientIntelligence==CLIENT_INTELLIGENCE_HASH_DISTRIBUTION_AWARE) {
        tInfo->hashFuncNum = readByte(ctx, reader); // This should always read 0x03
        if (tInfo->hashFuncNum>0) {
            tInfo->segmentsNum = readVInt(ctx, reader); // Number of segments
            // Allocate and array of int8 for the number of owners for each segment
            tInfo->ownersNumPerSegment = (uint8_t*)malloc(sizeof(uint8_t)*tInfo->segmentsNum);
            // Allocate and array of struct for owners, one struct for each segment
            tInfo->ownersPerSegment = (uint32_t**)malloc(sizeof(uint32_t*)*tInfo->segmentsNum);
            for (int i=0; i<tInfo->segmentsNum; i++) { // for each segment
                tInfo->ownersNumPerSegment[i] = readByte(ctx, reader); // read the # of owners
                tInfo->ownersPerSegment[i] = (uint32_t*)malloc(sizeof(uint32_t)*tInfo->ownersNumPerSegment[i]);
                for (int j=0; j<tInfo->ownersNumPerSegment[i]; j++) { // read all the owner for this segment
                    tInfo->ownersPerSegment[i][j] = readVInt(ctx, reader); 
                }
            }
        }
    }
}

/**
 *  readResponseHeader read a response header from the bytes stream
 *  
 * Hotrod 3.0 response header description
 * 
 * Field | Size (bytes) or type | Comment | References
 * ------|----------------------|---------|------------
 * Magic | 1 | Valid value is 0xA1 | |
 * Message ID | vLong | | readVLong() |
 * Operation Code | 1 | response opcode | @ref ResponseOpcode |
 * Status Code | 1 | status code | @ref ErrorResponseCode |
 * Error Message | array | optional | readResponseError() , readBytes() | 
 */
void readResponseHeader(void *ctx, streamReader reader, responseHeader *hdr, const requestHeader* const reqHdr, topologyInfo *tInfo) {
    hdr->magic = readByte(ctx, reader);
    hdr->messageId = readVLong(ctx, reader);
    hdr->opCode = readByte(ctx, reader);
    hdr->status = readByte(ctx, reader);
    hdr->topologyChanged = readByte(ctx, reader);
    if (hdr->topologyChanged) {
        readNewTopology(ctx, reader, hdr, reqHdr, tInfo);
    }
    uint8_t *errMsg;
    hdr->error.len= readResponseError(ctx, reader, hdr->status, &errMsg);
    // Following cast is true when sizeof(char)==8
    hdr->error.buff= errMsg;
    // TODO implement here topology changes
}


/**
 *  readMediaType read a mediaType from the stream
 *  
 * Field | Size (bytes) or type | Comment | References
 * ------|----------------------|---------|------------
 * infoType | 1 | type of the info | |
 * infoType == 0 stop | | | |
 * infoType == 1 | | | |
 * predefined mediaType | vInt | the id of a well know mediaType (TODO provide table)| |
 * infoType == 2| | repeated ServerNum times| |
 * MediaType name | array | name of the mediaType | readBytes() |
 * paramsNum | vInt | numeber of parameters for this mediaType | |
 * loop 1 | | repeated paramsNum times| |
 * param i key | array | key of the i-th param | |
 * param i value | array | value of the i-th param |  |
 *  end loop 1| | | |
 */
void readMediaType(void *ctx, streamReader reader, mediaType *mt) {
    mt->infoType = readByte(ctx, reader);
    switch (mt->infoType) {
        case 0:
        break;
        case 1:
            mt->predefinedMediaType = readVInt(ctx,reader);
        break;
        case 2:
            mt->customMediaType.len = readBytes(ctx, reader, &mt->customMediaType.buff);
            mt->paramsNum = readVInt(ctx,reader);
            mt->keys = (byteArray*)(sizeof(byteArray)*mt->paramsNum);
            mt->values = (byteArray*)(sizeof(byteArray)*mt->paramsNum);
            for (int i=0; i<mt->paramsNum; i++) {
                mt->keys[i].len = readBytes(ctx, reader, &mt->keys[i].buff);
                mt->values[i].len = readBytes(ctx, reader, &mt->values[i].buff);
            }
        break;
    }
}

void writeMediaType(uint8_t **buff, const mediaType *const mt) {
    switch (mt->infoType) {
        case 0:
            writeByte(buff, 0x00);
        break;
        case 1:
            writeVInt(buff, mt->predefinedMediaType);
        break;
        case 2:
            writeBytes(buff, mt->customMediaType.buff, mt->customMediaType.len);
            writeVInt(buff, mt->paramsNum);
            for (int i=0; i<mt->paramsNum; i++) {
                writeBytes(buff, mt->keys[i].buff, mt->keys[i].len);
                writeBytes(buff, mt->values[i].buff, mt->values[i].len);
            }
        break;
    }
}

/**
 *  writeRequestHeader populates and header a 2.8 hotrod response
 *  
 * Hotrod 3.0 response header description
 * 
 * Field | Size (bytes) or type | Comment | References
 * ------|----------------------|---------|------------
 * Magic | 1 | Valid value is 0xA0 | |
 * Message ID | vLong | | readVLong() |
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
    writeMediaType(&curs, &hdr->keyMediaType);
    writeMediaType(&curs, &hdr->valueMediaType);
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

void readGet(void *ctx, streamReader reader, responseHeader *hdr, requestHeader *reqHdr, topologyInfo *tInfo, byteArray *arr) {
    readResponseHeader(ctx, reader, hdr, reqHdr, tInfo);
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

void readPut(void *ctx, streamReader reader, responseHeader *hdr, requestHeader *reqHdr, topologyInfo *tInfo, byteArray *arr) {
    readResponseHeader(ctx, reader, hdr, reqHdr, tInfo);
}

/**
 * writePing send a request for a ping operation
 */
void writePing(void *ctx, streamWriter writer, requestHeader *hdr) {
    uint8_t *buff=(uint8_t *)malloc(hdr->cacheName.len+29);
    hdr->opCode=PING_REQUEST;
    int len=writeRequestHeader(buff, hdr);
    writer(ctx, buff, len);
    free(buff);
}

/**
 * readPing ping operation result
 */
void readPing(void *ctx, streamReader reader, responseHeader *hdr, requestHeader *reqHdr, topologyInfo *tInfo, mediaType *keyMt, mediaType *valueMt) {
    uint8_t version;
    uint32_t operationsNum;
    uint16_t *operations;
    readResponseHeader(ctx, reader, hdr, reqHdr, tInfo);
    readMediaType(ctx, reader, keyMt);
    readMediaType(ctx, reader, valueMt);
    version = readByte(ctx, reader);
    operationsNum = readVInt(ctx, reader);
    operations = (uint16_t*)malloc(sizeof(uint16_t)*operationsNum);
    for (int i=0; i<operationsNum; i++) {
        operations[i]= readShort(ctx, reader);
    }
}

