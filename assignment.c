#include <assert.h>
#include <stdio.h>
#include <omp.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h> 

#define NUM_PROCS 4
#define CACHE_SIZE 4
#define MEM_SIZE 16
#define MSG_BUFFER_SIZE 256
#define MAX_INSTR_NUM 32


#define ADDRESS_NODE_SHIFT 8 
#define ADDRESS_NODE_MASK 0xFF
#define ADDRESS_BLOCK_MASK 0xFF
#define BLOCK_SIZE 64 

#define DIR_U 0  // Uncached
#define DIR_S 1  // Shared
#define DIR_EM 2  // Exclusive/Modified

typedef unsigned char byte;

typedef enum { MODIFIED, EXCLUSIVE, SHARED, INVALID } cacheLineState;

typedef enum { EM, S, U } directoryEntryState;

typedef enum { 
    READ_REQUEST,       // requesting node sends to home node on a read miss 
    WRITE_REQUEST,      // requesting node sends to home node on a write miss 
    REPLY_RD,           // home node replies with data to requestor for read request
    REPLY_WR,           // home node replies to requestor for write request
    REPLY_ID,           // home node replies with IDs of sharers to requestor
    INV,                // owner node asks sharers to invalidate
    UPGRADE,            // owner node asks home node to change state to EM
    WRITEBACK_INV,      // home node asks owner node to flush and change to INVALID
    WRITEBACK_INT,      // home node asks owner node to flush and change to SHARED
    FLUSH,              // owner flushes data to home + requestor
    FLUSH_INVACK,       // flush, piggybacking an InvAck message
    EVICT_SHARED,       // handle cache replacement of a shared cache line
    EVICT_MODIFIED      // handle cache replacement of a modified cache line
} transactionType;

typedef struct instruction {
    byte type;      // 'R' for read, 'W' for write
    byte address;
    byte value;     // used only for write operations
} instruction;

typedef struct cacheLine {
    byte address;           // this is the address in memory
    byte value;             // this is the value stored in cached memory
    cacheLineState state;   // state for you to implement MESI protocol
} cacheLine;

typedef struct directoryEntry {
    byte bitVector;         // each bit indicates whether that processor has this
                            // memory block in its cache
    directoryEntryState state;
} directoryEntry;

typedef struct message {
    transactionType type;
    int sender;          // thread id that sent the message
    byte address;        // memory block address
    byte value;          // value in memory / cache
    byte bitVector;      // ids of sharer nodes
    int secondReceiver;  // used when you need to send a message to 2 nodes, where
                         // 1 node id is in the sender field
    directoryEntryState dirState;   // directory entry state of the memory block
} message;

typedef struct messageBuffer {
    message queue[ MSG_BUFFER_SIZE ];
    // a circular queue message buffer
    int head;
    int tail;
    int count;          // store total number of messages processed by the node
} messageBuffer;

typedef struct processorNode {
    cacheLine cache[ CACHE_SIZE ];
    byte memory[ MEM_SIZE ];
    directoryEntry directory[ MEM_SIZE ];
    instruction instructions[ MAX_INSTR_NUM ];
    int instructionCount;
} processorNode;

void initializeProcessor( int threadId, processorNode *node, char *dirName );
void sendMessage( int receiver, message msg );  // IMPLEMENT
void handleCacheReplacement( int sender, cacheLine oldCacheLine );  // IMPLEMENT
void printProcessorState( int processorId, processorNode node );

pthread_mutex_t msgBufferLocks[NUM_PROCS];
pthread_cond_t bufferNotEmptyConditions[NUM_PROCS];

messageBuffer messageBuffers[ NUM_PROCS ];

void setBit(byte *bitVector, int position) {
    *bitVector |= (1 << position);
}

void clearBit(byte *bitVector, int position) {
    *bitVector &= ~(1 << position);
}

int isBitSet(byte bitVector, int position) {
    return (bitVector & (1 << position)) != 0;
}

int findFirstSetBit(byte bitVector) {
    for (int i = 0; i < 8; i++) {
        if (isBitSet(bitVector, i)) return i;
    }
    return -1;  // No bit set
}

int countSetBits(byte bitVector) {
    int count = 0;
    for (int i = 0; i < 8; i++) {
        if (isBitSet(bitVector, i)) count++;
    }
    return count;
}

void clearAllBits(byte *bitVector) {
    *bitVector = 0;
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <test_directory>\n", argv[0]);
        return EXIT_FAILURE;
    }
    char *dirName = argv[1];
    
    // Set number of threads to NUM_PROCS
    omp_set_num_threads(NUM_PROCS);

    for (int i = 0; i < NUM_PROCS; i++) {
        messageBuffers[i].count = 0;
        messageBuffers[i].head = 0;
        messageBuffers[i].tail = 0;
        // Initialize the locks and condition variables
        pthread_mutex_init(&msgBufferLocks[i], NULL);
        pthread_cond_init(&bufferNotEmptyConditions[i], NULL);
    }
    processorNode node;

    // Create the omp parallel region with an appropriate data environment
    #pragma omp parallel private(node)
    {
        int threadId = omp_get_thread_num();
        initializeProcessor(threadId, &node, dirName);
        // Wait for all processors to complete initialization before proceeding
        #pragma omp barrier

        message msg;
        message msgReply;
        instruction instr;
        int instructionIdx = -1;
        int printProcState = 1;         // control how many times to dump processor
        byte waitingForReply = 0;       // if a processor is waiting for a reply
                                        // then dont proceed to next instruction
                                        // as current instruction has not finished
       while (1) {
            // Process all messages in message queue first
            while (
                messageBuffers[threadId].count > 0 &&
                messageBuffers[threadId].head != messageBuffers[threadId].tail
            ) {
                if (printProcState == 0) {
                    printProcState++;
                }
                int head = messageBuffers[threadId].head;
                msg = messageBuffers[threadId].queue[head];
                messageBuffers[threadId].head = (head + 1) % MSG_BUFFER_SIZE;

                #ifdef DEBUG_MSG
                printf("Processor %d msg from: %d, type: %d, address: 0x%02X\n",
                        threadId, msg.sender, msg.type, msg.address);
                #endif /* ifdef DEBUG */

                byte procNodeAddr = (msg.address >> ADDRESS_NODE_SHIFT) & ADDRESS_NODE_MASK;
                byte memBlockAddr = msg.address & ADDRESS_BLOCK_MASK;
                byte cacheIndex = memBlockAddr % CACHE_SIZE;

                switch (msg.type) {
                    case READ_REQUEST: {
                        directoryEntry *dir = &node.directory[memBlockAddr];
                        
                        if (dir->state == DIR_U) {
                            dir->state = DIR_S;
                            setBit(&dir->bitVector, msg.sender);
                            
                            msgReply.type = REPLY_RD;
                            msgReply.sender = threadId;
                            msgReply.address = msg.address;
                            msgReply.value = node.memory[memBlockAddr];
                            sendMessage(msg.sender, msgReply);
                        } 
                        else if (dir->state == DIR_S) {
                            setBit(&dir->bitVector, msg.sender);

                            msgReply.type = REPLY_RD;
                            msgReply.sender = threadId;
                            msgReply.address = msg.address;
                            msgReply.value = node.memory[memBlockAddr];

                            sendMessage(msg.sender, msgReply);
                        }
                        else if (dir->state == DIR_EM) {
                            int ownerNode = findFirstSetBit(dir->bitVector);
                            
                            msgReply.type = WRITEBACK_INT;
                            msgReply.sender = threadId;
                            msgReply.address = msg.address;
                            msgReply.secondReceiver = msg.sender; 
                            sendMessage(ownerNode, msgReply);
                        }
                        break;
                    }

                    case REPLY_RD: {

                        cacheLine *cacheLine = &node.cache[cacheIndex];
                        
                        if (cacheLine->state != INVALID && (cacheLine->address != msg.address)) {
                            handleCacheReplacement(threadId, *cacheLine);
                        }

                        cacheLine->address = msg.address;
                        cacheLine->state = SHARED;

                        cacheLine->value = msg.value;

                        waitingForReply = 0;
                        break;
                    }

                    case WRITEBACK_INT: {
                        cacheLine *cacheLine = &node.cache[cacheIndex];

                        msgReply.type = FLUSH;
                        msgReply.sender = threadId;
                        msgReply.address = msg.address;
                        msgReply.value = cacheline->value;

                        sendMessage(procNodeAddr, msgReply);

                        if (msg.secondReceiver != procNodeAddr) {
                            sendMessage(msg.secondReceiver, msgReply);
                        }

                        cacheLine->state = SHARED;
                        break;
                    }

                    case FLUSH: {
                        if (threadId == procNodeAddr) {
                            directoryEntry *dir = &node.directory[memBlockAddr];

                            node.memory[memBlockAddr] = msg.value;

                            if (dir->state == DIR_EM) {
                                dir->state = DIR_S;

                                setBit(&dir->bitVector, msg.sender);
                                setBit(&dir->bitVector, msg.secondReceiver);
                            }
                        } 
                        else if (threadId == msg.secondReceiver) {
                            cacheLine *cacheLine = &node.cache[cacheIndex];
                            
                            if (cacheLine->state != INVALID && (cacheLine->address != msg.address)) {
                                handleCacheReplacement(threadId, *cacheLine);
                            }

                            cacheLine->address = msg.address;
                            cacheLine->state = SHARED;

                            cacheLine->value = msg.value;

                            waitingForReply = 0;
                        }
                        break;
                    }

                    case UPGRADE: {
                        directoryEntry *dir = &node.directory[memBlockAddr];

                        byte sharers[NUM_PROCS];
                        int sharerCount = 0;

                        for (int i = 0; i < NUM_PROCS; i++) {
                            if (i != msg.sender && isBitSet(dir->bitVector, i)) {
                                sharers[sharerCount++] = i;
                            }
                        }

                        dir->state = DIR_EM;
                        clearAllBits(&dir->bitVector);
                        setBit(&dir->bitVector, msg.sender);

                        msgReply.type = REPLY_ID;
                        msgReply.sender = threadId;
                        msgReply.address = msg.address;

                        memcpy(msgReply.value, sharers, sharerCount);
                        
                        sendMessage(msg.sender, msgReply);
                        break;
                    }

                    case REPLY_ID: {
                        cacheLine *cacheLine = &node.cache[cacheIndex];

                        for (int i = 0; i < msg.dataSize; i++) {
                            byte sharer = msg.value[i];
                            
                            msgReply.type = INV;
                            msgReply.sender = threadId;
                            msgReply.address = msg.address;
                            
                            sendMessage(sharer, msgReply);
                        }

                        cacheLine->state = MODIFIED;

                        waitingForReply = 0;
                        break;
                    }

                    case INV: {
                        cacheLine *cacheLine = &node.cache[cacheIndex];

                        if (cacheLine->state != INVALID && cacheLine->address == msg.address) {
                            cacheLine->state = INVALID;
                        }
                        break;
                    }

                    case WRITE_REQUEST: {
                        directoryEntry *dir = &node.directory[memBlockAddr];
                        
                        if (dir->state == DIR_U) {
                            dir->state = DIR_EM;
                            clearAllBits(&dir->bitVector);
                            setBit(&dir->bitVector, msg.sender);
                            
                            msgReply.type = REPLY_WR;
                            msgReply.sender = threadId;
                            msgReply.address = msg.address;
                            msgReply.value = node.memory[memBlockAddr];
                            sendMessage(msg.sender, msgReply);
                        }
                        else if (dir->state == DIR_S) {
                            byte sharers[NUM_PROCS];
                            int sharerCount = 0;

                            for (int i = 0; i < NUM_PROCS; i++) {
                                if (i != msg.sender && isBitSet(dir->bitVector, i)) {
                                    sharers[sharerCount++] = i;
                                }
                            }

                            dir->state = DIR_EM;
                            clearAllBits(&dir->bitVector);
                            setBit(&dir->bitVector, msg.sender);

                            msgReply.type = REPLY_ID;
                            msgReply.sender = threadId;
                            msgReply.address = msg.address;

                            memcpy(msgReply.value, sharers, sharerCount);
                            msgReply.value = node.memory[memBlockAddr];
                            
                            sendMessage(msg.sender, msgReply);
                        }
                        else if (dir->state == DIR_EM) {
                            int ownerNode = findFirstSetBit(dir->bitVector);
                            
                            msgReply.type = WRITEBACK_INV;
                            msgReply.sender = threadId;
                            msgReply.address = msg.address;
                            msgReply.secondReceiver = msg.sender;
                            
                            sendMessage(ownerNode, msgReply);
                        }
                        break;
                    }

                    case REPLY_WR: {
                        cacheLine *cacheLine = &node.cache[cacheIndex];
                        
                        if (cacheLine->state != INVALID && (cacheLine->address != msg.address)) {
                            handleCacheReplacement(threadId, *cacheLine);
                        }

                        cacheLine->address = msg.address;
                        cacheLine->state = MODIFIED;

                        cacheLine->value = msg.value;

                        waitingForReply = 0;
                        break;
                    }

                    case WRITEBACK_INV: {
                        cacheLine *cacheLine = &node.cache[cacheIndex];

                        msgReply.type = FLUSH_INVACK;
                        msgReply.sender = threadId;
                        msgReply.address = msg.address;
                        msgReply.value = cacheline->value;
                        
                        sendMessage(procNodeAddr, msgReply);

                        if (msg.secondReceiver != procNodeAddr) {
                            sendMessage(msg.secondReceiver, msgReply);
                        }
                        cacheLine->state = INVALID;
                        break;
                    }

                    case FLUSH_INVACK: {
                        if (threadId == procNodeAddr) {
                            directoryEntry *dir = &node.directory[memBlockAddr];

                            node.memory[memBlockAddr] = msg.value;

                            dir->state = DIR_EM;
                            clearAllBits(&dir->bitVector);
                            setBit(&dir->bitVector, msg.secondReceiver);
                        }
                        else if (threadId == msg.secondReceiver) {
                            cacheLine *cacheLine = &node.cache[cacheIndex];
                            
                            if (cacheLine->state != INVALID && (cacheLine->address != msg.address)) {
                                handleCacheReplacement(threadId, *cacheLine);
                            }

                            cacheLine->address = msg.address;
                            cacheLine->state = MODIFIED;

                            cacheLine->value = msg.value;

                            waitingForReply = 0;
                        }
                        break;
                    }
                    
                    case EVICT_SHARED: {
                        if (threadId == procNodeAddr) {
                            directoryEntry *dir = &node.directory[memBlockAddr];

                            clearBit(&dir->bitVector, msg.sender);
                            
                            int remainingSharers = countSetBits(dir->bitVector);
                            
                            if (remainingSharers == 0) {
                                dir->state = DIR_U;
                            }
                            else if (remainingSharers == 1) {
                                dir->state = DIR_EM;

                                int remainingSharer = findFirstSetBit(dir->bitVector);

                                msgReply.type = EVICT_SHARED;
                                msgReply.sender = threadId;
                                msgReply.address = msg.address;
                                
                                sendMessage(remainingSharer, msgReply);
                            }
                        }
                        else {
                            cacheLine *cacheLine = &node.cache[cacheIndex];

                            if (cacheLine->state != INVALID && (cacheLine->address != msg.address)) {
                                cacheLine->state = EXCLUSIVE;
                            }
                        }
                        break;
                    }

                    case EVICT_MODIFIED: {
                        directoryEntry *dir = &node.directory[memBlockAddr];

                        node.memory[memBlockAddr] = msg.value;

                        dir->state = DIR_U;
                        clearAllBits(&dir->bitVector);
                        break;
                    }
                }
            messageBuffers[threadId].count--;
            }

            if (waitingForReply > 0) {
                continue;
            }

            if (instructionIdx < node.instructionCount - 1) {
                instructionIdx++;
            } else {
                if (printProcState > 0) {
                    printProcessorState(threadId, node);
                    printProcState--;
                }
                continue;
            }
            instr = node.instructions[instructionIdx];

            #ifdef DEBUG_INSTR
            printf("Processor %d: instr type=%c, address=0x%02X, value=%hhu\n",
                    threadId, instr.type, instr.address, instr.value);
            #endif

            byte procNodeAddr = (instr.address >> ADDRESS_NODE_SHIFT) & ADDRESS_NODE_MASK;
            byte memBlockAddr = instr.address & ADDRESS_BLOCK_MASK;
            byte cacheIndex = memBlockAddr % CACHE_SIZE;

            if (instr.type == 'R') {

                cacheLine *cacheLine = &node.cache[cacheIndex];

                if (cacheLine->state != INVALID && cacheLine->address == instr.address) {

                } 
                else {

                    msg.type = READ_REQUEST;
                    msg.sender = threadId;
                    msg.address = instr.address;
                    
                    sendMessage(procNodeAddr, msg);
                    waitingForReply = 1;
                }
            } 
            else {
                cacheLine *cacheLine = &node.cache[cacheIndex];

                if (cacheLine->address == instr.address && 
                    cacheLine->state != INVALID) {

                    if (cacheLine->state == MODIFIED || cacheLine->state == EXCLUSIVE) {
                        byte offset = 0; 
                        cacheline->value[offset] = instr.value;

                        cacheLine->state = MODIFIED;
                    } 
                    else if (cacheLine->state == SHARED) {
                        msg.type = UPGRADE;
                        msg.sender = threadId;
                        msg.address = instr.address;
                        
                        sendMessage(procNodeAddr, msg);
                        waitingForReply = 1;
                    }
                } 
                else {

                    msg.type = WRITE_REQUEST;
                    msg.sender = threadId;
                    msg.address = instr.address;
                    
                    sendMessage(procNodeAddr, msg);
                    waitingForReply = 1;
                }
            }
        }
    }
}

void sendMessage(int receiver, message msg) {
    pthread_mutex_lock(&msgBufferLocks[receiver]);

    if ((messageBuffers[receiver].tail + 1) % MSG_BUFFER_SIZE == messageBuffers[receiver].head) {

        pthread_mutex_unlock(&msgBufferLocks[receiver]);
        usleep(100);
        sendMessage(receiver, msg); 
        return;
    }

    messageBuffers[receiver].queue[messageBuffers[receiver].tail] = msg;

    messageBuffers[receiver].tail = (messageBuffers[receiver].tail + 1) % MSG_BUFFER_SIZE;

    messageBuffers[receiver].count++;

    pthread_cond_signal(&bufferNotEmptyConditions[receiver]);

    pthread_mutex_unlock(&msgBufferLocks[receiver]);
}
void handleCacheReplacement(int sender, cacheLine oldCacheLine) {

    byte memBlockAddr = oldCacheLine.address & ADDRESS_BLOCK_MASK;
    byte procNodeAddr = (oldCacheLine.address >> ADDRESS_NODE_SHIFT) & ADDRESS_NODE_MASK;
    
    message msg;
    
    switch (oldCacheLine.state) {
        case EXCLUSIVE:
        case SHARED:
            msg.type = EVICT_SHARED;
            msg.sender = sender;
            msg.address = oldCacheLine.address;
            sendMessage(procNodeAddr, msg);
            break;
        case MODIFIED:
            msg.type = EVICT_MODIFIED; 
            msg.sender = sender;
            msg.address = oldCacheLine.address;
            msg.value = oldCacheLine.value;  
            sendMessage(procNodeAddr, msg);
            break;
        case INVALID:
            break;
    }
}
void initializeProcessor( int threadId, processorNode *node, char *dirName ) {
    // IMPORTANT: DO NOT MODIFY
    for ( int i = 0; i < MEM_SIZE; i++ ) {
        node->memory[ i ] = 20 * threadId + i;  // some initial value to mem block
        node->directory[ i ].bitVector = 0;     // no cache has this block at start
        node->directory[ i ].state = U;         // this block is in Unowned state
    }

    for ( int i = 0; i < CACHE_SIZE; i++ ) {
        node->cache[ i ].address = 0xFF;        // this address is invalid as we can
                                                // have a maximum of 8 nodes in the 
                                                // current implementation
        node->cache[ i ].value = 0;
        node->cache[ i ].state = INVALID;       // all cache lines are invalid
    }

    // read and parse instructions from core_<threadId>.txt
    char filename[ 128 ];
    snprintf(filename, sizeof(filename), "tests/%s/core_%d.txt", dirName, threadId);
    FILE *file = fopen( filename, "r" );
    if ( !file ) {
        fprintf( stderr, "Error: count not open file %s\n", filename );
        exit( EXIT_FAILURE );
    }

    char line[ 20 ];
    node->instructionCount = 0;
    while ( fgets( line, sizeof( line ), file ) &&
            node->instructionCount < MAX_INSTR_NUM ) {
        if ( line[ 0 ] == 'R' && line[ 1 ] == 'D' ) {
            sscanf( line, "RD %hhx",
                    &node->instructions[ node->instructionCount ].address );
            node->instructions[ node->instructionCount ].type = 'R';
            node->instructions[ node->instructionCount ].value = 0;
        } else if ( line[ 0 ] == 'W' && line[ 1 ] == 'R' ) {
            sscanf( line, "WR %hhx %hhu",
                    &node->instructions[ node->instructionCount ].address,
                    &node->instructions[ node->instructionCount ].value );
            node->instructions[ node->instructionCount ].type = 'W';
        }
        node->instructionCount++;
    }

    fclose( file );
    printf( "Processor %d initialized\n", threadId );
}

void printProcessorState(int processorId, processorNode node) {
    // IMPORTANT: DO NOT MODIFY
    static const char *cacheStateStr[] = { "MODIFIED", "EXCLUSIVE", "SHARED",
                                           "INVALID" };
    static const char *dirStateStr[] = { "EM", "S", "U" };

    char filename[32];
    snprintf(filename, sizeof(filename), "core_%d_output.txt", processorId);

    FILE *file = fopen(filename, "w");
    if (!file) {
        printf("Error: Could not open file %s\n", filename);
        return;
    }

    fprintf(file, "=======================================\n");
    fprintf(file, " Processor Node: %d\n", processorId);
    fprintf(file, "=======================================\n\n");

    // Print memory state
    fprintf(file, "-------- Memory State --------\n");
    fprintf(file, "| Index | Address |   Value  |\n");
    fprintf(file, "|----------------------------|\n");
    for (int i = 0; i < MEM_SIZE; i++) {
        fprintf(file, "|  %3d  |  0x%02X   |  %5d   |\n", i, (processorId << 4) + i,
                node.memory[i]);
    }
    fprintf(file, "------------------------------\n\n");

    // Print directory state
    fprintf(file, "------------ Directory State ---------------\n");
    fprintf(file, "| Index | Address | State |    BitVector   |\n");
    fprintf(file, "|------------------------------------------|\n");
    for (int i = 0; i < MEM_SIZE; i++) {
        fprintf(file, "|  %3d  |  0x%02X   |  %2s   |   0x%08B   |\n",
                i, (processorId << 4) + i, dirStateStr[node.directory[i].state],
                node.directory[i].bitVector);
    }
    fprintf(file, "--------------------------------------------\n\n");
    
    // Print cache state
    fprintf(file, "------------ Cache State ----------------\n");
    fprintf(file, "| Index | Address | Value |    State    |\n");
    fprintf(file, "|---------------------------------------|\n");
    for (int i = 0; i < CACHE_SIZE; i++) {
        fprintf(file, "|  %3d  |  0x%02X   |  %3d  |  %8s \t|\n",
               i, node.cache[i].address, node.cache[i].value,
               cacheStateStr[node.cache[i].state]);
    }
    fprintf(file, "----------------------------------------\n\n");

    fclose(file);
} 
