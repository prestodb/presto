


#define MHASH_M  0xc6a4a7935bd1e995L
#define MHASH_R 47

    #define MHASH_STEP_1(h, data) \
{ \
  long __k = data; \
      __k *= MHASH_M;  \
      __k ^= __k >> MHASH_R;  \
      h = __k * MHASH_M; \
    }

    #define MHASH_STEP(h, data) \
{ \
  long __k = data; \
      __k *= MHASH_M;  \
      __k ^= __k >> MHASH_R;  \
      __k *= MHASH_M;  \
      h ^= __k; \
      h *= MHASH_M; \
    }

#define BF_BIT_1(h) (63 & ((h) >> 32))
#define BF_BIT_2(h) (63 & ((h) >> 38))
#define BF_BIT_3(h) (63 & ((h) >> 44))
#define BF_BIT_4(h) (63 & ((h) >> 50))
    #define BF_WORD(h, size) (int)((h & 0x7fffffff) % size)
#define BF_MASK(h) ((1L << BF_BIT_1 (h)) | (1L << BF_BIT_2 (h)) | (1L << BF_BIT_3 (h)) | (1L << BF_BIT_4 (h)))



    #define SLABSIZE (128 * 1024)
#define SLABSHIFT 17
    #define SLABMASK 0x1ffff


    
#ifndef SLICES
#ifdef SLICES
    Not supported;
#else
    #define ALLOC_LONG_ARRAY(size) unsafe.allocateMemory(size * 8) 
        #define FREE_LONG_ARRAY(a) unsafe.freeMemory(a)
            #define DECL_ARRAY(prefix)
        #define LONG_ARRAY long
    #define ARRAY_PREGET(prefix, array, index)
        #define ARRAY_GET(prefix, array, index) unsafe.getLong((array) + 8 * (index))
        #define ARRAY_SET(prefix, array, index, value) unsafe.putLong((array) + 8 * (index), value)


#endif
    #else
        #define ALLOC_LONG_ARRAY(size) new long[size]
            #define FREE_LONG_ARRAY(a)
            #define DECL_ARRAY(prefix)
        #define LONG_ARRAY long[]
    #define ARRAY_PREGET(prefix, array, index)
    #define ARRAY_GET(prefix, array, index) array[index]
            #define ARRAY_SET(prefix, array, index, value) array[index] = value
        #endif
    
    #ifdef SLICES
#define LABEL "With Slices"
        
	#define DECLTABLE(table_in) \
AriaLookupSource table = table_in; \
int statusMask = table.statusMask;              \
Slice[] slices = table.slices;
  

#define DECLTABLE_N(table_in, sub)          \
  AriaLookupSource table##sub = table_in; \
  int statusMask##sub = table##sub.statusMask; \
Slice[] slices##sub = table##sub.slices;

      
#define DECLGET(prefix) Slice prefix##slice; int prefix##offset 

    #define  PREGET(prefix, l)					\
{ prefix##slice = slices[(int)((l) >> SLABSHIFT)]; prefix##offset = (int)(l) & SLABMASK; }

#define  PREGET_N(prefix, l, sub)                                       \
{ prefix##slice = slices##sub[(int)((l) >> SLABSHIFT)]; prefix##offset = (int)(l) & SLABMASK; }


#define GETL(prefix, field)			\
  getLongUnchecked(prefix##slice, prefix##offset + field)

    #define SETL(prefix, field, v)			\
    prefix##slice.setLong(prefix##offset + field, v)

    #define SLICEPARAM(prefix) \
    Slice prefix##slice, int prefix##offset
    
#define SLICEARG(prefix) prefix##slice, prefix##offset

#define SLICEREF(s, o) \
						 (((s) << SLABSHIFT) + (o))
    #else

                                                 #define LABEL "With unmanaged"

                                                 #define DECLTABLE(table_in) \
AriaLookupSource table = table_in; \
int statusMask = table.statusMask;              \
long[] slabs = table.slabs;

#define DECLTABLE_N(table_in, sub)          \
      AriaLookupSource table##sub = table_in; \
      int statusMask##sub = table##sub.statusMask;   \
long[] slabs##sub = table##sub.slabs;

      
                                                 #define DECLGET(prefix) long prefix##ptr
	    #define PREGET(prefix, l)		\
	  prefix##ptr = l

#define PREGET_N(prefix, l, sub)                    \
	  prefix##ptr = l


#define GETL(prefix, field)                     \
    unsafe.getLong(prefix##ptr + field)

						 #define SETL(prefix, field, v) \
    unsafe.putLong(prefix##ptr + field, v)

#define SLICEREF(s, o)                                                 \
                                                 ((slabs[s]) + (o))
                                                 
#define SLICEREF_N(s, o)                                                 \
                                                 ((slabs[s]) + (o))


#endif




		#define DECLPROBE(sub)		\
		    long entry##sub = -1; \
		    long field##sub;		\
    long empty##sub; \
	    long hits##sub; \
	    int hash##sub; \
	    int row##sub; \
	    boolean match##sub = false; \
	DECLGET(g##sub); \
        DECL_ARRAY(st##sub); \
        DECL_ARRAY(ent##sub); 


	    #define PREPROBE(sub) \
DECLTABLE_N(tables[partitions[candidates[currentProbe + sub]]], sub); \
		row##sub = candidates[currentProbe + sub]; \
        tempHash = hashes[row##sub]; \
	hash##sub = (int)tempHash & statusMask##sub;	   \
	    field##sub = (tempHash >> 56) & 0x7f; \
        ARRAY_PREGET(st##sub, table##sub.status, h); \
        hits##sub = ARRAY_GET(st##sub, table##sub.status, hash##sub); \
	    field##sub |= field##sub << 8; \
	    field##sub |= field##sub << 16; \
	    field##sub |= field##sub << 32; 
	

	#define FIRSTPROBE(sub)	      \
            empty##sub = hits##sub & 0x8080808080808080L; \
	    hits##sub ^= field##sub;  \
	hits##sub -= 0x0101010101010101L; \
	hits##sub &= 0x8080808080808080L ^ empty##sub; \
	if (hits##sub != 0) { \
	    int pos = Long.numberOfTrailingZeros(hits##sub) >> 3; \
	    hits##sub &= hits##sub - 1; \
            ARRAY_PREGET(ent##sub, table##sub.table, hash##sub * 8 + pos); \
	    entry##sub = ARRAY_GET(ent##sub, table##sub.table, hash##sub * 8 + pos); \
	    PREGET_N(g##sub, entry##sub, sub);                              \
	    match##sub =GETL(g##sub, 0) == k1d[k1Map[row##sub]] \
		& GETL(g##sub, 8) == k2d[k2Map[row##sub]]; \
	}
	
	    	    #define FULLPROBE(sub) \
    if (match##sub) { \
	if (addResult(entry##sub, currentProbe + sub)) return returnPage; \
    } \
    else { \
	bucketLoop##sub: \
	for (;;) {		 \
	while (hits##sub != 0) { \
	    int pos = Long.numberOfTrailingZeros(hits##sub) >> 3; \
            ARRAY_PREGET(st##sub, table##sub.table, hash##sub * 8 + pos); \
	    entry##sub = ARRAY_GET(ent##sub, table##sub.table, hash##sub * 8 + pos); \
	    PREGET_N(g##sub, entry##sub, sub);                              \
	    if (GETL(g##sub, 0) == k1d[k1Map[row##sub]] && GETL(g##sub, 8) == k2d[k2Map[row##sub]]) { \
		if (addResult(entry##sub, currentProbe + sub)) { \
		    return returnPage; \
		} \
		break bucketLoop##sub;		\
	    } \
	    hits##sub &= hits##sub - 1; \
	} \
	if (empty##sub != 0) break; \
	hash##sub = (hash##sub + 1) & statusMask##sub;	\
        ARRAY_PREGET(st##sub, table##sub.status, hash##sub); \
	hits##sub = ARRAY_GET(st##sub, table##sub.status, hash##sub);        \
        empty##sub = hits##sub & 0x8080808080808080L; \
	hits##sub ^= field##sub;          \
	hits##sub -= 0x0101010101010101L; \
	hits##sub &= 0x8080808080808080L ^ empty##sub; \
    }			  \
}      


