/* 
 * Bare-C io-scheduled coroutines: based on ucontext libc support.
 *
 * Authors: 
 *      Alexander Sabourenkov
 *
 * License: MIT License
 *
 */
 
#ifndef COEV_H
#define COEV_H
#ifdef __cplusplus
extern "C" {
#endif
#ifdef THREADING_MADNESS
#include <pthread.h>
#endif
#include <stdint.h>
#include <unistd.h>
#include <ucontext.h>

#include <ev.h>

/* coev_t::state */
#define CSTATE_ZERO          0 /* not initialized */
#define CSTATE_CURRENT       1 /* currently executing */
#define CSTATE_RUNNABLE      2 /* switched out voluntarily */
#define CSTATE_SCHEDULED     3 /* in runqueue */
#define CSTATE_IOWAIT        4 /* waiting on an fd */
#define CSTATE_SLEEP         5 /* sleeping */
#define CSTATE_LOCKWAIT      6 /* waiting on a lock */
#define CSTATE_DEAD          7 /* dead */

/* coev_t::status */
#define CSW_NONE             0 /* there was no switch */
#define CSW_VOLUNTARY        1 /* explicit switch, not from the scheduler */
#define CSW_EVENT            2 /* io-event fired */
#define CSW_WAKEUP           3 /* sleep elapsed */
#define CSW_TIMEOUT          4 /* io-event timed out */
#define CSW_YOURTURN         5 /* explicity scheduled switch */
#define CSW_SIGCHLD          6 /* child died */

/* below are immediate (no actual switch) error return values */
#define CSW_LESS_THAN_AN_ERROR   9 /* used to distinguish errors, never actually returned. */
#define CSW_SCHEDULER_NEEDED    10 /* wait, sleep or stall w/o scheduler: please run one */
#define CSW_TARGET_SELF         11 /* switch to self attempted. */
#define CSW_TARGET_DEAD         12 /* switch to/scheduling of a dead coroutine attempted  */
#define CSW_TARGET_BUSY         13 /* switch to/scheduling of a coroutine with active event watcher attempted */


#define CSW_ERROR(c) ( (c)->status  > CSW_LESS_THAN_AN_ERROR )

/* This is preallocated size for (T)CLS storage.
   that much pointers can be stored per coroutine
   without separate memory allocation. 
   Takes up 512 bytes on x86, 1k on x86-64. */
#define CLS_KEYCHAIN_SIZE 63

/* This is preallocated size for lock storage:
   number of locks. */
#ifndef COLOCK_PREALLOCATE
#define COLOCK_PREALLOCATE 64
#endif


typedef struct _coev coev_t;
typedef void (*coev_runner_t)(coev_t *);
typedef struct _coev_lock colock_t;

typedef struct _key_tuple {
    long key;
    void *value;
} cokey_t;

typedef struct _key_chain cokeychain_t;
struct _key_chain {
    cokeychain_t *next;   
    cokey_t keys[CLS_KEYCHAIN_SIZE]; 
};

typedef struct _coev_stack coevst_t;

struct _coev {
    ucontext_t ctx;         /* the context */
    coevst_t *stack;        /* to free it fast*/
    unsigned int id;        /* serial, to build debug representations / show tree position */
    
    coev_t *parent;         /* report death here */

    coev_t *origin;         /* switched from here last time */
    int state;              /* CSTATE_* -- state of this coroutine */
    int status;             /* CSW_*  -- status of last switch into this coroutine */
    char *treepos;          /* position in the tree */
    int treepos_is_stale;   /* if it needs to be updated */
    unsigned int child_count; /* internal refcount */
    
    coev_runner_t run;      /* entry point into the coroutine, NULL if the coro has already started. */
    
    struct ev_io watcher;        /* IO watcher */
    struct ev_timer io_timer;    /* IO timeout timer. */
    struct ev_timer sleep_timer; /* sleep timer */
    
    coev_t *rq_next;        /* runqueue list pointer */
    coev_t *cb_next;        /* allocator internals */
    coev_t *cb_prev;        /* allocator internals */
    
    coev_t *lq_next;        /* lock waiting queue */
    coev_t *lq_prev;        /* lock waiting queue */
    
    cokeychain_t kc;        /* CLS keychain */
    cokeychain_t *kc_tail;  /* CLS meta-keychain tail (if it was ever extended) */
       
    void *A, *X, *Y, *S;    /* user-used stuff so that they don't need to fiddle with offsetof (6502 ftw) */
    
#ifdef THREADING_MADNESS
    pthread_t thread;
#endif    
};



/*** METHOD OF OPERATION
    
    The run function pointer points to the function that would be 
    executed when a coroutine is first switch()-ed into.

    Prototype:
        void *run(coev_t *self, void *p);

    Parameters:
        self - pointer to this coroutine just in case 
        p - parameter given to the initial coev_switch().

    The coroutine can and should call coev_switch() inside the run()
    function in order to switch to some other coroutine. 

    ISTHISSO? If the coroutine that is being switch()-ed into was never 
    run, it will be assigned as a child.
    
    Return value of this function can be
     - a parameter passed by some other coroutine calling coev_switch()
     - return value of run() function of a child coroutine 
       that happened to die.
    
    To facilitate distinction between these two cases, an optional hook is provided,
    which will be called in context of dying coroutine, just after run() returns.
    It will be supplied run() return value, and must return a value of same type.
    

    IO SCHEDULER

    A coroutine also can and should call coev_wait() function. This function
    will set up a wait for given event and then switch to the io-scheduler
    coroutine. 
    
    A coroutine in a wait state can not be switch()-ed into. 
    
    The io-scheduler coroutine is any regular coroutine, blessed with a
    call of coev_set_scheduler(), so that it is known where to switch
    upon colo_wait() calls.
    
    Such coroutine's run() function is expected to call coev_ev_loop() to listen
    for events and dispatch by switching. 
    
    In the absence of designated scheduler coroutine, switches are perfomed to 'root'
    coroutine (created at the time of coev_initialize())
    
    THREAD SAFETY
    
    In order to be safe, do not use threads.
    Switches are possible only in context of a single thread. 
    Failure to observe that will be reported with crossthread_fail callback.
    
*/
typedef struct _coev_instrumentation {
    /* counters */
    volatile uint64_t c_ctxswaps;
    
    volatile uint64_t c_switches;
    volatile uint64_t c_waits;
    volatile uint64_t c_sleeps;
    volatile uint64_t c_stalls;
    
    volatile uint64_t c_runqruns;
    volatile uint64_t c_news;
    
    volatile uint64_t c_lock_acquires;
    volatile uint64_t c_lock_acfails;
    volatile uint64_t c_lock_waits;
    volatile uint64_t c_lock_releases;
    
    /* gauges */
    volatile uint64_t stacks_allocated;
    volatile uint64_t stacks_used;
    volatile uint64_t cnrbufs_allocated;
    volatile uint64_t cnrbufs_used;
    volatile uint64_t coevs_allocated;
    volatile uint64_t coevs_on_lock;
    volatile uint64_t coevs_used;
    volatile uint64_t colocks_allocated;
    volatile uint64_t colocks_used;
    
    
    volatile uint64_t waiters;
    volatile uint64_t slackers;
} coev_instrumentation_t;

/* memory management + error reporting to use */
typedef struct _coev_framework_methods {
    /* memory management */
    void *(*malloc)(size_t);
    void *(*realloc)(void *, size_t); 
    void  (*free)(void *);
    
    /* total failure: must not return. */
    void (*abort)(const char *);
    
    /* same, but look at errno. */
    void (*eabort)(const char *, int);

    /* handle SIGINT (or NULL to ignore) */
    void (*inthdlr)(void);
    
    /* debug output collector */
    size_t dm_size;
    void (*dm_flush)(const char *start, size_t length);
    
    /* debug output bitmask*/
    int debug;
    
    /* instrumentation */
    coev_instrumentation_t i;
    
} coev_frameth_t;

void coev_libinit(const coev_frameth_t *fm, coev_t *root);
void coev_libfini(void);

/* call this immediately after a fork. */
void coev_fork_notify(void);

coev_t *coev_new(coev_runner_t runner, size_t stacksize);

coev_t *coev_current(void);
/* returns 0 on success or -1 if a cycle would result */
int coev_setparent(coev_t *target, coev_t *newparent);

/* return tree traverse from root to given greenlet. 
   memory is not to be touched by caller */
const char *coev_treepos(coev_t *coio);
const char *coev_state(coev_t *);
const char *coev_status(coev_t *);

/* explicitly switch to the target */
void coev_switch(coev_t *target);

/* coev_wait()'s revents bits*/
#define COEV_READ       EV_READ
#define COEV_WRITE      EV_WRITE

/* wait for an IO event, switch back to caller's coroutine when event fires, 
   event times out or voluntary switch occurs. 
   if ((fd == -1) && (revents == 0)), sleep for the specified time. */
void coev_wait(int fd, int revents, ev_tstamp timeout);

/* wrapper around the above. */
void coev_sleep(ev_tstamp timeout);

/* coev_schedule() return values */
#define CSCHED_NOERROR          0  /* no error */
#define CSCHED_DEADMEAT         1  /* attempt to schedule dead coroutine */
#define CSCHED_ALREADY          2  /* attempt to schedule already scheduled coroutine */
#define CSCHED_NOSCHEDULER      3  /* attempt to yield, but no scheduler to switch to (from coev_stall() only) */

/* schedule a switch to the waiter */
int coev_schedule(coev_t *waiter);

/* switch to scheduler until something happens.
   returns 0 on success, CSCHED_* on error */
int coev_stall(void);

/* switch to scheduler and not schedule itself. */
int coev_switch2scheduler(void);

/* report if a scheduler is active */
int coev_is_scheduling(void);

/* ensure switchback will only occur after given coro exits, and will be from it */
void coev_join(coev_t *);

/* must be called for IO scheduling to begin. 
   returns current scheduler and does nothing if there is one running. 
   returns NULL after runqueue and I/O waiters are exhausted. */
coev_t *coev_loop(void);

/* can be called anywhere to stop and return from the coev_loop().
does not perform a switch to scheduler. */
void coev_unloop(void);

/*  Locking implemented only to satisfy Python's current 
    threading model. Design criticism is devnulled. 

    As all execution is serial, there should not be any blocking
    on a lock - allowing a switch inside acquire/release pair is
    as bad a programmer error as it could get. switch() and friends
    themselves are guarded against GIL deadlocks by releasing it before,
    and reacquiring immediately after any context swap - in the threadingmodule.c
    replacement.
    
    acquire() does this: while(lock is held) { spam_dire_insults(); coev_stall(); }
    Witness the awesomeness of a possible deadlock.

*/
colock_t *colock_allocate(void);
void  colock_free(colock_t *p);
int   colock_acquire(colock_t *p, int wf);
void  colock_release(colock_t *p); 
int   colock_is_locked(colock_t *p);

/*  Coroutine-local storage is designed to satisfy perverse semantics 
    that Python/thread.c expects. Go figure.
    
    key value of 0 means this slot is not used. 0 is never returned 
    by cls_allocate().
*/
#define CLS_FREE_SLOT 0L
long  cls_new(void);   
void *cls_get(long k);  /* NULL if not found in the current ctx */
int   cls_set(long k, void *v); /* -1 if value already set. 0 otherwise */
void  cls_del(long k);  
void  cls_drop_across(long k); /* drops key from all keychain */
void  cls_drop_others(void); /* drops all keychains except current */


/* Buffered read/write on network sockets
   
   Originally written as a Python file-like object.
 */

struct _coev_nrbuf {
    int fd;
    char *in_buffer, *in_position;
    ssize_t in_allocated, in_used;
    ssize_t in_limit;
    double iop_timeout;
    coev_t *owner; /* if waiting on socket, who called the wait(). */
    int err_no; /* saved errno */
};

typedef struct _coev_nrbuf cnrbuf_t;

/* prealloc - how much to allocate right away
   rlim - soft limit on read buffer. Is implicitly raised if subsequent
          read() or readline() request more data than that. */
void cnrbuf_init(cnrbuf_t *buf, int fd, double timeout, size_t prealloc, size_t rlim);
void cnrbuf_fini(cnrbuf_t *buf);

/* reads up enough to get you hint bytes in the internal buffer.
   (may actually recv() more than that) If hint == -1, reads until EOF.
   return value: 
       -1 - see errno, *p not changed
        0 - immediate EOF, *p not changed.
       >0 - *p points into the buffer, where you can get this much bytes.
       p and bytecount values are valid until next call to read/readline.
*/
ssize_t cnrbuf_read(cnrbuf_t *buf, void **p, ssize_t hint);

/* same as the above, but reads up to newline, or up to hint, 
   or if hint is -1, up to soflim and returns that.  */
ssize_t cnrbuf_readline(cnrbuf_t *buf, void **p, ssize_t hint);

/* call this to update internal pointer after you're done with data. */
void cnrbuf_done(cnrbuf_t *buf, ssize_t eaten);

/* attempt to send given data. 
   returns 0 on success, or -1 on error, consult errno.
   bytecount of data sent is always stored in *sent. */
int coev_send(int fd, const void *data, ssize_t dlen, ssize_t *sent, double timeout);

/* libwide stuff */
void coev_getstats(coev_instrumentation_t *i);


#define CDF_COEV         0x001   /* switches */
#define CDF_COEV_DUMP    0x002   /* coev state dumps on switches */
#define CDF_RUNQ_DUMP    0x004   /* runq dumps in scheduler */
#define CDF_NBUF         0x010   /* reads/writes */
#define CDF_NBUF_DUMP    0x020   /* buffer metadata before/after */
#define CDF_COLOCK       0x040   /* locking support */
#define CDF_COLOCK_DUMP  0x080   /* lock dumps */
#define CDF_STACK        0x100   /* stack debug */
#define CDF_STACK_DUMP   0x200   /* stack bunch dumps */
#define CDF_COLB_DUMP    0x400   /* lock storage dumps */
#define CDF_CB_ON_NEW_DUMP 0x800   /* coev busy bunch dump on new coev allocation */

void coev_setdebug(int flagsmask);
void coev_dmprintf(const char *, ...);
void coev_dmflush(void);


#ifdef __cplusplus
}
#endif
#endif /* COEV_H */